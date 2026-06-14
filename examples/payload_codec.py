"""Payload codec: offload large payloads out of Postgres, transparently.

A codec keeps small values inline but pushes anything large into a side "blob store"
(here just an in-memory dict standing in for S3), storing only a reference in the
database. The workflow and engine are untouched — they still pass and receive plain
Python values; only what lands in the DB column changes.

    cd duratiq && python -m examples.payload_codec
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy import text

from duratiq import Engine, Registry, SqlStore, activity, set_payload_codec, workflow
from duratiq.drivers.local import LocalDriver

BLOBS: dict[str, bytes] = {}  # stands in for S3/GCS/blob storage
INLINE_LIMIT = 64  # bytes; anything bigger is offloaded


class OffloadingCodec:
    def encode(self, value: Any) -> Any:
        blob = json.dumps(value).encode()
        if len(blob) <= INLINE_LIMIT:
            return value
        key = hashlib.sha256(blob).hexdigest()[:16]
        BLOBS[key] = blob
        return {"__blob__": key}

    def decode(self, value: Any) -> Any:
        if isinstance(value, dict) and "__blob__" in value:
            return json.loads(BLOBS[value["__blob__"]])
        return value


reg = Registry()


@activity(name="render", registry=reg)
def render(n: int) -> dict:
    # A deliberately big result that shouldn't sit inline in the history.
    return {"rows": [{"i": i, "v": f"value-{i}"} for i in range(n)]}


@workflow(name="report", registry=reg)
def report(ctx, n: int) -> dict:
    return ctx.activity(render, n)


def main() -> None:
    set_payload_codec(OffloadingCodec())

    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("report", n=50)
    driver.run_until_idle()

    run = engine.get(run_id)
    print(f"run status: {run.status}")
    print(f"engine sees a result with {len(run.result['value']['rows'])} rows (decoded)")

    # Peek at what actually landed in the DB for the big activity result.
    with store.engine.connect() as conn:
        raw = conn.execute(
            text("SELECT result FROM workflow_steps WHERE run_id = :id AND kind = 'ACTIVITY'"),
            {"id": run_id},
        ).scalar()
    print(f"\nstored in the DB column: {raw}")
    print(f"blob store holds {len(BLOBS)} offloaded payload(s)")

    assert run.status == "COMPLETED"
    assert len(run.result["value"]["rows"]) == 50
    assert "__blob__" in str(raw)  # the big result was offloaded by reference
    print("\n✓ big payload offloaded to the blob store; only a reference hit Postgres. ✅")


if __name__ == "__main__":
    main()
