"""Pluggable payload codec: workflow/step/signal payloads pass through the active
codec on the way into and out of the database, with the engine seeing plain values.

The default IdentityCodec is exercised by every other test; here we install a custom
codec and prove (a) the engine still sees decoded values end-to-end and (b) what
actually lands in the DB is the encoded form."""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import text

from duratiq import Engine, Registry, SqlStore, activity, get_payload_codec, set_payload_codec, workflow
from duratiq.codec import IdentityCodec
from duratiq.drivers.local import LocalDriver


class WrapCodec:
    """Wraps every payload in an envelope and counts calls — reversible, so values
    round-trip while leaving a visible marker in the stored JSON."""

    def __init__(self) -> None:
        self.encodes = 0
        self.decodes = 0

    def encode(self, value: Any) -> Any:
        self.encodes += 1
        return {"__wrapped__": value}

    def decode(self, value: Any) -> Any:
        self.decodes += 1
        if isinstance(value, dict) and "__wrapped__" in value:
            return value["__wrapped__"]
        return value


@pytest.fixture
def codec() -> WrapCodec:
    c = WrapCodec()
    set_payload_codec(c)
    try:
        yield c
    finally:
        set_payload_codec(IdentityCodec())  # never leak into other tests


def test_engine_sees_decoded_values_end_to_end(codec: WrapCodec) -> None:
    reg = Registry()

    @activity(name="double", registry=reg)
    def double(x: int) -> int:
        return x * 2

    @workflow(name="wf", registry=reg)
    def wf(ctx, start: int) -> dict:
        return {"doubled": ctx.activity(double, start)}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)

    run_id = engine.start("wf", start=21)
    engine.driver.run_until_idle()

    run = engine.get(run_id)
    # Despite the wrapping codec, the engine reads back plain, decoded values.
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"doubled": 42}
    assert run.input == {"start": 21}

    # The activity's memoized step result is decoded too.
    step = next(s for s in store.get_steps(run_id) if s.kind == "ACTIVITY")
    assert step.result["value"] == 42

    assert codec.encodes > 0 and codec.decodes > 0


def test_stored_bytes_are_encoded(codec: WrapCodec) -> None:
    reg = Registry()

    @workflow(name="wf2", registry=reg)
    def wf2(ctx, note: str) -> str:
        return note

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)

    run_id = engine.start("wf2", note="hello")
    engine.driver.run_until_idle()

    # Read the raw column with the codec disabled, to see what actually hit the DB.
    set_payload_codec(IdentityCodec())
    with store.engine.connect() as conn:
        raw_input, raw_result = conn.execute(
            text("SELECT input, result FROM workflow_runs WHERE id = :id"), {"id": run_id}
        ).one()
    # Re-enable for the fixture teardown's symmetry (teardown resets anyway).
    set_payload_codec(codec)

    # The stored JSON carries the codec's envelope marker, not the bare value.
    assert "__wrapped__" in str(raw_input)
    assert "__wrapped__" in str(raw_result)


def test_default_is_identity() -> None:
    # Outside the fixture the global codec is the pass-through identity codec.
    assert isinstance(get_payload_codec(), IdentityCodec)
    assert get_payload_codec().encode({"a": 1}) == {"a": 1}
