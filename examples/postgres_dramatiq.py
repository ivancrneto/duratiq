"""The real durable path: PostgreSQL store + a real Dramatiq (Redis) broker.

Unlike ``checkout`` (synchronous LocalDriver, in-memory SQLite) this exercises the
production-shaped stack:

* **PostgreSQL** for the durable store — so ``locked_run`` uses the real
  ``pg_advisory_xact_lock`` guarantee, not the SQLite in-process fallback.
* **Redis** as a real Dramatiq broker — ticks and activity dispatches are encoded,
  put on the broker, and pulled back off by a worker.

Run several workflows at once to show the broker fan out across worker threads
while the per-run advisory lock keeps each run's ticks serialised.

Bring infra up first (any Postgres + Redis will do)::

    docker run -d --name duratiq-pg -e POSTGRES_PASSWORD=duratiq -e POSTGRES_USER=duratiq \\
        -e POSTGRES_DB=duratiq -p 55432:5432 postgres:16-alpine
    docker run -d --name duratiq-redis -p 56379:6379 redis:7-alpine

Then::

    cd duratiq && pip install -e ".[examples]" && python -m examples.postgres_dramatiq

Override the endpoints with ``DURATIQ_PG_URL`` / ``DURATIQ_REDIS_URL`` if needed.
"""

from __future__ import annotations

import os
import time

import dramatiq
from dramatiq.brokers.redis import RedisBroker

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.dramatiq import DramatiqDriver

PG_URL = os.environ.get("DURATIQ_PG_URL", "postgresql+psycopg://duratiq:duratiq@localhost:55432/duratiq")
REDIS_URL = os.environ.get("DURATIQ_REDIS_URL", "redis://localhost:56379")

reg = Registry()


@activity(name="charge_card", registry=reg)
def charge_card(order_id: str, amount: int) -> str:
    print(f"  [charge_card] {order_id}: {amount}", flush=True)
    return f"pay_{order_id}"


@activity(name="reserve_inventory", registry=reg)
def reserve_inventory(order_id: str) -> str:
    print(f"  [reserve_inventory] {order_id}", flush=True)
    return f"resv_{order_id}"


@activity(name="ship_order", registry=reg)
def ship_order(order_id: str, payment_id: str, reservation_id: str) -> str:
    print(f"  [ship_order] {order_id}", flush=True)
    return f"track_{order_id}"


@workflow(name="fulfil", registry=reg)
def fulfil(ctx, order_id: str) -> dict:
    payment_id = ctx.activity(charge_card, order_id, 1999)
    reservation_id = ctx.activity(reserve_inventory, order_id)
    tracking = ctx.activity(ship_order, order_id, payment_id, reservation_id)
    return {"order_id": order_id, "payment_id": payment_id, "tracking": tracking}


def main() -> None:
    broker = RedisBroker(url=REDIS_URL)
    broker.flush_all()  # clear any leftovers from a previous run

    store = SqlStore(PG_URL)
    store.create_all()
    assert store.is_postgres, "expected the PostgreSQL advisory-lock path to be active"
    print(f"store dialect: {store.engine.dialect.name}  (is_postgres={store.is_postgres})")
    print(f"broker: {REDIS_URL}\n")

    engine = Engine(reg, store)
    DramatiqDriver(engine, broker=broker)

    order_ids = [f"ORD-{n:03d}" for n in range(1, 6)]

    worker = dramatiq.Worker(broker, worker_threads=4)
    worker.start()
    try:
        print(f"starting {len(order_ids)} concurrent workflows...")
        run_ids = {oid: engine.start("fulfil", order_id=oid) for oid in order_ids}

        # Let the broker drain the tick/activity cascade, then confirm durability by
        # reading every run back from a FRESH store (new connections, no caches).
        verifier = SqlStore(PG_URL)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            runs = {oid: verifier.get_run(rid) for oid, rid in run_ids.items()}
            if all(r.status in {"COMPLETED", "FAILED"} for r in runs.values()):
                break
            time.sleep(0.2)
    finally:
        worker.stop()

    print("\n=== results (read back from PostgreSQL) ===")
    ok = 0
    for oid, rid in run_ids.items():
        run = verifier.get_run(rid)
        marker = "✓" if run.status == "COMPLETED" else "✗"
        print(f"  {marker} {oid}  {run.status:<10} {run.result['value'] if run.result else run.error}")
        ok += run.status == "COMPLETED"

    print(f"\n{ok}/{len(run_ids)} workflows COMPLETED via Postgres + Redis. ✅" if ok == len(run_ids)
          else f"\n{ok}/{len(run_ids)} completed — something went wrong. ✗")
    if ok != len(run_ids):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
