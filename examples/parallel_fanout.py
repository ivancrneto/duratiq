"""Parallel fan-out: ``ctx.gather`` runs several activities at once and waits for
all of them — a barrier, not a sequence.

An order-fulfilment step that, once payment clears, needs three independent things
done: make a receipt, reserve inventory, and notify the warehouse. They don't
depend on each other, so running them sequentially would waste time. ``ctx.defer``
captures each call without starting it; ``ctx.gather`` launches all of them
together and resumes the workflow only when every branch has completed, returning
the results in order.

    cd duratiq && python -m examples.parallel_fanout
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@activity(name="make_receipt", registry=reg)
def make_receipt(order_id: str) -> str:
    print(f"  made receipt for {order_id}")
    return f"receipt_{order_id}"


@activity(name="reserve_inventory", registry=reg)
def reserve_inventory(order_id: str) -> str:
    print(f"  reserved inventory for {order_id}")
    return f"resv_{order_id}"


@activity(name="notify_warehouse", registry=reg)
def notify_warehouse(order_id: str) -> str:
    print(f"  notified warehouse for {order_id}")
    return "notified"


@workflow(name="fulfil", registry=reg)
def fulfil(ctx, order_id: str) -> dict:
    receipt, reservation, _ = ctx.gather(
        ctx.defer(make_receipt, order_id),
        ctx.defer(reserve_inventory, order_id),
        ctx.defer(notify_warehouse, order_id),
    )
    return {"order_id": order_id, "receipt": receipt, "reservation": reservation}


def main() -> None:
    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("fulfil", order_id="A123")

    # After the first tick, all three branches are queued together — before any runs.
    driver.step()
    pending = [item for item in driver.queue if item[0] == "activity"]
    print(f"\nafter one tick, {len(pending)} activities are queued in parallel "
          f"(run is {engine.get(run_id).status})\n")

    driver.run_until_idle()
    run = engine.get(run_id)
    print(f"\nrun is {run.status}: {run.result['value']}")
    assert run.status == "COMPLETED"
    print("\n✓ three independent activities ran in parallel under one barrier. ✅")


if __name__ == "__main__":
    main()
