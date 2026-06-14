"""Queries: read a running workflow's state without advancing it.

A workflow registers read-only handlers with ``ctx.set_query_handler``;
``engine.query(run_id, name)`` replays it side-effect-free and calls the handler,
which is a closure over the workflow's locals — so it sees every signal processed so
far. The query never schedules, commits, or dispatches anything.

    cd duratiq && python -m examples.queries
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@workflow(name="cart", registry=reg)
def cart(ctx, customer: str) -> dict:
    items: list[str] = []
    status = {"state": "shopping"}

    # Read-only views into the live workflow state.
    ctx.set_query_handler("item_count", lambda: len(items))
    ctx.set_query_handler("items", lambda: list(items))
    ctx.set_query_handler("state", lambda: status["state"])

    while True:
        event = ctx.wait_signal("event")
        if event["type"] == "checkout":
            status["state"] = "checked_out"
            return {"customer": customer, "items": list(items)}
        items.append(event["sku"])


def main() -> None:
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("cart", customer="ivan")
    driver.run_until_idle()
    print(f"started cart {run_id[:8]}; querying as items are added:\n")

    def show(label: str) -> None:
        count = engine.query(run_id, "item_count")
        items = engine.query(run_id, "items")
        print(f"  {label:<18} state={engine.query(run_id, 'state'):<12} count={count}  items={items}")

    show("(empty)")
    for sku in ["A1", "B2", "C3"]:
        engine.signal(run_id, "event", {"type": "add", "sku": sku})
        driver.run_until_idle()
        show(f"after add {sku}")

    engine.signal(run_id, "event", {"type": "checkout"})
    driver.run_until_idle()
    show("after checkout")  # queries still answer on a completed run

    print(f"\nfinal status: {engine.get(run_id).status}")


if __name__ == "__main__":
    main()
