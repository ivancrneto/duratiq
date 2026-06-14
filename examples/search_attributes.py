"""Search attributes: typed, indexed metadata for an ops/admin view.

Tag runs with structured fields — ``region``, ``priority``, ``customer`` — at start or
from inside the workflow with ``ctx.upsert_search_attributes``, then filter on them
with ``engine.list_runs``. Filters AND together and match by type, so this is the read
side a "show me FAILED high-priority EU orders" dashboard is built on.

    cd duratiq && python -m examples.search_attributes
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@workflow(name="order", registry=reg)
def order(ctx, order_id: str, region: str, priority: int) -> dict:
    # Attributes can also be set/updated mid-flight from the workflow body.
    ctx.upsert_search_attributes({"region": region, "priority": priority, "stage": "received"})
    return {"order_id": order_id, "stage": "received"}


def main() -> None:
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    orders = [
        ("A1", "eu", 1),
        ("A2", "eu", 3),
        ("A3", "us", 1),
        ("A4", "us", 1),
    ]
    for order_id, region, priority in orders:
        # Set some at start, the rest the workflow upserts itself — both are queryable.
        engine.start("order", order_id=order_id, region=region, priority=priority)
    driver.run_until_idle()
    print(f"started {len(orders)} orders\n")

    def show(label: str, **filters: object) -> None:
        runs = engine.list_runs(search_attributes=filters)
        ids = sorted(engine.get(r.id).result["value"]["order_id"] for r in runs)
        print(f"  {label:<28} -> {ids}")

    show("region=eu", region="eu")
    show("region=us", region="us")
    show("priority=1", priority=1)
    show("region=us AND priority=1", region="us", priority=1)
    show("priority='1' (string, typed)", priority="1")  # int != str -> no match

    print(f"\n  count(priority=1) = {engine.count_runs(search_attributes={'priority': 1})}")
    print(
        f"  attributes of A1   = {engine.get_search_attributes(engine.list_runs(search_attributes={'region': 'eu', 'priority': 1})[0].id)}"
    )


if __name__ == "__main__":
    main()
