"""Child workflows: ``ctx.child_workflow`` runs another workflow as a sub-run and
returns its result — durable composition.

An order-processing workflow that delegates shipping to its own workflow. The child
is a full workflow in its own right (it runs activities, can sleep, can wait on
signals); the parent suspends while it runs and resumes with its result once it
completes. A failed child raises ``ChildWorkflowFailed`` in the parent, where it can
be caught — here the parent falls back to a manual-handling path.

    cd duratiq && python -m examples.child_workflow
"""

from __future__ import annotations

from duratiq import ChildWorkflowFailed, Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@activity(name="pack_box", registry=reg)
def pack_box(order_id: str) -> str:
    print(f"  packed box for {order_id}")
    return f"box_{order_id}"


@activity(name="buy_label", registry=reg)
def buy_label(order_id: str, *, fail: bool) -> str:
    if fail:
        raise RuntimeError("carrier API down")
    print(f"  bought shipping label for {order_id}")
    return f"label_{order_id}"


@workflow(name="ship_order", registry=reg)
def ship_order(ctx, order_id: str, fail_label: bool) -> dict:
    box = ctx.activity(pack_box, order_id)
    label = ctx.activity(buy_label, order_id, fail=fail_label)
    return {"box": box, "label": label}


@workflow(name="process_order", registry=reg)
def process_order(ctx, order_id: str, fail_label: bool) -> dict:
    try:
        shipment = ctx.child_workflow("ship_order", order_id=order_id, fail_label=fail_label)
    except ChildWorkflowFailed as exc:
        print(f"  shipping failed ({exc.error.get('message')}); routing to manual handling")
        return {"order_id": order_id, "status": "needs_manual_shipping"}
    return {"order_id": order_id, "status": "shipped", "shipment": shipment}


def main() -> None:
    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    print("happy path — child ships successfully:")
    ok = engine.start("process_order", order_id="A123", fail_label=False)
    driver.run_until_idle()
    run = engine.get(ok)
    print(f"\nparent is {run.status}: {run.result['value']}\n")
    assert run.status == "COMPLETED" and run.result["value"]["status"] == "shipped"

    print("failure path — child fails, parent catches and falls back:")
    bad = engine.start("process_order", order_id="B456", fail_label=True)
    driver.run_until_idle()
    run = engine.get(bad)
    print(f"\nparent is {run.status}: {run.result['value']}")
    assert run.status == "COMPLETED" and run.result["value"]["status"] == "needs_manual_shipping"

    print("\n✓ a parent durably ran a child workflow and handled its failure. ✅")


if __name__ == "__main__":
    main()
