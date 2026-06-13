"""Signals: a workflow that parks on a human decision, then resumes when the
decision is delivered with ``engine.signal``.

A "hold the order until a reviewer approves it" workflow. ``ctx.wait_signal``
suspends the run — holding no worker — until an outside actor (a reviewer clicking
a button, another service) delivers a ``"review"`` signal. The signal's payload
flows back into the workflow as the return value of ``wait_signal``.

Signals that arrive *before* the wait is reached are queued and matched FIFO, so
there is no race between the reviewer and the workflow.

    cd duratiq && python -m examples.manual_approval
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@activity(name="fulfil_order", registry=reg)
def fulfil_order(order_id: str) -> str:
    print(f"  fulfilling {order_id}")
    return f"shipped_{order_id}"


@activity(name="reject_order", registry=reg)
def reject_order(order_id: str) -> str:
    print(f"  rejecting {order_id}")
    return f"refunded_{order_id}"


@workflow(name="review_order", registry=reg)
def review_order(ctx, order_id: str) -> dict:
    decision = ctx.wait_signal("review")  # parks here until a reviewer decides
    if decision["approved"]:
        return {"order_id": order_id, "result": ctx.activity(fulfil_order, order_id)}
    return {"order_id": order_id, "result": ctx.activity(reject_order, order_id)}


def main() -> None:
    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("review_order", order_id="A123")
    driver.run_until_idle()

    run = engine.get(run_id)
    print(f"\nrun is {run.status} — waiting for a reviewer (no worker held)")
    assert run.status == "SUSPENDED"

    # ... minutes or days later, a reviewer approves the order.
    print('\nreviewer delivers signal "review" with {"approved": True}')
    engine.signal(run_id, "review", {"approved": True})
    driver.run_until_idle()

    run = engine.get(run_id)
    print(f"\nrun is {run.status}: {run.result['value']}")
    assert run.status == "COMPLETED"
    print("\n✓ the run waited on a human decision, then resumed with its payload. ✅")


if __name__ == "__main__":
    main()
