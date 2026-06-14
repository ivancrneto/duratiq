"""Observability: a listener prints every run/activity lifecycle event.

The same hook you'd wire to structured logs, a Prometheus counter, or an
OpenTelemetry span. Here it just prints, so you can watch a run move through its
states: started -> activity scheduled -> suspended -> activity completed -> completed.

    cd duratiq && python -m examples.observability
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, WorkflowEvent, activity, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@activity(name="reserve", registry=reg)
def reserve(order_id: str) -> str:
    return f"resv_{order_id}"


@activity(name="charge", registry=reg)
def charge(order_id: str) -> str:
    return f"pay_{order_id}"


@workflow(name="checkout", registry=reg)
def checkout(ctx, order_id: str) -> dict:
    reservation = ctx.activity(reserve, order_id)
    payment = ctx.activity(charge, order_id)
    return {"reservation": reservation, "payment": payment}


def on_event(e: WorkflowEvent) -> None:
    detail = ""
    if e.seq is not None:
        detail = f" seq={e.seq}"
    if e.result is not None:
        detail = f" result={e.result}"
    if e.error is not None:
        detail = f" error={e.error['type']}"
    print(f"  📡 {e.type:<20} {e.name or ''}{detail}")


def main() -> None:
    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store, listener=on_event)
    driver = LocalDriver(engine)

    print("events for one checkout run:\n")
    run_id = engine.start("checkout", order_id="A123")
    driver.run_until_idle()

    run = engine.get(run_id)
    print(f"\nfinal status: {run.status} -> {run.result['value']}")
    assert run.status == "COMPLETED"
    print("\n✓ every state transition surfaced through the listener hook. ✅")


if __name__ == "__main__":
    main()
