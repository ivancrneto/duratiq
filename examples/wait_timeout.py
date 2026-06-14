"""Waiting for a signal *with a deadline*: approve, or auto-reject after a timeout.

``ctx.wait_signal(name, timeout=...)`` races the signal against a durable timer. If
the signal arrives first you get its payload; if the timer fires first you get the
``TIMEOUT`` sentinel. The losing side is cancelled, so a late approval can't sneak in
after the workflow already gave up.

The timer is advanced with ``fire_due_timers(now=...)`` so the example fast-forwards
instead of waiting an hour.

    cd duratiq && python -m examples.wait_timeout
"""

from __future__ import annotations

from datetime import timedelta

from duratiq import TIMEOUT, Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import utcnow

reg = Registry()


@activity(name="fulfil", registry=reg)
def fulfil(order_id: str) -> str:
    return f"shipped::{order_id}"


@workflow(name="approval", registry=reg)
def approval(ctx, order_id: str) -> dict:
    decision = ctx.wait_signal("review", timeout="PT1H")  # human has an hour to act
    if decision is TIMEOUT:
        return {"order_id": order_id, "outcome": "auto-rejected (no review in time)"}
    if decision["approved"]:
        return {"order_id": order_id, "outcome": ctx.activity(fulfil, order_id)}
    return {"order_id": order_id, "outcome": "rejected by reviewer"}


def _fresh() -> tuple[Engine, LocalDriver]:
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    return engine, LocalDriver(engine)


def main() -> None:
    # 1) A reviewer approves within the window — the signal wins the race.
    engine, driver = _fresh()
    run_id = engine.start("approval", order_id="A1")
    driver.run_until_idle()
    print(f"order A1: parked waiting for review ({engine.get(run_id).status})")
    engine.signal(run_id, "review", {"approved": True})
    driver.run_until_idle()
    print(f"  reviewer approved in time -> {engine.get(run_id).result['value']['outcome']}\n")

    # 2) Nobody reviews — an hour later the timer fires and the workflow auto-rejects.
    engine, driver = _fresh()
    run_id = engine.start("approval", order_id="B2")
    driver.run_until_idle()
    print(f"order B2: parked waiting for review ({engine.get(run_id).status})")
    engine.fire_due_timers(now=utcnow() + timedelta(hours=2))  # the once-a-minute scanner, fast-forwarded
    driver.run_until_idle()
    print(f"  no review in an hour -> {engine.get(run_id).result['value']['outcome']}")


if __name__ == "__main__":
    main()
