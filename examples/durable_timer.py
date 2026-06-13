"""Durable timers: a workflow that parks on ``ctx.sleep`` and is resumed by the
timer scanner — and survives a crash mid-sleep.

A "send a follow-up 10 minutes after checkout" workflow. The interesting part is
the wait: ``ctx.sleep`` records a deadline in the store and suspends the run, so
*no worker is held* while it waits. A periodic scanner (here, ``fire_due_timers``)
delivers the timer once its deadline elapses and re-ticks the run.

To keep the demo instant we fast-forward time by passing ``now=`` to the scanner —
exactly what the unit tests do — instead of sleeping for real ten minutes.

    cd duratiq && python -m examples.durable_timer
"""

from __future__ import annotations

from datetime import timedelta

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import utcnow

reg = Registry()


@activity(name="checkout", registry=reg)
def checkout(order_id: str) -> str:
    print(f"  checkout {order_id}")
    return f"pay_{order_id}"


@activity(name="send_followup", registry=reg)
def send_followup(order_id: str) -> str:
    print(f"  sent follow-up for {order_id}")
    return "sent"


@workflow(name="followup", registry=reg)
def followup(ctx, order_id: str) -> dict:
    payment_id = ctx.activity(checkout, order_id)
    ctx.sleep("PT10M")  # wait ten minutes — durably, holding no worker
    receipt = ctx.activity(send_followup, order_id)
    return {"order_id": order_id, "payment_id": payment_id, "receipt": receipt}


def main() -> None:
    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("followup", order_id="A123")
    driver.run_until_idle()

    run = engine.get(run_id)
    print(f"\nafter checkout, run is {run.status} (parked on the 10-minute timer)")
    assert run.status == "SUSPENDED"

    # A scan *before* the deadline does nothing — the run stays parked.
    fired_now = engine.fire_due_timers(now=utcnow())
    print(f"scan at T+0:    fired {fired_now} timer(s), run is {engine.get(run_id).status}")

    # Fast-forward past the deadline: the scanner delivers the timer and the run
    # drives itself to completion.
    fired_later = engine.fire_due_timers(now=utcnow() + timedelta(minutes=11))
    driver.run_until_idle()
    run = engine.get(run_id)
    print(f"scan at T+11m:  fired {fired_later} timer(s), run is {run.status}")
    print(f"\nresult: {run.result['value']}")

    assert run.status == "COMPLETED"
    print("\n✓ the run waited 10 minutes without holding a worker, then resumed. ✅")


if __name__ == "__main__":
    main()
