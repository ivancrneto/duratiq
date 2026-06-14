"""The scanner: one loop that drives timers, schedules, and crash recovery.

In production you run the scanner as its own process next to your workers —

    duratiq-scanner myapp.workers:make_engine          # console script
    python -m duratiq.scanner myapp.workers:make_engine # equivalent

— or embed ``Scanner(engine).run_forever()`` on a background thread. It calls the
three engine scans on independent intervals; each requested tick is processed by a
worker (the ``DramatiqDriver``), so a deployment makes progress on its own.

Here we use the synchronous ``LocalDriver`` and call ``scanner.run_once(now=...)``
to fast-forward the clock — the same calls ``run_forever`` makes each interval,
just stepped by hand so the example is deterministic.

    cd duratiq && python -m examples.scanner
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from duratiq import Engine, Registry, Scanner, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import utcnow

UTC = timezone.utc
reg = Registry()


@activity(name="send_reminder", registry=reg)
def send_reminder(order_id: str) -> str:
    print(f"  reminder sent for {order_id}")
    return f"reminded::{order_id}"


@workflow(name="reminder", registry=reg)
def reminder(ctx, order_id: str) -> dict:
    ctx.sleep("PT10M")  # park for 10 minutes, holding no worker
    return {"sent": ctx.activity(send_reminder, order_id)}


@activity(name="build_report", registry=reg)
def build_report(region: str) -> str:
    print(f"  built report for {region}")
    return f"report::{region}"


@workflow(name="daily_report", registry=reg)
def daily_report(ctx, region: str) -> dict:
    return {"report": ctx.activity(build_report, region)}


def main() -> None:
    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    scanner = Scanner(engine)

    # A run that sleeps, and a schedule that fires weekday mornings.
    run_id = engine.start("reminder", order_id="A123")
    driver.run_until_idle()
    engine.create_schedule(
        "daily_report", "0 9 * * 1-5", schedule_id="eu-daily", now=datetime(2026, 6, 15, 0, 0, tzinfo=UTC), region="eu"
    )
    print(f"started run {run_id[:8]} (sleeping); registered a weekday-9am schedule\n")

    # One scan now: nothing is due yet (the timer's deadline is 10 minutes out).
    print("scan at T+0:", scanner.run_once())

    # Fast-forward 11 minutes: the timer is due, so the reminder run advances.
    later = scanner.run_once(now=utcnow() + timedelta(minutes=11))
    driver.run_until_idle()
    print("scan at T+11m:", later)

    # Fast-forward to Monday 09:00: the schedule fires and starts a report run.
    monday = scanner.run_once(now=datetime(2026, 6, 15, 9, 0, tzinfo=UTC))
    driver.run_until_idle()
    print("scan at Mon 09:00:", monday)

    print(f"\nreminder run -> {engine.get(run_id).status}")


if __name__ == "__main__":
    main()
