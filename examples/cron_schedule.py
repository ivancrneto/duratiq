"""Recurring schedules: start a workflow on a cron cadence.

A nightly-report workflow registered to run at 09:00 every weekday. A real
deployment calls ``engine.fire_due_schedules()`` once a minute (from cron or
periodiq) next to ``fire_due_timers``; here we fast-forward by passing ``now=`` to
the scanner, the same call that periodic trigger would make.

    cd duratiq && python -m examples.cron_schedule
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver

UTC = timezone.utc
reg = Registry()


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

    # 09:00 every weekday (Mon-Fri).
    sid = engine.create_schedule(
        "daily_report", "0 9 * * 1-5",
        schedule_id="eu-daily", now=datetime(2026, 6, 15, 0, 0, tzinfo=UTC), region="eu",
    )
    sch = store.get_schedule(sid)
    print(f"registered schedule {sid!r}; first fire at {sch.next_fire_at}\n")

    # Simulate the once-a-minute scanner running each day at 09:00 across a full
    # week (Mon 15th .. Sun 21st). Each weekday fires; the weekend fires nothing.
    fired_days = []
    for day in range(15, 22):
        now = datetime(2026, 6, day, 9, 0, tzinfo=UTC)
        n = engine.fire_due_schedules(now=now)
        driver.run_until_idle()
        if n:
            fired_days.append(now.strftime("%A"))
        print(f"  scan at {now:%Y-%m-%d %H:%M} (a {now:%A}): started {n} run(s)")

    print(f"\nfired on: {fired_days}")
    assert fired_days == ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    print("✓ the workflow ran every weekday at 9am and skipped the weekend. ✅")


if __name__ == "__main__":
    main()
