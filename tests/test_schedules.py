"""Recurring schedules: engine.create_schedule + fire_due_schedules start workflow
runs on a cron cadence, advancing to the next cron time each fire.

Like the timer tests, time is fast-forwarded by passing ``now=`` to the scanner."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select

from duratiq import Engine, Registry, SqlStore, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import WorkflowRun

UTC = timezone.utc


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()

    @workflow(name="beep", registry=reg)
    def beep(ctx, label: str) -> dict:
        return {"label": label}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine)


def _run_count(store: SqlStore) -> int:
    with store.Session() as s:
        return s.scalar(select(func.count()).select_from(WorkflowRun))


def test_schedule_fires_only_when_due(ns: SimpleNamespace) -> None:
    t0 = datetime(2026, 6, 15, 8, 59, tzinfo=UTC)
    sid = ns.engine.create_schedule("beep", "0 9 * * *", now=t0, label="x")

    # Before 09:00 nothing fires.
    assert ns.engine.fire_due_schedules(now=t0) == 0
    assert _run_count(ns.store) == 0

    # At 09:00 a run starts.
    assert ns.engine.fire_due_schedules(now=datetime(2026, 6, 15, 9, 0, tzinfo=UTC)) == 1
    ns.engine.driver.run_until_idle()
    assert _run_count(ns.store) == 1

    sch = ns.store.get_schedule(sid)
    assert sch.last_run_id is not None
    # next_fire_at advanced to the following day at 09:00.
    assert (sch.next_fire_at.day, sch.next_fire_at.hour, sch.next_fire_at.minute) == (16, 9, 0)

    # The started run actually ran with the schedule's input.
    run = ns.store.get_run(sch.last_run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"label": "x"}


def test_schedule_is_idempotent_on_id(ns: SimpleNamespace) -> None:
    t0 = datetime(2026, 6, 15, 8, 0, tzinfo=UTC)
    a = ns.engine.create_schedule("beep", "0 9 * * *", schedule_id="daily", now=t0, label="one")
    b = ns.engine.create_schedule("beep", "0 10 * * *", schedule_id="daily", now=t0, label="two")
    assert a == b == "daily"
    sch = ns.store.get_schedule("daily")
    assert sch.cron == "0 9 * * *"  # the first registration wins; the second is a no-op
    assert sch.input == {"label": "one"}


def test_schedule_fires_repeatedly(ns: SimpleNamespace) -> None:
    sid = ns.engine.create_schedule("beep", "0 * * * *", now=datetime(2026, 6, 15, 8, 30, tzinfo=UTC), label="hourly")
    # Fire at the top of three consecutive hours.
    for hour in (9, 10, 11):
        fired = ns.engine.fire_due_schedules(now=datetime(2026, 6, 15, hour, 0, tzinfo=UTC))
        assert fired == 1
        ns.engine.driver.run_until_idle()
    assert _run_count(ns.store) == 3
    assert ns.store.get_schedule(sid).next_fire_at.hour == 12


def test_due_schedule_fires_once_per_scan(ns: SimpleNamespace) -> None:
    ns.engine.create_schedule("beep", "0 9 * * *", now=datetime(2026, 6, 15, 8, 0, tzinfo=UTC), label="x")
    now = datetime(2026, 6, 15, 9, 5, tzinfo=UTC)
    assert ns.engine.fire_due_schedules(now=now) == 1
    # A second scan at the same instant finds nothing — it was claimed/advanced.
    assert ns.engine.fire_due_schedules(now=now) == 0
    ns.engine.driver.run_until_idle()
    assert _run_count(ns.store) == 1


def test_pause_resume_and_delete(ns: SimpleNamespace) -> None:
    sid = ns.engine.create_schedule("beep", "0 9 * * *", now=datetime(2026, 6, 15, 8, 0, tzinfo=UTC), label="x")
    due = datetime(2026, 6, 15, 9, 0, tzinfo=UTC)

    assert ns.engine.pause_schedule(sid) is True
    assert ns.engine.fire_due_schedules(now=due) == 0  # paused -> never fires
    assert _run_count(ns.store) == 0

    assert ns.engine.resume_schedule(sid) is True
    assert ns.engine.fire_due_schedules(now=due) == 1
    ns.engine.driver.run_until_idle()
    assert _run_count(ns.store) == 1

    assert ns.engine.delete_schedule(sid) is True
    assert ns.store.get_schedule(sid) is None
    # A later due time can't fire a deleted schedule.
    assert ns.engine.fire_due_schedules(now=datetime(2026, 6, 16, 9, 0, tzinfo=UTC)) == 0


def test_create_schedule_validates_workflow_and_cron(ns: SimpleNamespace) -> None:
    from duratiq import WorkflowNotFound

    with pytest.raises(WorkflowNotFound):
        ns.engine.create_schedule("nope", "0 9 * * *")
    with pytest.raises(ValueError):
        ns.engine.create_schedule("beep", "not a cron")
