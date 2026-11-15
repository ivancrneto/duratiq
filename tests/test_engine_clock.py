"""``Engine(clock=...)``: every deadline the engine computes reads the injected clock.

This is the contract ``duratiq.testing`` is built on. If any code path fell back to
the real wall clock, a test that skips virtual time would see deadlines computed
from two different "nows" — e.g. a second ``ctx.sleep`` scheduled *behind* the
virtual clock and firing immediately.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from duratiq import Engine, Registry, SqlStore, activity, heartbeat, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import WorkflowTimer, utcnow

T0 = datetime(2030, 1, 7, 8, 0, tzinfo=timezone.utc)  # a Monday, far from the real clock


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.value = start

    def __call__(self) -> datetime:
        return self.value

    def advance(self, **kwargs: float) -> None:
        self.value += timedelta(**kwargs)


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(T0)


def _engine(reg: Registry, clock: FakeClock) -> tuple[Engine, LocalDriver, SqlStore]:
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store, clock=clock)
    return engine, LocalDriver(engine), store


def test_default_engine_uses_real_time() -> None:
    engine = Engine(Registry(), SqlStore())
    before = utcnow()
    assert before <= engine.now() <= utcnow()


def test_timer_deadline_and_default_scan_time_follow_the_clock(clock: FakeClock) -> None:
    reg = Registry()

    @workflow(name="nap", registry=reg)
    def nap(ctx) -> str:
        ctx.sleep("PT1H")
        ctx.sleep("PT1H")
        return "rested"

    engine, driver, store = _engine(reg, clock)
    run_id = engine.start("nap")
    driver.run_until_idle()

    with store.Session() as s:
        assert _aware(s.get(WorkflowTimer, (run_id, 0)).fire_at) == T0 + timedelta(hours=1)

    # No ``now=`` passed: the scan reads the engine clock, not the wall clock.
    assert engine.fire_due_timers() == 0
    clock.advance(hours=1)
    assert engine.fire_due_timers() == 1
    driver.run_until_idle()

    # The second timer is measured from the *virtual* time it was scheduled at.
    with store.Session() as s:
        assert _aware(s.get(WorkflowTimer, (run_id, 1)).fire_at) == T0 + timedelta(hours=2)
    clock.advance(minutes=59)
    assert engine.fire_due_timers() == 0
    clock.advance(minutes=1)
    assert engine.fire_due_timers() == 1
    driver.run_until_idle()
    assert store.get_run(run_id).status == "COMPLETED"


def test_ctx_now_reads_the_clock(clock: FakeClock) -> None:
    reg = Registry()

    @workflow(name="stamp", registry=reg)
    def stamp(ctx) -> str:
        return ctx.now().isoformat()

    engine, driver, store = _engine(reg, clock)
    run_id = engine.start("stamp")
    driver.run_until_idle()
    assert datetime.fromisoformat(store.get_run(run_id).result["value"]) == T0


def test_activity_and_workflow_deadlines_follow_the_clock(clock: FakeClock) -> None:
    reg = Registry()

    @activity(
        name="slow",
        registry=reg,
        start_to_close_ms=5_000,
        schedule_to_start_timeout_ms=7_000,
        schedule_to_close_timeout_ms=9_000,
    )
    def slow() -> str:
        return "done"

    @workflow(name="timed", registry=reg, execution_timeout=60, run_timeout=30)
    def timed(ctx) -> str:
        return ctx.activity(slow)

    engine, driver, store = _engine(reg, clock)
    run_id = engine.start("timed")
    run = store.get_run(run_id)
    assert _aware(run.execution_timeout_at) == T0 + timedelta(seconds=60)
    assert _aware(run.run_timeout_at) == T0 + timedelta(seconds=30)

    driver.step()  # the first tick schedules the activity; leave it undelivered
    step = store.get_step(run_id, 0)
    assert _aware(step.timeout_at) == T0 + timedelta(seconds=5)
    assert _aware(step.schedule_to_start_at) == T0 + timedelta(seconds=7)
    assert _aware(step.schedule_to_close_at) == T0 + timedelta(seconds=9)


def test_heartbeat_deadline_follows_the_clock(clock: FakeClock) -> None:
    reg = Registry()

    @activity(name="beat", registry=reg, heartbeat_timeout_ms=2_000)
    def beat() -> str:
        clock.advance(seconds=10)  # virtual time passes inside the activity
        heartbeat("halfway")
        return "done"

    @workflow(name="beating", registry=reg)
    def beating(ctx) -> str:
        return ctx.activity(beat)

    engine, driver, store = _engine(reg, clock)
    run_id = engine.start("beating")
    driver.run_until_idle()

    step = store.get_step(run_id, 0)
    # Pushed out from the clock's time at the heartbeat, not from the wall clock.
    assert _aware(step.timeout_at) == T0 + timedelta(seconds=12)
    assert store.get_run(run_id).status == "COMPLETED"


def test_schedules_follow_the_clock(clock: FakeClock) -> None:
    reg = Registry()

    @workflow(name="daily", registry=reg)
    def daily(ctx) -> str:
        return "ran"

    engine, driver, store = _engine(reg, clock)
    sid = engine.create_schedule("daily", "0 9 * * *")
    assert _aware(store.get_schedule(sid).next_fire_at) == T0.replace(hour=9)

    assert engine.fire_due_schedules() == 0
    clock.advance(hours=1)
    assert engine.fire_due_schedules() == 1
    driver.run_until_idle()
    assert _aware(store.get_schedule(sid).next_fire_at) == T0.replace(hour=9) + timedelta(days=1)
