"""Tests for activity schedule-to-start and schedule-to-close timeouts (Group 4)."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace


from duratiq import Engine, Registry, SqlStore
from duratiq.decorators import activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import utcnow


def _ns() -> SimpleNamespace:
    reg = Registry()
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, driver=driver, engine=engine)


# ------------------------------------------------------------------ schedule-to-start
def test_schedule_to_start_timeout_fails_step() -> None:
    ns = _ns()

    @activity(name="slow_start", schedule_to_start_timeout_ms=1, registry=ns.reg)
    def slow_start():
        return "ok"

    @workflow(name="wf_s2s", registry=ns.reg)
    def wf(ctx):
        return ctx.activity(slow_start)

    run_id = ns.engine.start("wf_s2s")
    ns.driver.step()  # tick: schedules the activity step; also queues ("activity", ...)

    far_future = utcnow() + timedelta(hours=1)
    fired = ns.engine.fire_due_schedule_to_start_timeouts(now=far_future)
    assert fired == 1

    # Verify the step is FAILED before the queued activity worker runs
    step = ns.store.get_step(run_id, 0)
    assert step is not None
    assert step.status == "FAILED"
    assert step.error["type"] == "ScheduleToStartTimeout"

    # Directly tick the run (bypassing the driver queue which has the stale activity)
    ns.engine.tick(run_id)
    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "ActivityFailed"


def test_schedule_to_start_timeout_not_fired_if_activity_runs_first() -> None:
    ns = _ns()

    @activity(name="fast_act", schedule_to_start_timeout_ms=999_999, registry=ns.reg)
    def fast_act():
        return "done"

    @workflow(name="wf_fast", registry=ns.reg)
    def wf(ctx):
        return ctx.activity(fast_act)

    run_id = ns.engine.start("wf_fast")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"

    far_future = utcnow() + timedelta(hours=1)
    fired = ns.engine.fire_due_schedule_to_start_timeouts(now=far_future)
    assert fired == 0


def test_schedule_to_start_no_timeout_when_not_set() -> None:
    ns = _ns()

    @activity(name="no_timeout_act", registry=ns.reg)
    def no_timeout():
        return "ok"

    @workflow(name="wf_no_s2s", registry=ns.reg)
    def wf(ctx):
        return ctx.activity(no_timeout)

    ns.engine.start("wf_no_s2s")
    ns.driver.step()  # tick schedules activity — no s2s timeout set

    far_future = utcnow() + timedelta(hours=1)
    fired = ns.engine.fire_due_schedule_to_start_timeouts(now=far_future)
    assert fired == 0


# ------------------------------------------------------------------ schedule-to-close
def test_schedule_to_close_timeout_fails_step_immediately() -> None:
    """schedule_to_close_at is total budget; once exceeded, activity fails without retry."""
    ns = _ns()

    @activity(name="long_act", schedule_to_close_timeout_ms=1, max_retries=5, registry=ns.reg)
    def long_act():
        raise RuntimeError("boom")

    @workflow(name="wf_s2c", registry=ns.reg)
    def wf(ctx):
        return ctx.activity(long_act)

    run_id = ns.engine.start("wf_s2c")
    ns.driver.step()  # tick: schedules the step with schedule_to_close_at

    # Simulate the activity executor calling report_activity_result with a failed attempt
    # after the close deadline has passed — _timeout_activity should detect it
    far_future = utcnow() + timedelta(hours=1)
    ns.engine._timeout_activity(run_id, seq=0, now=far_future)
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"


def test_schedule_to_close_step_column_set() -> None:
    """Verify the schedule_to_close_at column is populated on the step row."""
    ns = _ns()

    @activity(name="col_act", schedule_to_close_timeout_ms=60_000, registry=ns.reg)
    def col_act():
        return 1

    @workflow(name="wf_col", registry=ns.reg)
    def wf(ctx):
        return ctx.activity(col_act)

    run_id = ns.engine.start("wf_col")
    ns.driver.step()

    step = ns.store.get_step(run_id, 0)
    assert step is not None
    assert step.schedule_to_close_at is not None


def test_schedule_to_start_step_column_set() -> None:
    """Verify the schedule_to_start_at column is populated on the step row."""
    ns = _ns()

    @activity(name="s2s_col_act", schedule_to_start_timeout_ms=30_000, registry=ns.reg)
    def s2s_col_act():
        return 1

    @workflow(name="wf_s2s_col", registry=ns.reg)
    def wf(ctx):
        return ctx.activity(s2s_col_act)

    run_id = ns.engine.start("wf_s2s_col")
    ns.driver.step()

    step = ns.store.get_step(run_id, 0)
    assert step is not None
    assert step.schedule_to_start_at is not None
