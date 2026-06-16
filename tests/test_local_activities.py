"""Tests for ctx.local_activity — inline synchronous execution (Group 9)."""

from __future__ import annotations

from types import SimpleNamespace


from duratiq import ActivityFailed, Engine, Registry, SqlStore
from duratiq.decorators import workflow
from duratiq.drivers.local import LocalDriver


def _ns() -> SimpleNamespace:
    reg = Registry()
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, driver=driver, engine=engine)


def _add(a: int, b: int) -> int:
    return a + b


def _fail_fn() -> None:
    raise ValueError("local failure")


# ------------------------------------------------------------------ basic execution
def test_local_activity_returns_result() -> None:
    ns = _ns()
    calls: list = []

    def compute():
        calls.append(1)
        return 42

    @workflow(name="wf_la_basic", registry=ns.reg)
    def wf(ctx):
        return ctx.local_activity(compute)

    run_id = ns.engine.start("wf_la_basic")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == 42
    assert len(calls) == 1  # executed exactly once, not re-run on replay


def test_local_activity_with_args() -> None:
    ns = _ns()

    @workflow(name="wf_la_args", registry=ns.reg)
    def wf(ctx):
        return ctx.local_activity(_add, 3, 7)

    run_id = ns.engine.start("wf_la_args")
    ns.driver.run_until_idle()

    assert ns.store.get_run(run_id).result["value"] == 10


def test_local_activity_result_memoized_on_replay() -> None:
    ns = _ns()
    calls: list = []

    def compute():
        calls.append(1)
        return "memo"

    @workflow(name="wf_la_memo", registry=ns.reg)
    def wf(ctx):
        return ctx.local_activity(compute)

    run_id = ns.engine.start("wf_la_memo")
    ns.driver.run_until_idle()

    assert ns.store.get_run(run_id).status == "COMPLETED"

    # Re-tick: replay should NOT call compute again
    ns.engine.tick(run_id)
    assert len(calls) == 1  # still 1; memoized from history


# ------------------------------------------------------------------ failure
def test_local_activity_failure_fails_run_by_default() -> None:
    ns = _ns()

    @workflow(name="wf_la_fail", registry=ns.reg)
    def wf(ctx):
        return ctx.local_activity(_fail_fn)

    run_id = ns.engine.start("wf_la_fail")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"


def test_local_activity_failure_raises_activity_failed_in_workflow() -> None:
    ns = _ns()
    caught: list = []

    @workflow(name="wf_la_catch", registry=ns.reg)
    def wf(ctx):
        try:
            ctx.local_activity(_fail_fn)
        except ActivityFailed as exc:
            caught.append(exc)
            return "caught"
        return "not reached"

    run_id = ns.engine.start("wf_la_catch")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "caught"
    assert len(caught) == 1


def test_local_activity_retry_on_failure() -> None:
    ns = _ns()
    calls: list = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("not yet")
        return "ok"

    @workflow(name="wf_la_retry", registry=ns.reg)
    def wf(ctx):
        return ctx.local_activity(flaky, max_retries=2)

    run_id = ns.engine.start("wf_la_retry")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "ok"
    assert len(calls) == 3  # called 3 times total (1 + 2 retries)


def test_local_activity_exhausted_retries_fails() -> None:
    ns = _ns()
    calls: list = []

    def always_fails():
        calls.append(1)
        raise RuntimeError("always")

    @workflow(name="wf_la_exhaust", registry=ns.reg)
    def wf(ctx):
        return ctx.local_activity(always_fails, max_retries=1)

    run_id = ns.engine.start("wf_la_exhaust")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"
    assert len(calls) == 2  # 1 initial + 1 retry


# ------------------------------------------------------------------ step recorded
def test_local_activity_step_recorded_in_history() -> None:
    ns = _ns()

    @workflow(name="wf_la_step", registry=ns.reg)
    def wf(ctx):
        return ctx.local_activity(_add, 1, 2)

    run_id = ns.engine.start("wf_la_step")
    ns.driver.run_until_idle()

    step = ns.store.get_step(run_id, 0)
    assert step is not None
    assert step.kind == "LOCAL_ACTIVITY"
    assert step.status == "COMPLETED"
    assert step.result["value"] == 3


# ------------------------------------------------------------------ sequence with regular activity
def test_local_activity_after_regular_activity() -> None:
    from duratiq.decorators import activity

    ns = _ns()

    @activity(name="reg_act_la", registry=ns.reg)
    def reg_act():
        return "from_broker"

    def local_fn(v: str) -> str:
        return f"local({v})"

    @workflow(name="wf_la_seq", registry=ns.reg)
    def wf(ctx):
        broker_result = ctx.activity(reg_act)
        local_result = ctx.local_activity(local_fn, broker_result)
        return local_result

    run_id = ns.engine.start("wf_la_seq")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "local(from_broker)"
