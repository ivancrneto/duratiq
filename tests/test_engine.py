"""Core durable-execution semantics: completion, crash-resume, and failure.

These drive the engine through the synchronous LocalDriver so each step is pumped
explicitly — which lets the crash test discard a driver mid-run and resume on a
fresh engine backed by the same store, proving memoization survives a "crash".
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()
    calls = {"a": 0, "b": 0}

    @activity(name="step_a", registry=reg)
    def step_a(x: int) -> int:
        calls["a"] += 1
        return x + 1

    @activity(name="step_b", registry=reg)
    def step_b(x: int) -> int:
        calls["b"] += 1
        return x * 10

    @workflow(name="pipeline", registry=reg)
    def pipeline(ctx, start: int) -> dict:
        a = ctx.activity(step_a, start)
        b = ctx.activity(step_b, a)
        return {"a": a, "b": b}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver, calls=calls)


def test_sequential_workflow_completes(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("pipeline", start=5)
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"a": 6, "b": 60}
    assert ns.calls == {"a": 1, "b": 1}


def test_crash_mid_run_resumes_without_re_executing(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("pipeline", start=5)

    # Pump just far enough to finish the first activity, then "crash".
    assert ns.driver.step() == "tick"      # schedules step_a, run suspends
    assert ns.driver.step() == "activity"  # runs step_a once, records result, queues a tick
    assert ns.calls["a"] == 1
    assert ns.calls["b"] == 0
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    # CRASH: throw away the engine + its in-memory queue (the pending tick is lost).
    # Recovery: a fresh engine on the SAME store, as the recovery scanner would do.
    engine2 = Engine(ns.reg, ns.store)
    driver2 = LocalDriver(engine2)
    driver2.request_tick(run_id)
    driver2.run_until_idle()

    # step_a was NOT re-executed — its result was replayed from the store.
    assert ns.calls["a"] == 1
    assert ns.calls["b"] == 1
    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"a": 6, "b": 60}


def test_activity_failure_marks_run_failed() -> None:
    reg = Registry()

    @activity(name="boom", registry=reg)
    def boom() -> None:
        raise ValueError("nope")

    @workflow(name="wf_fail", registry=reg)
    def wf_fail(ctx) -> str:
        ctx.activity(boom)
        return "unreachable"

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("wf_fail")
    driver.run_until_idle()

    run = store.get_run(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "ActivityFailed"
