"""Child workflows: ``ctx.child_workflow`` starts a sub-run, the parent awaits its
result, failures propagate, and a parent waiting on a child survives a crash.

Driven through the synchronous LocalDriver so each tick/activity is pumped
explicitly — which lets the crash test discard a driver mid-run and resume on a
fresh engine backed by the same store."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()
    calls = {"square": 0, "boom": 0}

    @activity(name="square", registry=reg)
    def square(x: int) -> int:
        calls["square"] += 1
        return x * x

    @activity(name="boom", registry=reg)
    def boom() -> None:
        calls["boom"] += 1
        raise ValueError("child activity exploded")

    @workflow(name="child_sum", registry=reg)
    def child_sum(ctx, a: int, b: int) -> int:
        # A child is an ordinary workflow — it can use activities itself.
        return ctx.activity(square, a) + ctx.activity(square, b)

    @workflow(name="child_fails", registry=reg)
    def child_fails(ctx) -> None:
        ctx.activity(boom)
        return None  # pragma: no cover - the activity fails first

    @workflow(name="parent", registry=reg)
    def parent(ctx, a: int, b: int) -> dict:
        total = ctx.child_workflow("child_sum", a=a, b=b)
        return {"total": total}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver, calls=calls)


def test_child_workflow_completes_and_returns_result(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("parent", a=3, b=4)
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"total": 25}  # 3*3 + 4*4

    # A distinct child run was created, linked back to the parent's CHILD_WORKFLOW step.
    steps = ns.store.get_steps(run_id)
    child_step = next(s for s in steps if s.kind == "CHILD_WORKFLOW")
    child = ns.store.find_child_run(run_id, child_step.seq)
    assert child is not None
    assert child.name == "child_sum"
    assert child.status == "COMPLETED"
    assert child.parent_run_id == run_id


def test_child_failure_propagates_to_parent(ns: SimpleNamespace) -> None:
    @workflow(name="parent_of_failure", registry=ns.reg)
    def parent_of_failure(ctx) -> str:
        ctx.child_workflow("child_fails")
        return "unreachable"  # pragma: no cover

    run_id = ns.engine.start("parent_of_failure")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "ChildWorkflowFailed"


def test_child_failure_can_be_caught_by_parent(ns: SimpleNamespace) -> None:
    from duratiq import ChildWorkflowFailed

    @workflow(name="parent_recovers", registry=ns.reg)
    def parent_recovers(ctx) -> str:
        try:
            ctx.child_workflow("child_fails")
        except ChildWorkflowFailed:
            return "handled"
        return "unreachable"  # pragma: no cover

    run_id = ns.engine.start("parent_recovers")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "handled"


def test_parent_waiting_on_child_survives_crash(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("parent", a=3, b=4)

    # Pump until the child has been started but before it finishes: the parent tick
    # schedules the child and suspends, then the child's first tick + activities run.
    ns.driver.step()  # parent tick: schedules child, parent SUSPENDED, queues child start
    assert ns.store.get_run(run_id).status == "SUSPENDED"
    steps = ns.store.get_steps(run_id)
    child_step = next(s for s in steps if s.kind == "CHILD_WORKFLOW")
    child = ns.store.find_child_run(run_id, child_step.seq)
    assert child is not None and child.status == "PENDING"

    # CRASH: throw away the engine + its queued ticks (the child is mid-flight).
    engine2 = Engine(ns.reg, ns.store)
    driver2 = LocalDriver(engine2)
    # Recovery re-ticks both non-terminal runs, as the recovery scanner would.
    driver2.request_tick(child.id)
    driver2.request_tick(run_id)
    driver2.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"total": 25}
    # Each child activity ran exactly once across the crash — results were memoized.
    assert ns.calls["square"] == 2


def test_child_start_is_idempotent(ns: SimpleNamespace) -> None:
    # Calling _start_child twice for the same (parent, seq) must not create a 2nd run.
    run_id = ns.engine.start("parent", a=2, b=2)
    ns.driver.step()  # parent tick records the child step and starts the child once

    steps = ns.store.get_steps(run_id)
    child_step = next(s for s in steps if s.kind == "CHILD_WORKFLOW")
    first = ns.store.find_child_run(run_id, child_step.seq)

    ns.engine._start_child(run_id, child_step.seq, "child_sum", {"a": 2, "b": 2})
    second = ns.store.find_child_run(run_id, child_step.seq)
    assert first.id == second.id


def test_cancelling_child_fails_the_parent(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("parent", a=1, b=1)
    ns.driver.step()  # parent schedules + starts the child, parent SUSPENDED

    steps = ns.store.get_steps(run_id)
    child_step = next(s for s in steps if s.kind == "CHILD_WORKFLOW")
    child = ns.store.find_child_run(run_id, child_step.seq)

    assert ns.engine.cancel(child.id) is True
    ns.driver.run_until_idle()  # cancellation re-ticks the parent

    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "ChildWorkflowFailed"


def test_child_workflow_accepts_decorated_function(ns: SimpleNamespace) -> None:
    @workflow(name="echo_child", registry=ns.reg)
    def echo_child(ctx, value: int) -> int:
        return value

    @workflow(name="parent_by_ref", registry=ns.reg)
    def parent_by_ref(ctx) -> int:
        # Pass the decorated function itself rather than the name string.
        return ctx.child_workflow(echo_child, value=99)

    run_id = ns.engine.start("parent_by_ref")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == 99
