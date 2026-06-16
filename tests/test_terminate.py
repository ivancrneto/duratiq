"""Tests for engine.terminate, batch_cancel, batch_terminate, reset_to_step, update_with_start."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import (
    Engine,
    Registry,
    SqlStore,
    activity,
    workflow,
)
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns():
    reg = Registry()
    calls = {"count": 0}

    @activity(name="noop", registry=reg)
    def noop():
        calls["count"] += 1
        return "done"

    @workflow(name="simple", registry=reg)
    def simple(ctx):
        return ctx.activity(noop)

    @workflow(name="parent", registry=reg)
    def parent(ctx):
        return ctx.child_workflow("child")

    @workflow(name="child", registry=reg)
    def child(ctx):
        return ctx.activity(noop)

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)
    return SimpleNamespace(engine=engine, store=store, calls=calls)


# --- terminate ---

def test_terminate_marks_run_failed(ns):
    run_id = ns.engine.start("simple")
    assert ns.engine.terminate(run_id) is True
    run = ns.engine.get(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "WorkflowTerminated"


def test_terminate_with_reason(ns):
    run_id = ns.engine.start("simple")
    assert ns.engine.terminate(run_id, reason="ops team killed it") is True
    run = ns.engine.get(run_id)
    assert run.error["message"] == "ops team killed it"


def test_terminate_already_terminal_returns_false(ns):
    run_id = ns.engine.start("simple")
    ns.engine.driver.run_until_idle()
    assert ns.engine.get(run_id).status == "COMPLETED"
    assert ns.engine.terminate(run_id) is False


def test_terminate_cascades_to_children(ns):
    parent_id = ns.engine.start("parent")
    # Let it start but not finish (noop hasn't run yet at first tick)
    ns.engine.driver.step()  # parent tick schedules child
    child_run = ns.store.find_child_run(parent_id, 0)
    assert child_run is not None
    ns.engine.driver.step()  # child tick schedules noop

    assert ns.engine.terminate(parent_id) is True
    assert ns.engine.get(parent_id).status == "FAILED"
    assert ns.engine.get(child_run.id).status == "FAILED"
    child_err = ns.engine.get(child_run.id).error
    assert child_err["type"] == "WorkflowTerminated"
    assert "terminated with parent" in child_err["message"]


def test_terminate_notifies_parent_step_as_failed(ns):
    """If a child is terminated, its parent's child_workflow step becomes FAILED."""
    parent_id = ns.engine.start("parent")
    ns.engine.driver.step()
    child_run = ns.store.find_child_run(parent_id, 0)
    ns.engine.driver.step()

    # Terminate the child directly
    ns.engine.terminate(child_run.id)
    ns.engine.driver.run_until_idle()

    # Parent should now be FAILED (child raised ChildWorkflowFailed)
    assert ns.engine.get(parent_id).status == "FAILED"


# --- cancel still works unchanged ---

def test_cancel_marks_cancelled(ns):
    run_id = ns.engine.start("simple")
    assert ns.engine.cancel(run_id) is True
    assert ns.engine.get(run_id).status == "CANCELLED"


# --- batch_cancel ---

def test_batch_cancel_returns_count(ns):
    ids = [ns.engine.start("simple") for _ in range(5)]
    count = ns.engine.batch_cancel(status="PENDING")
    assert count == 5
    for rid in ids:
        assert ns.engine.get(rid).status == "CANCELLED"


def test_batch_cancel_skips_terminal(ns):
    run_id = ns.engine.start("simple")
    ns.engine.driver.run_until_idle()
    assert ns.engine.get(run_id).status == "COMPLETED"
    count = ns.engine.batch_cancel()
    assert count == 0


def test_batch_cancel_filter_by_name(ns):
    id1 = ns.engine.start("simple")
    id2 = ns.engine.start("parent")
    ns.engine.batch_cancel(name="simple")
    assert ns.engine.get(id1).status == "CANCELLED"
    assert ns.engine.get(id2).status != "CANCELLED"


# --- batch_terminate ---

def test_batch_terminate_marks_failed(ns):
    ids = [ns.engine.start("simple") for _ in range(3)]
    count = ns.engine.batch_terminate(status="PENDING", reason="mass kill")
    assert count == 3
    for rid in ids:
        run = ns.engine.get(rid)
        assert run.status == "FAILED"
        assert run.error["message"] == "mass kill"


# --- reset_to_step ---

def test_reset_to_step_requires_failed_run(ns):
    run_id = ns.engine.start("simple")
    assert ns.engine.reset_to_step(run_id, seq=0) is False  # run is PENDING


def test_reset_to_step_invalid_seq_returns_false(ns):
    reg = Registry()

    @activity(name="boom", registry=reg, max_retries=0)
    def boom():
        raise RuntimeError("oops")

    @workflow(name="two_step", registry=reg)
    def two_step(ctx):
        ctx.activity(boom)

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)

    run_id = engine.start("two_step")
    engine.driver.run_until_idle()
    assert engine.get(run_id).status == "FAILED"
    assert engine.reset_to_step(run_id, seq=99) is False


def test_reset_to_step_replays_from_checkpoint():
    reg = Registry()
    side = {"step_a": 0, "step_b": 0}

    @activity(name="step_a", registry=reg)
    def step_a():
        side["step_a"] += 1
        return "a"

    @activity(name="step_b", registry=reg, max_retries=0)
    def step_b():
        side["step_b"] += 1
        raise RuntimeError("fail b")

    @workflow(name="two_act", registry=reg)
    def two_act(ctx):
        a = ctx.activity(step_a)
        b = ctx.activity(step_b)
        return (a, b)

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)

    run_id = engine.start("two_act")
    engine.driver.run_until_idle()
    assert engine.get(run_id).status == "FAILED"
    assert side["step_a"] == 1
    assert side["step_b"] == 1

    # Reset to after step_a (seq=0) — step_b will be re-run
    # seq 0 = step_a; after reset, seq 1 (step_b) is deleted
    assert engine.reset_to_step(run_id, seq=0) is True
    assert engine.get(run_id).status == "PENDING"

    # Fix step_b
    @activity(name="step_b", registry=reg)
    def step_b_fixed():
        side["step_b"] += 1
        return "b_fixed"

    reg._activities["step_b"] = reg._activities["step_b"].__class__(
        fn=step_b_fixed, name="step_b", max_retries=3
    )

    engine.driver.run_until_idle()
    run = engine.get(run_id)
    assert run.status == "COMPLETED"
    # step_a was NOT re-executed (memoized from history)
    assert side["step_a"] == 1
    # step_b was re-executed
    assert side["step_b"] == 2


# --- update_with_start ---

def test_update_with_start_delivers_update_atomically():
    reg = Registry()
    received = {}

    @workflow(name="updatable", registry=reg)
    def updatable(ctx):
        def on_set(key, value):
            received[key] = value
            return "ok"

        ctx.set_update_handler("set", on_set)
        ctx.wait_update()
        return received

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)

    run_id, update_id = engine.update_with_start("updatable", "set", "foo", "bar")
    engine.driver.run_until_idle()

    run = engine.get(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"foo": "bar"}
    result = engine.get_update_result(run_id, update_id)
    assert result == "ok"
