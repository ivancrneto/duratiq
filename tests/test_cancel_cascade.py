"""Cancelling a parent cascades to its still-running child workflows (and theirs),
while cancelling a child directly still fails its parent."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()

    @workflow(name="leaf", registry=reg)
    def leaf(ctx) -> str:
        return ctx.wait_signal("go")  # parks forever (no signal delivered in these tests)

    @workflow(name="mid", registry=reg)
    def mid(ctx) -> str:
        return ctx.child_workflow("leaf")

    @workflow(name="top", registry=reg)
    def top(ctx) -> str:
        return ctx.child_workflow("mid")

    @workflow(name="parent", registry=reg)
    def parent(ctx) -> str:
        return ctx.child_workflow("leaf")

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine)


def test_cancelling_parent_cancels_running_child(ns: SimpleNamespace) -> None:
    parent_id = ns.engine.start("parent")
    ns.engine.driver.run_until_idle()

    child = ns.store.find_child_run(parent_id, 0)  # the child_workflow is seq 0
    assert child is not None
    assert ns.store.get_run(parent_id).status == "SUSPENDED"
    assert ns.store.get_run(child.id).status == "SUSPENDED"

    assert ns.engine.cancel(parent_id) is True

    # Both the parent and its in-flight child are cancelled.
    assert ns.store.get_run(parent_id).status == "CANCELLED"
    assert ns.store.get_run(child.id).status == "CANCELLED"


def test_cancel_cascades_through_grandchildren(ns: SimpleNamespace) -> None:
    top_id = ns.engine.start("top")
    ns.engine.driver.run_until_idle()

    mid = ns.store.find_child_run(top_id, 0)
    leaf = ns.store.find_child_run(mid.id, 0)
    assert {ns.store.get_run(r).status for r in (top_id, mid.id, leaf.id)} == {"SUSPENDED"}

    assert ns.engine.cancel(top_id) is True

    # The whole sub-tree comes down.
    for run_id in (top_id, mid.id, leaf.id):
        assert ns.store.get_run(run_id).status == "CANCELLED"


def test_cancelling_child_directly_still_fails_parent(ns: SimpleNamespace) -> None:
    parent_id = ns.engine.start("parent")
    ns.engine.driver.run_until_idle()
    child = ns.store.find_child_run(parent_id, 0)

    # Cancel the child directly: the parent must learn about it and fail (not hang).
    assert ns.engine.cancel(child.id) is True
    ns.engine.driver.run_until_idle()

    assert ns.store.get_run(child.id).status == "CANCELLED"
    parent = ns.store.get_run(parent_id)
    assert parent.status == "FAILED"
    assert parent.error["type"] == "ChildWorkflowFailed"


def test_cancel_leaves_completed_children_untouched(ns: SimpleNamespace) -> None:
    # A parent whose child has already completed: cancelling the parent must not
    # touch the finished child.
    reg = ns.reg

    @workflow(name="done_child", registry=reg)
    def done_child(ctx) -> str:
        return "done"

    @workflow(name="slow_parent", registry=reg)
    def slow_parent(ctx) -> str:
        first = ctx.child_workflow("done_child")  # completes immediately
        ctx.wait_signal("never")  # then the parent parks
        return first

    parent_id = ns.engine.start("slow_parent")
    ns.engine.driver.run_until_idle()

    child = ns.store.find_child_run(parent_id, 0)
    assert ns.store.get_run(child.id).status == "COMPLETED"
    assert ns.store.get_run(parent_id).status == "SUSPENDED"

    assert ns.engine.cancel(parent_id) is True
    assert ns.store.get_run(parent_id).status == "CANCELLED"
    # The already-completed child is left as-is.
    assert ns.store.get_run(child.id).status == "COMPLETED"
