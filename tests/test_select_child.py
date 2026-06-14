"""``ctx.select`` with a child-workflow branch (``ctx.defer_child``).

A child can win a race (its result is returned), or lose — in which case its step is
cancelled *and the sub-run itself is cancelled*, cascading to its children, so it stops
doing work and a late completion can't flip the winner.
"""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import utcnow


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()

    @activity(name="boom", registry=reg, max_retries=0)
    def boom() -> None:
        raise ValueError("kaboom")

    @workflow(name="child_double", registry=reg)
    def child_double(ctx, x: int) -> dict:
        return {"doubled": x * 2}

    @workflow(name="child_waiter", registry=reg)
    def child_waiter(ctx) -> dict:
        return {"got": ctx.wait_signal("never")}  # parks forever

    @workflow(name="child_boom", registry=reg)
    def child_boom(ctx) -> None:
        ctx.activity(boom)

    @workflow(name="child_or_timer", registry=reg)
    def child_or_timer(ctx, x: int) -> dict:
        idx, val = ctx.select(ctx.defer_child("child_double", x=x), ctx.defer_timer("PT1H"))
        return {"idx": idx, "val": val}

    @workflow(name="waiter_or_timer", registry=reg)
    def waiter_or_timer(ctx) -> dict:
        idx, val = ctx.select(ctx.defer_child("child_waiter"), ctx.defer_timer("PT1H"))
        return {"idx": idx, "val": val}

    @workflow(name="boom_or_timer", registry=reg)
    def boom_or_timer(ctx) -> dict:
        idx, val = ctx.select(ctx.defer_child("child_boom"), ctx.defer_timer("PT1H"))
        return {"idx": idx, "val": val}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver)


def test_child_wins(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("child_or_timer", x=21)
    ns.driver.run_until_idle()  # the child runs to completion and wins

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"idx": 0, "val": {"doubled": 42}}
    assert ns.store.get_step(run_id, 1).status == "CANCELLED"  # timer dropped


def test_timer_wins_and_child_run_is_cancelled(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("waiter_or_timer")
    ns.driver.run_until_idle()
    child = ns.store.find_child_run(run_id, 0)  # the child started and is waiting
    assert child is not None
    assert ns.store.get_run(child.id).status == "SUSPENDED"

    assert ns.engine.fire_due_timers(now=utcnow() + timedelta(hours=2)) == 1
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.result["value"] == {"idx": 1, "val": None}
    assert ns.store.get_step(run_id, 0).status == "CANCELLED"  # losing child branch
    assert ns.store.get_run(child.id).status == "CANCELLED"  # the sub-run was cancelled


def test_late_child_completion_does_not_flip_winner(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("waiter_or_timer")
    ns.driver.run_until_idle()
    child = ns.store.find_child_run(run_id, 0)

    ns.engine.fire_due_timers(now=utcnow() + timedelta(hours=2))
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).result["value"] == {"idx": 1, "val": None}

    # The (now cancelled) child gets a late signal; even if it tried to finish, the
    # parent's branch is CANCELLED and the winner can't change.
    assert ns.engine.signal(child.id, "never", {"x": 1}) is False  # child already terminal
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).result["value"] == {"idx": 1, "val": None}
    assert ns.store.get_step(run_id, 0).status == "CANCELLED"


def test_failing_child_reraises(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("boom_or_timer")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "ChildWorkflowFailed"
    assert ns.store.get_step(run_id, 1).status == "CANCELLED"  # timer dropped
