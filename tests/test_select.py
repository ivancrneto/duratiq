"""``ctx.select`` — wait for the first of several branches (activity / signal / timer).

The winning branch's value is returned; the losers that are still pending are
cancelled so the decision is fixed across replays. Timers are advanced with
``fire_due_timers(now=...)`` to fast-forward.
"""

from __future__ import annotations

from collections import deque
from datetime import timedelta
from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import utcnow


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()

    @activity(name="quick", registry=reg)
    def quick(x: int) -> int:
        return x * 10

    @activity(name="other", registry=reg)
    def other(x: int) -> int:
        return x + 1

    @activity(name="boom", registry=reg, max_retries=0)
    def boom() -> None:
        raise ValueError("kaboom")

    @workflow(name="act_or_timer", registry=reg)
    def act_or_timer(ctx, x: int) -> dict:
        idx, val = ctx.select(ctx.defer(quick, x), ctx.defer_timer("PT1H"))
        return {"idx": idx, "val": val}

    @workflow(name="signal_or_timer", registry=reg)
    def signal_or_timer(ctx) -> dict:
        idx, val = ctx.select(ctx.defer_signal("go"), ctx.defer_timer("PT1H"))
        return {"idx": idx, "val": val}

    @workflow(name="two_acts", registry=reg)
    def two_acts(ctx) -> dict:
        idx, val = ctx.select(ctx.defer(quick, 5), ctx.defer(other, 5))
        return {"idx": idx, "val": val}

    @workflow(name="two_signals", registry=reg)
    def two_signals(ctx) -> dict:
        idx, val = ctx.select(ctx.defer_signal("a"), ctx.defer_signal("b"))
        return {"idx": idx, "val": val}

    @workflow(name="boom_or_timer", registry=reg)
    def boom_or_timer(ctx) -> dict:
        idx, val = ctx.select(ctx.defer(boom), ctx.defer_timer("PT1H"))
        return {"idx": idx, "val": val}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver)


def test_activity_wins_and_timer_cancelled(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("act_or_timer", x=4)
    ns.driver.run_until_idle()  # the activity runs inline and wins before the timer

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"idx": 0, "val": 40}
    assert ns.store.get_step(run_id, 1).status == "CANCELLED"  # timer dropped
    assert ns.engine.fire_due_timers(now=utcnow() + timedelta(hours=2)) == 0


def test_signal_wins(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("signal_or_timer")
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    ns.engine.signal(run_id, "go", {"n": 7})
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.result["value"] == {"idx": 0, "val": {"n": 7}}
    assert ns.store.get_step(run_id, 1).status == "CANCELLED"  # timer dropped
    assert ns.engine.fire_due_timers(now=utcnow() + timedelta(hours=2)) == 0


def test_timer_wins(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("signal_or_timer")
    ns.driver.run_until_idle()

    assert ns.engine.fire_due_timers(now=utcnow() + timedelta(hours=2)) == 1
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.result["value"] == {"idx": 1, "val": None}
    assert ns.store.get_step(run_id, 0).status == "CANCELLED"  # abandoned signal wait

    # A late "go" signal isn't consumed by the cancelled wait (the run is terminal anyway).
    assert ns.engine.signal(run_id, "go", {"n": 1}) is False


def test_late_activity_result_is_dropped(ns: SimpleNamespace) -> None:
    # A manual driver lets branch 1 finish first while branch 0 is still in flight,
    # so branch 0 is cancelled and its later result must be dropped (winner can't flip).
    class ManualDriver:
        def __init__(self, engine: Engine) -> None:
            self.engine = engine
            engine.driver = self
            self.ticks: deque[str] = deque()

        def request_tick(self, run_id: str) -> None:
            self.ticks.append(run_id)

        def dispatch_activity(self, *a: object) -> None:  # we report results by hand
            pass

        def run_ticks(self) -> None:
            while self.ticks:
                self.engine.tick(self.ticks.popleft())

    driver = ManualDriver(ns.engine)
    run_id = ns.engine.start("two_acts")
    driver.run_ticks()  # schedules both activity branches; suspends

    # Branch 1 (seq 1) finishes first and wins; branch 0 (seq 0) is cancelled.
    ns.engine.report_activity_result(run_id, 1, 6, None)
    driver.run_ticks()
    assert ns.store.get_run(run_id).result["value"] == {"idx": 1, "val": 6}
    assert ns.store.get_step(run_id, 0).status == "CANCELLED"

    # Branch 0's straggler result lands late — it's dropped, the winner is unchanged.
    ns.engine.report_activity_result(run_id, 0, 50, None)
    driver.run_ticks()
    assert ns.store.get_step(run_id, 0).status == "CANCELLED"
    assert ns.store.get_run(run_id).result["value"] == {"idx": 1, "val": 6}


def test_failing_branch_reraises(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("boom_or_timer")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "ActivityFailed"
    assert ns.store.get_step(run_id, 1).status == "CANCELLED"  # timer dropped


def test_losing_signal_wait_left_for_later(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("two_signals")
    ns.driver.run_until_idle()

    ns.engine.signal(run_id, "b", {"v": 2})  # branch 1 wins
    ns.driver.run_until_idle()
    run = ns.store.get_run(run_id)
    assert run.result["value"] == {"idx": 1, "val": {"v": 2}}
    assert ns.store.get_step(run_id, 0).status == "CANCELLED"  # branch 0 ("a") cancelled


def test_empty_select_raises(ns: SimpleNamespace) -> None:
    reg = ns.reg

    @workflow(name="bad", registry=reg)
    def bad(ctx) -> None:
        ctx.select()

    run_id = ns.engine.start("bad")
    ns.driver.run_until_idle()
    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "ValueError"
