"""``ctx.wait_signal(name, timeout=...)`` races a signal against a durable timer.

Whichever completes first wins; the loser is cancelled so it can't fire or match
later. Timers are advanced with ``fire_due_timers(now=...)`` so the tests fast-forward
instead of sleeping.
"""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest

from duratiq import TIMEOUT, Engine, Registry, SqlStore, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import utcnow


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()

    @workflow(name="review", registry=reg)
    def review(ctx) -> dict:
        decision = ctx.wait_signal("approval", timeout="PT1H")
        if decision is TIMEOUT:
            return {"outcome": "auto-rejected"}
        return {"outcome": "decided", "decision": decision}

    @workflow(name="twice", registry=reg)
    def twice(ctx) -> dict:
        first = ctx.wait_signal("go", timeout="PT1H")  # this one times out
        second = ctx.wait_signal("go")  # then waits (forever) for the next "go"
        return {"first": "timeout" if first is TIMEOUT else first, "second": second}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver)


def test_signal_before_timeout_wins(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("review")
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    ns.engine.signal(run_id, "approval", {"approved": True})
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"outcome": "decided", "decision": {"approved": True}}

    # The timer (seq 1) was cancelled and its due-time row removed.
    assert ns.store.get_step(run_id, 1).status == "CANCELLED"
    assert ns.engine.fire_due_timers(now=utcnow() + timedelta(hours=2)) == 0


def test_timeout_fires_when_no_signal(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("review")
    ns.driver.run_until_idle()

    # Before the deadline: nothing fires.
    assert ns.engine.fire_due_timers(now=utcnow()) == 0
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    # Past the deadline: the timer fires and the run takes the timeout branch.
    assert ns.engine.fire_due_timers(now=utcnow() + timedelta(hours=2)) == 1
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"outcome": "auto-rejected"}
    # The abandoned signal wait (seq 0) was cancelled.
    assert ns.store.get_step(run_id, 0).status == "CANCELLED"


def test_late_signal_is_not_consumed_by_a_timed_out_wait(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("twice")
    ns.driver.run_until_idle()

    # First wait times out -> the workflow advances to the second (open-ended) wait.
    ns.engine.fire_due_timers(now=utcnow() + timedelta(hours=2))
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "SUSPENDED"
    assert ns.store.get_step(run_id, 0).status == "CANCELLED"  # the timed-out wait

    # A signal arriving now must go to the live second wait, not the cancelled first.
    ns.engine.signal(run_id, "go", {"n": 2})
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"first": "timeout", "second": {"n": 2}}


def test_queued_signal_wins_over_timeout(ns: SimpleNamespace) -> None:
    # signal_with_start queues the signal before the first tick, so the wait finds it
    # already there and never reaches the timer.
    run_id = ns.engine.signal_with_start("review", signal="approval", payload={"approved": False})
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"outcome": "decided", "decision": {"approved": False}}
    assert ns.store.get_step(run_id, 1).status == "CANCELLED"  # timer never needed


def test_resolution_is_stable_on_replay(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("review")
    ns.driver.run_until_idle()
    ns.engine.fire_due_timers(now=utcnow() + timedelta(hours=2))
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).result["value"] == {"outcome": "auto-rejected"}

    # Re-ticking a terminal run is a no-op and never flips the recorded outcome.
    ns.engine.tick(run_id)
    assert ns.store.get_run(run_id).result["value"] == {"outcome": "auto-rejected"}
