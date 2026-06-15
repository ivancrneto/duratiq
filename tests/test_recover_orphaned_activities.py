"""recover_stalled(redispatch_orphaned_activities=True): re-dispatch an untimed
activity whose dispatch was lost in the commit->enqueue window.

This is the one lost-activity case neither the broker (nothing was enqueued) nor the
activity-timeout scanner (no deadline) can recover. It's opt-in: the default re-tick
leaves such a run stuck, matching the documented "broker owns redelivery" behaviour."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()
    calls = {"untimed": 0, "timed": 0}

    @activity(name="untimed", registry=reg)
    def untimed(x: int) -> int:
        calls["untimed"] += 1
        return x + 1

    @activity(name="timed", registry=reg, start_to_close_ms=30_000)
    def timed(x: int) -> int:
        calls["timed"] += 1
        return x + 1

    @workflow(name="wf", registry=reg)
    def wf(ctx, start: int) -> dict:
        return {"v": ctx.activity(untimed, start)}

    @workflow(name="timed_wf", registry=reg)
    def timed_wf(ctx, start: int) -> dict:
        return {"v": ctx.activity(timed, start)}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver, calls=calls)


def _lose_pending_dispatch(ns: SimpleNamespace, workflow_name: str, start: int = 10) -> str:
    """Start a run, let the first tick schedule its activity, then drop the in-memory
    dispatch — simulating a worker that crashed after committing the step but before
    the broker enqueued the message."""
    run_id = ns.engine.start(workflow_name, start=start)
    assert ns.driver.step() == "tick"  # schedules the activity, suspends
    ns.driver.queue.clear()  # the activity dispatch is lost
    run = ns.store.get_run(run_id)
    assert run.status == "SUSPENDED"
    assert [s.status for s in ns.store.get_steps(run_id) if s.kind == "ACTIVITY"] == ["SCHEDULED"]
    return run_id


def test_default_recovery_leaves_orphan_stuck(ns: SimpleNamespace) -> None:
    run_id = _lose_pending_dispatch(ns, "wf")

    # A bare re-tick can't recover a lost dispatch: it replays, sees the activity
    # still SCHEDULED, and re-suspends.
    assert ns.engine.recover_stalled(older_than_seconds=0) == 1
    ns.driver.run_until_idle()

    assert ns.store.get_run(run_id).status == "SUSPENDED"
    assert ns.calls["untimed"] == 0


def test_opt_in_recovery_redispatches_orphan(ns: SimpleNamespace) -> None:
    run_id = _lose_pending_dispatch(ns, "wf")

    assert ns.engine.recover_stalled(older_than_seconds=0, redispatch_orphaned_activities=True) == 1
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"v": 11}
    assert ns.calls["untimed"] == 1  # ran exactly once


def test_redispatch_skips_timed_activities(ns: SimpleNamespace) -> None:
    # An activity with a start-to-close timeout is the timeout scanner's job; recovery
    # must not also re-dispatch it (that would double up with the scanner).
    run_id = _lose_pending_dispatch(ns, "timed_wf")

    assert ns.engine.recover_stalled(older_than_seconds=0, redispatch_orphaned_activities=True) == 1
    ns.driver.run_until_idle()

    # The timed activity was left for the timeout scanner, so recovery didn't run it.
    assert ns.calls["timed"] == 0
    assert ns.store.get_run(run_id).status == "SUSPENDED"


def test_redispatch_is_noop_for_a_genuinely_waiting_run(ns: SimpleNamespace) -> None:
    # A run parked on a signal (no orphaned activity) is unaffected by the option.
    reg = ns.reg

    @workflow(name="waiter", registry=reg)
    def waiter(ctx) -> str:
        return ctx.wait_signal("go")

    run_id = ns.engine.start("waiter")
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    assert ns.engine.recover_stalled(older_than_seconds=0, redispatch_orphaned_activities=True) == 1
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "SUSPENDED"  # still waiting; nothing re-dispatched

    ns.engine.signal(run_id, "go", "done")
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).result["value"] == "done"


def test_completed_activity_is_not_redispatched(ns: SimpleNamespace) -> None:
    # If the activity actually completed, recovery must not re-dispatch it.
    run_id = ns.engine.start("wf", start=5)
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "COMPLETED"
    assert ns.calls["untimed"] == 1

    # Completed run is terminal: not stalled, nothing to redispatch.
    assert ns.engine.recover_stalled(older_than_seconds=0, redispatch_orphaned_activities=True) == 0
    assert ns.calls["untimed"] == 1
