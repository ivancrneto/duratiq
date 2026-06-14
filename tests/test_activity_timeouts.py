"""Activity start-to-close timeouts: a dispatched activity that never reports back
is retried (a fresh dispatch + new deadline) while the retry budget lasts, then
failed — so a hung or lost activity can't wedge its run forever.

A ``CollectingDriver`` records dispatches without running them, which is exactly the
"worker took the message and never reported" situation the timeout guards against.
"""

from __future__ import annotations

from collections import deque
from datetime import timedelta
from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.models import utcnow


class CollectingDriver:
    """Records ticks and activity dispatches; runs ticks on demand, never activities."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        engine.driver = self
        self.ticks: deque[str] = deque()
        self.dispatches: list[tuple] = []

    def request_tick(self, run_id: str) -> None:
        self.ticks.append(run_id)

    def dispatch_activity(self, run_id, seq, name, args, kwargs, max_retries) -> None:  # noqa: ANN001
        self.dispatches.append((run_id, seq, name, args, kwargs, max_retries))

    def run_ticks(self) -> None:
        while self.ticks:
            self.engine.tick(self.ticks.popleft())


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()

    @activity(name="slow", registry=reg, max_retries=2, start_to_close_ms=1000)
    def slow() -> str:
        return "done"

    @activity(name="quick", registry=reg)  # no timeout
    def quick() -> str:
        return "ok"

    @workflow(name="uses_slow", registry=reg)
    def uses_slow(ctx) -> dict:
        return {"r": ctx.activity(slow)}

    @workflow(name="uses_quick", registry=reg)
    def uses_quick(ctx) -> dict:
        return {"r": ctx.activity(quick)}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = CollectingDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver)


def _scheduled_step(ns: SimpleNamespace, run_id: str, seq: int = 0):
    return ns.store.get_step(run_id, seq)


def test_timeout_retries_then_fails(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("uses_slow")
    ns.driver.run_ticks()  # schedules + dispatches the activity; run suspends
    assert len(ns.driver.dispatches) == 1
    assert _scheduled_step(ns, run_id).status == "SCHEDULED"

    base = utcnow()
    # Before the deadline: nothing times out.
    assert ns.engine.fire_due_activity_timeouts(now=base) == 0

    # First timeout -> retry (attempt 1), a fresh dispatch, new deadline.
    assert ns.engine.fire_due_activity_timeouts(now=base + timedelta(seconds=2)) == 1
    assert len(ns.driver.dispatches) == 2
    assert _scheduled_step(ns, run_id).status == "SCHEDULED"
    assert _scheduled_step(ns, run_id).attempt == 1

    # Second timeout -> retry (attempt 2 == max_retries), third dispatch.
    assert ns.engine.fire_due_activity_timeouts(now=base + timedelta(seconds=4)) == 1
    assert len(ns.driver.dispatches) == 3
    assert _scheduled_step(ns, run_id).attempt == 2

    # Third timeout -> budget spent: the step fails and the run is re-ticked.
    assert ns.engine.fire_due_activity_timeouts(now=base + timedelta(seconds=6)) == 1
    assert _scheduled_step(ns, run_id).status == "FAILED"
    assert _scheduled_step(ns, run_id).error["type"] == "ActivityTimeout"

    ns.driver.run_ticks()  # the workflow replays and sees ActivityFailed
    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "ActivityFailed"


def test_activity_without_timeout_never_times_out(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("uses_quick")
    ns.driver.run_ticks()
    assert _scheduled_step(ns, run_id).timeout_at is None
    # Even far in the future, an activity with no start-to-close is left alone.
    assert ns.engine.fire_due_activity_timeouts(now=utcnow() + timedelta(days=365)) == 0
    assert _scheduled_step(ns, run_id).status == "SCHEDULED"


def test_result_before_deadline_beats_the_timeout(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("uses_slow")
    ns.driver.run_ticks()
    # The worker reports a result (the activity actually finished).
    ns.engine.report_activity_result(run_id, 0, "done", None, attempt=0)
    assert _scheduled_step(ns, run_id).status == "COMPLETED"

    # A later timeout scan finds nothing SCHEDULED to time out.
    assert ns.engine.fire_due_activity_timeouts(now=utcnow() + timedelta(seconds=10)) == 0

    ns.driver.run_ticks()
    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"r": "done"}


def test_timeout_emits_events(ns: SimpleNamespace) -> None:
    seen: list[tuple] = []
    ns.engine.listener = lambda e: seen.append((e.type, e.attempt))

    ns.engine.start("uses_slow")
    ns.driver.run_ticks()
    base = utcnow()
    ns.engine.fire_due_activity_timeouts(now=base + timedelta(seconds=2))  # retry
    ns.engine.fire_due_activity_timeouts(now=base + timedelta(seconds=4))  # retry
    ns.engine.fire_due_activity_timeouts(now=base + timedelta(seconds=6))  # fail

    types = [t for t, _ in seen]
    assert types.count("activity.timed_out") == 2
    assert "activity.failed" in types
