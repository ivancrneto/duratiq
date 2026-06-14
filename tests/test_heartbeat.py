"""Activity heartbeats: a long-running activity proves liveness and records progress.

Each ``heartbeat()`` pushes the activity's timeout deadline out (so a beating activity
is never timed out) and stores progress that a retry reads back via
``heartbeat_details()`` to resume. Built on the activity-timeout scanner from #14.
"""

from __future__ import annotations

from collections import deque
from datetime import timedelta
from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, heartbeat, heartbeat_details, workflow
from duratiq.activity_runtime import activity_scope
from duratiq.models import utcnow


class CollectingDriver:
    """Records ticks and dispatches; runs ticks on demand, never activities."""

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

    @activity(name="proc", registry=reg, heartbeat_timeout_ms=60_000, max_retries=2)
    def proc() -> str:
        return "done"

    @workflow(name="job", registry=reg)
    def job(ctx) -> dict:
        return {"r": ctx.activity(proc)}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = CollectingDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver)


def _dispatch(ns: SimpleNamespace) -> str:
    run_id = ns.engine.start("job")
    ns.driver.run_ticks()  # schedules + dispatches proc; sets the heartbeat deadline
    return run_id


def test_heartbeat_records_details_and_pushes_deadline(ns: SimpleNamespace) -> None:
    run_id = _dispatch(ns)
    before = ns.store.get_step(run_id, 0).timeout_at

    with activity_scope(run_id, 0, ns.store, heartbeat_timeout_ms=60_000):
        heartbeat({"processed": 42})

    step = ns.store.get_step(run_id, 0)
    assert step.heartbeat == {"value": {"processed": 42}}
    # The deadline was pushed out (later than the dispatch-time deadline).
    assert step.timeout_at >= before


def test_heartbeat_details_reads_back_within_scope(ns: SimpleNamespace) -> None:
    run_id = _dispatch(ns)
    with activity_scope(run_id, 0, ns.store, heartbeat_timeout_ms=60_000):
        assert heartbeat_details() is None  # nothing reported yet
        heartbeat({"cursor": "abc"})
        assert heartbeat_details() == {"cursor": "abc"}


def test_beating_activity_is_not_timed_out(ns: SimpleNamespace) -> None:
    run_id = _dispatch(ns)
    base = utcnow()

    # A beat near the original deadline keeps it alive (deadline -> beat + 60s).
    with activity_scope(run_id, 0, ns.store, heartbeat_timeout_ms=60_000):
        heartbeat({"processed": 10})

    # A scan a little later finds nothing due — the activity is still beating.
    assert ns.engine.fire_due_activity_timeouts(now=base + timedelta(seconds=30)) == 0
    assert ns.store.get_step(run_id, 0).status == "SCHEDULED"


def test_silent_activity_times_out_and_retry_keeps_progress(ns: SimpleNamespace) -> None:
    run_id = _dispatch(ns)
    with activity_scope(run_id, 0, ns.store, heartbeat_timeout_ms=60_000):
        heartbeat({"processed": 25})

    # Long after the last beat the deadline has passed -> a retry is dispatched...
    assert ns.engine.fire_due_activity_timeouts(now=utcnow() + timedelta(minutes=5)) == 1
    step = ns.store.get_step(run_id, 0)
    assert step.status == "SCHEDULED"
    assert step.attempt == 1
    # ...and the prior progress survives, so the retry resumes from it.
    assert step.heartbeat == {"value": {"processed": 25}}
    with activity_scope(run_id, 0, ns.store, heartbeat_timeout_ms=60_000):
        assert heartbeat_details() == {"processed": 25}


def test_heartbeat_outside_activity_raises() -> None:
    with pytest.raises(RuntimeError, match="must be called inside an activity"):
        heartbeat({"x": 1})
    with pytest.raises(RuntimeError, match="must be called inside an activity"):
        heartbeat_details()


def test_heartbeat_ignored_after_step_finished(ns: SimpleNamespace) -> None:
    run_id = _dispatch(ns)
    ns.engine.report_activity_result(run_id, 0, "done", None, attempt=0)
    assert ns.store.get_step(run_id, 0).status == "COMPLETED"

    # A late beat from a straggler attempt must not revive or mutate a finished step.
    with activity_scope(run_id, 0, ns.store, heartbeat_timeout_ms=60_000):
        heartbeat({"processed": 999})
    assert ns.store.get_step(run_id, 0).heartbeat is None
