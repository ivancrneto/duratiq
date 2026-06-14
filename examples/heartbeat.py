"""Activity heartbeats: a long-running activity proves liveness and records progress.

An activity declared with ``heartbeat_timeout_ms`` must call ``heartbeat()`` at least
that often. Each beat pushes the timeout deadline out (so a beating activity is never
wrongly timed out) and records progress; a retry reads it back with
``heartbeat_details()`` and resumes instead of starting over.

This demo drives the parts by hand — it dispatches the activity, beats progress, then
fast-forwards the activity-timeout scanner past the deadline — to show the retry pick
up where the previous attempt stopped.

    cd duratiq && python -m examples.heartbeat
"""

from __future__ import annotations

from collections import deque
from datetime import timedelta

from duratiq import Engine, Registry, SqlStore, activity, heartbeat, heartbeat_details, workflow
from duratiq.activity_runtime import activity_scope
from duratiq.models import utcnow

reg = Registry()
HEARTBEAT_MS = 60_000


@activity(name="reindex", registry=reg, heartbeat_timeout_ms=HEARTBEAT_MS, max_retries=3)
def reindex(total: int) -> str:
    start = heartbeat_details() or 0  # resume from the last reported position
    for i in range(start, total):
        # ... do a chunk of work ...
        heartbeat(i + 1)  # report progress + stay alive
    return f"reindexed {total}"


@workflow(name="reindex_job", registry=reg)
def reindex_job(ctx, total: int) -> dict:
    return {"status": ctx.activity(reindex, total)}


class HandDriver:
    """Records ticks + dispatches; runs ticks but not activities (so we can interpose)."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        engine.driver = self
        self.ticks: deque[str] = deque()

    def request_tick(self, run_id: str) -> None:
        self.ticks.append(run_id)

    def dispatch_activity(self, *a: object) -> None:  # we run the body ourselves below
        pass

    def run_ticks(self) -> None:
        while self.ticks:
            self.engine.tick(self.ticks.popleft())


def main() -> None:
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = HandDriver(engine)

    run_id = engine.start("reindex_job", total=5)
    driver.run_ticks()  # dispatches reindex; deadline set from the heartbeat timeout
    print("reindex dispatched (heartbeat timeout 60s)\n")

    # Attempt 1: worker reindexes 2 of 5 items (beating progress), then "hangs".
    with activity_scope(run_id, 0, store, heartbeat_timeout_ms=HEARTBEAT_MS):
        for i in range(2):
            heartbeat(i + 1)
    print(f"  attempt 1 beat progress -> {store.get_step(run_id, 0).heartbeat['value']} of 5, then went silent")

    # The scanner fires past the deadline: the attempt is retried, progress preserved.
    engine.fire_due_activity_timeouts(now=utcnow() + timedelta(minutes=5))
    driver.ticks.clear()
    step = store.get_step(run_id, 0)
    print(f"  timed out -> retry (attempt {step.attempt}); kept progress {step.heartbeat['value']}\n")

    # Attempt 2: resumes from item 2 and finishes the remaining items.
    with activity_scope(run_id, 0, store, heartbeat_timeout_ms=HEARTBEAT_MS):
        resumed_from = heartbeat_details()
        result = reindex(5)
    engine.report_activity_result(run_id, 0, result, None, attempt=step.attempt)
    driver.run_ticks()

    print(f"  attempt 2 resumed from {resumed_from} and finished")
    print(f"\nrun -> {engine.get(run_id).status}: {engine.get(run_id).result['value']}")


if __name__ == "__main__":
    main()
