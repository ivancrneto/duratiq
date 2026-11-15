"""End-to-end: the per-run lock really serialises ticks under concurrency.

``SqlStore.locked_run`` is the primitive the whole engine leans on: two ticks of the
same run must never interleave, or a step can be scheduled (and its activity
dispatched) twice. On PostgreSQL that is a transaction-scoped
``pg_advisory_xact_lock``; on SQLite an in-process lock. Both are exercised here with
real threads against a real database.
"""

from __future__ import annotations

import threading
import time
from collections import Counter

import pytest

from duratiq import Engine, Registry, activity, workflow

HOLD = 0.4


def _hold_lock(store, run_id: str, entered: threading.Event, release: threading.Event) -> None:
    with store.locked_run(run_id):
        entered.set()
        release.wait(timeout=10)


def _time_to_acquire(store, run_id: str) -> float:
    started = time.monotonic()
    with store.locked_run(run_id):
        return time.monotonic() - started


def test_same_run_is_serialised_other_runs_are_not(backend) -> None:
    _, store = backend
    entered, release = threading.Event(), threading.Event()
    holder = threading.Thread(target=_hold_lock, args=(store, "run-a", entered, release))
    holder.start()
    try:
        assert entered.wait(timeout=10)
        # A different run is not blocked by run-a's lock.
        assert _time_to_acquire(store, "run-b") < HOLD / 2

        waited: list[float] = []
        contender = threading.Thread(target=lambda: waited.append(_time_to_acquire(store, "run-a")))
        contender.start()
        time.sleep(HOLD)
        assert contender.is_alive(), "a second locker of the same run got in while the first held it"
        release.set()
        contender.join(timeout=10)
        assert waited and waited[0] >= HOLD
    finally:
        release.set()
        holder.join(timeout=10)


def test_lock_is_released_when_the_tick_raises(backend) -> None:
    _, store = backend
    with pytest.raises(RuntimeError, match="tick blew up"):
        with store.locked_run("run-c"):
            raise RuntimeError("tick blew up")
    assert _time_to_acquire(store, "run-c") < HOLD


def test_tick_storm_on_one_run_schedules_each_step_once(backend) -> None:
    """Many workers ticking the same run at once must advance it exactly once."""
    name, store = backend
    reg = Registry()

    @activity(name="charge", registry=reg)
    def charge(order_id: str) -> str:
        return f"pay-{order_id}"

    @workflow(name="pay", registry=reg)
    def pay(ctx, order_id: str) -> str:
        return ctx.activity(charge, order_id)

    class RecordingDriver:
        def __init__(self) -> None:
            self.lock = threading.Lock()
            self.dispatched: Counter = Counter()

        def request_tick(self, run_id: str) -> None:
            pass  # the storm below is the only source of ticks

        def dispatch_activity(self, run_id, seq, name, args, kwargs, max_retries) -> None:
            with self.lock:
                self.dispatched[(run_id, seq, name)] += 1

    driver = RecordingDriver()
    engine = Engine(reg, store, driver)
    run_id = engine.start("pay", order_id="X1")

    workers = 12
    barrier = threading.Barrier(workers)
    errors: list[BaseException] = []

    def tick() -> None:
        try:
            barrier.wait(timeout=10)
            engine.tick(run_id)
        except BaseException as exc:  # noqa: BLE001 - surface any failure to the test thread
            errors.append(exc)

    threads = [threading.Thread(target=tick) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not errors, errors
    assert driver.dispatched == {(run_id, 0, "charge"): 1}
    steps = store.get_steps(run_id)
    assert [(step.seq, step.kind, step.status) for step in steps] == [(0, "ACTIVITY", "SCHEDULED")]
    assert store.get_run(run_id).status == "SUSPENDED"

    # And the run then completes normally from that single scheduled step.
    engine.report_activity_result(run_id, 0, "pay-X1", None)
    engine.tick(run_id)
    run = store.get_run(run_id)
    assert (run.status, run.result["value"]) == ("COMPLETED", "pay-X1")
