"""Hardening: the correctness invariants that the whole engine rests on.

Three properties from DURATIQ_MVP_PLAN.md §6/§9:

* **Single-writer per run** (the plan's "#1 correctness requirement"): two ticks for
  the same run can be in flight at once (an activity-completion tick racing a
  recovery tick). The per-run lock must let exactly one advance — never run a
  not-yet-recorded side effect twice.
* **Crash-resume at every step boundary**: killing the worker after any step and
  resuming on a fresh engine produces an identical result, and no activity re-runs.
* **Load**: a thousand concurrent runs all complete, each activity firing exactly
  the expected number of times.
"""

from __future__ import annotations

import threading
import time

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.pool import StaticPool

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import WorkflowRun, WorkflowStep


def _redeliver_scheduled_activities(engine: Engine, driver: LocalDriver, store: SqlStore, run_id: str) -> None:
    """Re-dispatch any still-SCHEDULED activity for a run.

    Stands in for the broker's at-least-once redelivery: the LocalDriver keeps
    activity messages only in memory, so a crash loses an in-flight dispatch. In
    production the broker redelivers it; the recovery scanner (a bare re-tick) does
    not, because it sees the step already SCHEDULED and suspends.
    """
    for step in store.get_steps(run_id):
        if step.kind == "ACTIVITY" and step.status == "SCHEDULED":
            act = engine.registry.get_activity(step.name)
            inp = step.input or {}
            driver.dispatch_activity(
                run_id, step.seq, step.name, inp.get("args", []), inp.get("kwargs", {}), act.max_retries
            )


def _shared_store() -> SqlStore:
    """A SQLite store safe to touch from several threads (one shared connection).

    This is the dev/test setup the in-process per-run lock guards; the Postgres
    advisory-lock path provides the same guarantee in production.
    """
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool, future=True)
    store = SqlStore(engine=engine)
    store.create_all()
    return store


# --------------------------------------------------------------- single writer
def test_concurrent_ticks_advance_a_run_exactly_once() -> None:
    reg = Registry()
    bumps = {"n": 0}

    def bump() -> int:
        # A non-idempotent side effect with a widened window: if two ticks ran it
        # concurrently (no lock), the count would reach 2.
        n = bumps["n"] + 1
        time.sleep(0.05)
        bumps["n"] = n
        return n

    @workflow(name="wf", registry=reg)
    def wf(ctx) -> str:
        ctx.side_effect(bump)  # seq 0 — recorded once, on the first tick to win
        ctx.wait_signal("go")  # seq 1 — then the run parks, staying re-tickable
        return "done"

    store = _shared_store()
    engine = Engine(reg, store)
    LocalDriver(engine)
    run_id = engine.start("wf")  # PENDING; the queued tick is never pumped

    # Two threads fire the very first tick for this run at the same instant.
    barrier = threading.Barrier(2)

    def race() -> None:
        barrier.wait()
        engine.tick(run_id)

    threads = [threading.Thread(target=race) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Exactly one tick advanced the run: the side effect ran once and is recorded once.
    assert bumps["n"] == 1
    with store.Session() as s:
        side_effects = s.scalar(
            select(func.count()).select_from(WorkflowStep).where(WorkflowStep.kind == "SIDE_EFFECT")
        )
    assert side_effects == 1
    assert store.get_run(run_id).status == "SUSPENDED"


def test_concurrent_completion_and_recovery_tick_dont_double_run() -> None:
    # The plan's exact scenario: an activity-completion tick and a recovery tick land
    # together. The activity must not be dispatched/run twice.
    reg = Registry()
    runs = {"n": 0}

    @activity(name="act", registry=reg)
    def act() -> int:
        runs["n"] += 1
        return runs["n"]

    @workflow(name="wf2", registry=reg)
    def wf2(ctx) -> dict:
        first = ctx.activity(act)
        ctx.wait_signal("go")
        return {"first": first}

    store = _shared_store()
    engine = Engine(reg, store)
    LocalDriver(engine)
    run_id = engine.start("wf2")
    engine.driver.run_until_idle()  # runs the activity once, parks on the signal
    assert runs["n"] == 1

    # Race two re-ticks (as a lost-tick recovery + a stray re-delivery would).
    barrier = threading.Barrier(2)

    def race() -> None:
        barrier.wait()
        engine.tick(run_id)

    threads = [threading.Thread(target=race) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # The completed activity is memoized — replaying it twice never re-runs it.
    assert runs["n"] == 1
    assert store.get_run(run_id).status == "SUSPENDED"


# ------------------------------------------------------------ crash resume
@pytest.mark.parametrize("crash_after", range(0, 9))
def test_crash_at_every_boundary_resumes_identically(crash_after: int) -> None:
    reg = Registry()
    calls = {"a": 0, "b": 0, "c": 0}

    @activity(name="a", registry=reg)
    def a(x: int) -> int:
        calls["a"] += 1
        return x + 1

    @activity(name="b", registry=reg)
    def b(x: int) -> int:
        calls["b"] += 1
        return x * 2

    @activity(name="c", registry=reg)
    def c(x: int) -> int:
        calls["c"] += 1
        return x - 3

    @workflow(name="pipeline", registry=reg)
    def pipeline(ctx, start: int) -> dict:
        va = ctx.activity(a, start)
        vb = ctx.activity(b, va)
        vc = ctx.activity(c, vb)
        return {"a": va, "b": vb, "c": vc}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    run_id = engine.start("pipeline", start=5)

    # Pump some prefix of the work, then "crash": throw away the engine + its queue.
    for _ in range(crash_after):
        if driver.step() is None:
            break

    # Resume on a fresh engine over the same store. Full recovery is two mechanisms:
    # the broker redelivers any in-flight activity message, and the recovery scanner
    # re-ticks the run. Together they resume from any boundary.
    engine2 = Engine(reg, store)
    driver2 = LocalDriver(engine2)
    _redeliver_scheduled_activities(engine2, driver2, store, run_id)  # broker redelivery
    driver2.request_tick(run_id)  # recovery scanner
    driver2.run_until_idle()

    run = store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"a": 6, "b": 12, "c": 9}
    # Memoization holds across the crash: each activity ran exactly once.
    assert calls == {"a": 1, "b": 1, "c": 1}


# -------------------------------------------------------------------- load
def test_thousand_concurrent_runs_all_complete() -> None:
    reg = Registry()
    calls = {"n": 0}

    @activity(name="inc", registry=reg)
    def inc(x: int) -> int:
        calls["n"] += 1
        return x + 1

    @workflow(name="twostep", registry=reg)
    def twostep(ctx, start: int) -> int:
        return ctx.activity(inc, ctx.activity(inc, start))

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    N = 1000
    run_ids = [engine.start("twostep", start=i) for i in range(N)]
    driver.run_until_idle()

    with store.Session() as s:
        completed = s.scalar(select(func.count()).select_from(WorkflowRun).where(WorkflowRun.status == "COMPLETED"))
    assert completed == N
    # Two activities per run, each run exactly once.
    assert calls["n"] == 2 * N
    # Spot-check a few results.
    for i in (0, 1, N // 2, N - 1):
        assert store.get_run(run_ids[i]).result["value"] == i + 2
