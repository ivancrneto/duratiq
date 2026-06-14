"""ctx.continue_as_new — restart a workflow with fresh input and an empty history.

For long-running / looping workflows whose step history would otherwise grow
without bound. Each continue-as-new truncates the run's history and restarts it
from seq 0 with new input, keeping the same run id. Unconsumed signals carry over."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()
    calls = {"tick_log": 0}

    @activity(name="emit", registry=reg)
    def emit(n: int) -> int:
        calls["tick_log"] += 1
        return n

    @workflow(name="counter", registry=reg)
    def counter(ctx, n: int, limit: int) -> int:
        # Do one unit of (memoized) work each iteration, then either finish or
        # continue-as-new with the next n — discarding this iteration's history.
        ctx.activity(emit, n)
        if n >= limit:
            return n
        ctx.continue_as_new(n=n + 1, limit=limit)

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver, calls=calls)


def test_continue_as_new_loops_to_completion(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("counter", n=0, limit=3)
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == 3
    # The activity ran once per iteration: n = 0,1,2,3 -> four runs.
    assert ns.calls["tick_log"] == 4


def test_continue_as_new_truncates_history(ns: SimpleNamespace) -> None:
    # A run that suspends partway through an iteration should carry only the current
    # iteration's history — not an ever-growing pile from every prior iteration.
    reg = ns.reg

    @workflow(name="paged", registry=reg)
    def paged(ctx, page: int) -> int:
        ctx.activity(ns.reg.get_activity("emit"), page)  # one completed step
        token = ctx.wait_signal("next")  # suspends here each iteration
        if token == "STOP":
            return page
        ctx.continue_as_new(page=page + 1)

    run_id = ns.engine.start("paged", page=0)
    ns.driver.run_until_idle()  # runs the activity, then suspends on the signal

    # Drive three pages, checking history stays bounded across iterations.
    for expected_page in range(3):
        run = ns.store.get_run(run_id)
        assert run.status == "SUSPENDED"
        assert run.input == {"page": expected_page}
        steps = ns.store.get_steps(run_id)
        # Exactly this iteration's two steps: the ACTIVITY and the SIGNAL_WAIT.
        assert len(steps) == 2
        assert {s.kind for s in steps} == {"ACTIVITY", "SIGNAL_WAIT"}
        ns.engine.signal(run_id, "next", "GO")
        ns.driver.run_until_idle()

    ns.engine.signal(run_id, "next", "STOP")
    ns.driver.run_until_idle()
    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == 3


def test_unconsumed_signals_carry_over(ns: SimpleNamespace) -> None:
    # Signals queued before they are awaited must survive continue-as-new: a workflow
    # draining a queue one item per iteration should see every queued item.
    @workflow(name="drain", registry=ns.reg)
    def drain(ctx, seen: list) -> list:
        item = ctx.wait_signal("item")
        seen = seen + [item]
        if item == "STOP":
            return seen
        ctx.continue_as_new(seen=seen)

    run_id = ns.engine.start("drain", seen=[])
    # Queue every signal up front — they sit unconsumed until each iteration's wait.
    ns.engine.signal(run_id, "item", "a")
    ns.engine.signal(run_id, "item", "b")
    ns.engine.signal(run_id, "item", "STOP")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == ["a", "b", "STOP"]


def test_continue_as_new_survives_crash(ns: SimpleNamespace) -> None:
    # A run mid-loop (suspended after a continue-as-new) resumes on a fresh engine.
    @workflow(name="resumable", registry=ns.reg)
    def resumable(ctx, n: int) -> int:
        ctx.wait_signal("go")
        if n >= 2:
            return n
        ctx.continue_as_new(n=n + 1)

    run_id = ns.engine.start("resumable", n=0)
    ns.driver.run_until_idle()
    ns.engine.signal(run_id, "go", None)
    ns.driver.run_until_idle()  # iteration 0 -> continue-as-new -> suspended on n=1

    assert ns.store.get_run(run_id).input == {"n": 1}

    # CRASH: fresh engine on the same store.
    engine2 = Engine(ns.reg, ns.store)
    driver2 = LocalDriver(engine2)
    driver2.request_tick(run_id)  # recovery re-tick
    driver2.run_until_idle()
    assert ns.store.get_run(run_id).status == "SUSPENDED"  # still waiting on "go"

    engine2.signal(run_id, "go", None)
    driver2.run_until_idle()
    engine2.signal(run_id, "go", None)
    driver2.run_until_idle()
    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == 2
