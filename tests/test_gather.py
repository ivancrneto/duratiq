"""``ctx.gather`` launches several deferred activities in parallel and resumes
only once all have completed — surviving a crash mid-fan-out and failing fast if a
branch errors.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()
    calls = {"double": 0, "triple": 0, "negate": 0}

    @activity(name="double", registry=reg)
    def double(x: int) -> int:
        calls["double"] += 1
        return x * 2

    @activity(name="triple", registry=reg)
    def triple(x: int) -> int:
        calls["triple"] += 1
        return x * 3

    @activity(name="negate", registry=reg)
    def negate(x: int) -> int:
        calls["negate"] += 1
        return -x

    @workflow(name="fanout", registry=reg)
    def fanout(ctx, x: int) -> dict:
        a, b, c = ctx.gather(
            ctx.defer(double, x),
            ctx.defer(triple, x),
            ctx.defer(negate, x),
        )
        return {"a": a, "b": b, "c": c, "sum": a + b + c}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver, calls=calls)


def test_gather_returns_results_in_order(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("fanout", x=5)
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"a": 10, "b": 15, "c": -5, "sum": 20}
    assert ns.calls == {"double": 1, "triple": 1, "negate": 1}


def test_all_branches_dispatch_in_one_tick(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("fanout", x=5)

    assert ns.driver.step() == "tick"  # the single tick that schedules the whole fan-out
    # All three branches are queued together, before any has run.
    queued = [item for item in ns.driver.queue if item[0] == "activity"]
    assert len(queued) == 3
    assert ns.calls == {"double": 0, "triple": 0, "negate": 0}
    assert ns.store.get_run(run_id).status == "SUSPENDED"


def test_gather_resumes_only_after_all_branches_complete(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("fanout", x=5)
    ns.driver.step()  # tick: schedule all three branches, suspend

    # Run one branch and its follow-up tick: the gather is still not satisfied.
    ns.driver.step()  # one activity completes
    ns.driver.step()  # its tick replays -> 1 done, 2 pending -> suspend
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "COMPLETED"


def test_crash_after_branches_resume_without_re_executing(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("fanout", x=5)
    ns.driver.step()  # tick: schedule all three branches

    # Run all three branches but NOT the resume tick each one queued — those pending
    # ticks are what the crash throws away. (Recovering a branch whose *dispatch* was
    # lost mid-flight is the broker's redelivery job, not the local driver's.)
    assert ns.driver.step() == "activity"
    assert ns.driver.step() == "activity"
    assert ns.driver.step() == "activity"
    assert ns.calls == {"double": 1, "triple": 1, "negate": 1}
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    # CRASH: drop the engine + its queued resume ticks. A fresh engine re-ticks.
    engine2 = Engine(ns.reg, ns.store)
    driver2 = LocalDriver(engine2)
    driver2.request_tick(run_id)
    driver2.run_until_idle()

    # No branch re-ran — all results were replayed from history.
    assert ns.calls == {"double": 1, "triple": 1, "negate": 1}
    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"]["sum"] == 20


def test_gather_fails_fast_when_a_branch_errors() -> None:
    reg = Registry()

    @activity(name="ok", registry=reg)
    def ok() -> str:
        return "fine"

    @activity(name="boom", registry=reg)
    def boom() -> None:
        raise ValueError("nope")

    @workflow(name="fanout_fail", registry=reg)
    def fanout_fail(ctx) -> str:
        ctx.gather(ctx.defer(ok), ctx.defer(boom))
        return "unreachable"

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("fanout_fail")
    driver.run_until_idle()

    run = store.get_run(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "ActivityFailed"


def test_empty_gather_returns_empty_list() -> None:
    reg = Registry()

    @workflow(name="empty_gather", registry=reg)
    def empty_gather(ctx) -> dict:
        return {"res": ctx.gather()}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("empty_gather")
    driver.run_until_idle()

    run = store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"res": []}
