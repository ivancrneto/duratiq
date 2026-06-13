"""``ctx.side_effect`` records a non-deterministic value once and replays it
verbatim — the wrapped function runs exactly once, even across a crash, so a
generated id or timestamp stays stable for the life of the run.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()
    counter = {"gen": 0}

    def gen_id() -> str:
        counter["gen"] += 1
        return f"id-{counter['gen']}"

    @activity(name="record", registry=reg)
    def record(request_id: str) -> str:
        return f"recorded:{request_id}"

    @workflow(name="with_id", registry=reg)
    def with_id(ctx) -> dict:
        request_id = ctx.side_effect(gen_id)
        out = ctx.activity(record, request_id)
        return {"request_id": request_id, "out": out}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver, counter=counter, gen_id=gen_id)


def test_side_effect_value_flows_into_activity(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("with_id")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"request_id": "id-1", "out": "recorded:id-1"}
    assert ns.counter["gen"] == 1


def test_side_effect_runs_once_across_crash(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("with_id")

    # Pump just far enough to compute the side effect + schedule the activity, then crash.
    assert ns.driver.step() == "tick"      # side_effect computed (id-1), activity scheduled, suspends
    assert ns.driver.step() == "activity"  # activity runs
    assert ns.counter["gen"] == 1
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    # CRASH: fresh engine on the same store re-ticks to finish.
    engine2 = Engine(ns.reg, ns.store)
    driver2 = LocalDriver(engine2)
    driver2.request_tick(run_id)
    driver2.run_until_idle()

    # gen_id was NOT called again — the value was replayed from the recorded step.
    assert ns.counter["gen"] == 1
    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"request_id": "id-1", "out": "recorded:id-1"}


def test_side_effect_completes_in_one_tick() -> None:
    reg = Registry()
    counter = {"gen": 0}

    def gen() -> str:
        counter["gen"] += 1
        return f"v{counter['gen']}"

    @workflow(name="just_value", registry=reg)
    def just_value(ctx) -> dict:
        return {"v": ctx.side_effect(gen)}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("just_value")
    driver.run_until_idle()

    run = store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"v": "v1"}
    assert counter["gen"] == 1
    # The value is persisted as a COMPLETED SIDE_EFFECT step.
    (step,) = store.get_steps(run_id)
    assert step.kind == "SIDE_EFFECT"
    assert step.status == "COMPLETED"
    assert step.result == {"value": "v1"}
