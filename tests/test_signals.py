"""Signals: ``ctx.wait_signal`` parks a run until ``engine.signal`` delivers a
matching payload — whether the signal arrives after the wait or before it (the
queued-signal race) — and the consumed payload is stable across replay and crash.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()
    calls = {"finalize": 0}

    @activity(name="finalize", registry=reg)
    def finalize(order_id: str, approved: bool) -> str:
        calls["finalize"] += 1
        return f"{order_id}:{'ok' if approved else 'rejected'}"

    @workflow(name="approval", registry=reg)
    def approval(ctx, order_id: str) -> dict:
        decision = ctx.wait_signal("review")
        outcome = ctx.activity(finalize, order_id, decision["approved"])
        return {"outcome": outcome}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver, calls=calls)


def test_signal_after_wait_resumes_run(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("approval", order_id="A1")
    ns.driver.run_until_idle()

    # Parked on the wait; downstream activity hasn't run.
    assert ns.store.get_run(run_id).status == "SUSPENDED"
    assert ns.calls["finalize"] == 0

    assert ns.engine.signal(run_id, "review", {"approved": True}) is True
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"outcome": "A1:ok"}
    assert ns.calls["finalize"] == 1


def test_signal_before_wait_is_queued_and_consumed(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("approval", order_id="A2")
    # Deliver the signal before the workflow has even ticked to the wait.
    assert ns.engine.signal(run_id, "review", {"approved": False}) is True
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"outcome": "A2:rejected"}
    assert ns.calls["finalize"] == 1


def test_wrong_name_does_not_wake_the_wait(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("approval", order_id="A3")
    ns.driver.run_until_idle()

    ns.engine.signal(run_id, "unrelated", {"approved": True})
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    # The matching signal still wakes it; the unrelated one stays queued, unconsumed.
    ns.engine.signal(run_id, "review", {"approved": True})
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "COMPLETED"


def test_signal_to_terminal_run_is_rejected(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("approval", order_id="A4")
    ns.driver.run_until_idle()
    ns.engine.signal(run_id, "review", {"approved": True})
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "COMPLETED"

    assert ns.engine.signal(run_id, "review", {"approved": True}) is False


def test_payload_is_stable_across_crash_and_replay(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("approval", order_id="A5")
    ns.driver.run_until_idle()
    ns.engine.signal(run_id, "review", {"approved": True})

    # CRASH before the post-signal tick is pumped: drop the engine + its queue.
    engine2 = Engine(ns.reg, ns.store)
    driver2 = LocalDriver(engine2)
    driver2.request_tick(run_id)
    driver2.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"outcome": "A5:ok"}
    assert ns.calls["finalize"] == 1
