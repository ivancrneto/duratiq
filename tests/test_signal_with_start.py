"""Engine.signal_with_start — start a run if absent, then deliver a signal to it.

The "signal-with-start" pattern: dedupe on an idempotency key so the first call
starts the per-entity workflow and every later call just signals the running one.
The signal is queued before the first tick, so the run's ``ctx.wait_signal`` finds
it already waiting — no race against the start."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import func, select

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import WorkflowRun


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()

    @activity(name="record", registry=reg)
    def record(item: str) -> str:
        return f"recorded:{item}"

    @workflow(name="cart", registry=reg)
    def cart(ctx) -> dict:
        first = ctx.wait_signal("add_item")
        return ctx.activity(record, first)

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver)


def _run_count(store: SqlStore) -> int:
    with store.Session() as s:
        return s.scalar(select(func.count()).select_from(WorkflowRun))


def test_signal_with_start_starts_and_delivers(ns: SimpleNamespace) -> None:
    run_id = ns.engine.signal_with_start("cart", signal="add_item", payload="apple")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "recorded:apple"
    assert _run_count(ns.store) == 1


def test_signal_with_start_dedupes_on_idempotency_key(ns: SimpleNamespace) -> None:
    # Two waits so the run stays alive long enough to receive the second signal.
    @workflow(name="two_step_cart", registry=ns.reg)
    def two_step_cart(ctx) -> list:
        a = ctx.wait_signal("add_item")
        b = ctx.wait_signal("add_item")
        return [a, b]

    first = ns.engine.signal_with_start("two_step_cart", signal="add_item", payload="apple", idempotency_key="cart-7")
    # Pump the first signal through so the run is suspended on the second wait.
    ns.driver.run_until_idle()

    second = ns.engine.signal_with_start("two_step_cart", signal="add_item", payload="pear", idempotency_key="cart-7")
    ns.driver.run_until_idle()

    assert first == second  # same run, not a second one
    assert _run_count(ns.store) == 1
    run = ns.store.get_run(first)
    assert run.status == "COMPLETED"
    assert run.result["value"] == ["apple", "pear"]


def test_signal_with_start_without_key_always_starts(ns: SimpleNamespace) -> None:
    a = ns.engine.signal_with_start("cart", signal="add_item", payload="x")
    b = ns.engine.signal_with_start("cart", signal="add_item", payload="y")
    ns.driver.run_until_idle()

    assert a != b
    assert _run_count(ns.store) == 2
    assert ns.store.get_run(a).result["value"] == "recorded:x"
    assert ns.store.get_run(b).result["value"] == "recorded:y"


def test_signal_with_start_unknown_workflow_raises(ns: SimpleNamespace) -> None:
    from duratiq import WorkflowNotFound

    with pytest.raises(WorkflowNotFound):
        ns.engine.signal_with_start("nope", signal="x")
