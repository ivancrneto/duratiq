"""Queries: read a running workflow's computed state without advancing it.

A workflow registers read-only handlers with ``ctx.set_query_handler``;
``engine.query`` replays it side-effect-free and calls the named handler, which is a
closure over the workflow's locals — so it reflects every signal processed so far.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, QueryNotFound, Registry, SqlStore, WorkflowNotFound, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()

    @workflow(name="cart", registry=reg)
    def cart(ctx) -> dict:
        items: list[str] = []
        status = {"state": "open"}
        ctx.set_query_handler("item_count", lambda: len(items))
        ctx.set_query_handler("items", lambda: list(items))
        ctx.set_query_handler("state", lambda: status["state"])
        ctx.set_query_handler("has", lambda sku: sku in items)
        while True:
            event = ctx.wait_signal("event")
            if event["type"] == "checkout":
                status["state"] = "checked_out"
                return {"items": list(items)}
            items.append(event["sku"])

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver)


def _add(ns: SimpleNamespace, run_id: str, sku: str) -> None:
    ns.engine.signal(run_id, "event", {"type": "add", "sku": sku})
    ns.driver.run_until_idle()


def test_query_reflects_processed_signals(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("cart")
    ns.driver.run_until_idle()

    assert ns.engine.query(run_id, "item_count") == 0
    assert ns.engine.query(run_id, "items") == []

    _add(ns, run_id, "A")
    assert ns.engine.query(run_id, "item_count") == 1
    assert ns.engine.query(run_id, "items") == ["A"]

    _add(ns, run_id, "B")
    assert ns.engine.query(run_id, "item_count") == 2
    assert ns.engine.query(run_id, "items") == ["A", "B"]
    assert ns.engine.query(run_id, "state") == "open"


def test_query_passes_arguments(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("cart")
    ns.driver.run_until_idle()
    _add(ns, run_id, "A")

    assert ns.engine.query(run_id, "has", "A") is True
    assert ns.engine.query(run_id, "has", "Z") is False


def test_query_does_not_advance_the_run(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("cart")
    ns.driver.run_until_idle()
    _add(ns, run_id, "A")

    before_status = ns.store.get_run(run_id).status
    before_steps = len(ns.store.get_steps(run_id))
    before_updated = ns.store.get_run(run_id).updated_at

    ns.engine.query(run_id, "item_count")
    ns.engine.query(run_id, "items")

    assert ns.store.get_run(run_id).status == before_status
    assert len(ns.store.get_steps(run_id)) == before_steps
    assert ns.store.get_run(run_id).updated_at == before_updated


def test_query_on_completed_workflow(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("cart")
    ns.driver.run_until_idle()
    _add(ns, run_id, "A")
    ns.engine.signal(run_id, "event", {"type": "checkout"})
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "COMPLETED"

    # Handlers are re-registered on the replay-to-completion, so queries still answer.
    assert ns.engine.query(run_id, "state") == "checked_out"
    assert ns.engine.query(run_id, "items") == ["A"]


def test_unknown_handler_raises(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("cart")
    ns.driver.run_until_idle()
    with pytest.raises(QueryNotFound) as exc:
        ns.engine.query(run_id, "nope")
    assert "item_count" in exc.value.available


def test_unknown_run_raises(ns: SimpleNamespace) -> None:
    with pytest.raises(WorkflowNotFound):
        ns.engine.query("does-not-exist", "item_count")
