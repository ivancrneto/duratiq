"""Search attributes: typed, indexed metadata on runs that ``list_runs`` filters on.

Set at start (``engine.start(search_attributes=...)``) or from inside the workflow
(``ctx.upsert_search_attributes``); query with ``list_runs(search_attributes=...)``,
an AND of equality matches, and read back with ``engine.get_search_attributes``.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()

    @workflow(name="trivial", registry=reg)
    def trivial(ctx, x: int) -> dict:
        return {"x": x}

    @workflow(name="lifecycle", registry=reg)
    def lifecycle(ctx) -> dict:
        ctx.upsert_search_attributes({"stage": "created"})
        stage = ctx.wait_signal("advance")
        ctx.upsert_search_attributes({"stage": stage})
        return {"stage": stage}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver)


def _ids(runs: list) -> set[str]:
    return {r.id for r in runs}


def test_set_at_start_and_filter(ns: SimpleNamespace) -> None:
    r1 = ns.engine.start("trivial", x=1, search_attributes={"region": "eu", "priority": 1})
    r2 = ns.engine.start("trivial", x=2, search_attributes={"region": "us", "priority": 1})
    ns.driver.run_until_idle()

    assert _ids(ns.engine.list_runs(search_attributes={"region": "eu"})) == {r1}
    assert _ids(ns.engine.list_runs(search_attributes={"region": "us"})) == {r2}
    assert _ids(ns.engine.list_runs(search_attributes={"priority": 1})) == {r1, r2}
    # Multiple attributes AND together.
    assert _ids(ns.engine.list_runs(search_attributes={"region": "us", "priority": 1})) == {r2}
    assert ns.engine.count_runs(search_attributes={"priority": 1}) == 2
    assert ns.engine.get_search_attributes(r1) == {"region": "eu", "priority": 1}


def test_typed_equality(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("trivial", x=1, search_attributes={"priority": 5})
    ns.driver.run_until_idle()

    assert _ids(ns.engine.list_runs(search_attributes={"priority": 5})) == {run_id}  # int matches
    assert ns.engine.list_runs(search_attributes={"priority": "5"}) == []  # str 5 does not


def test_combines_with_status_and_name(ns: SimpleNamespace) -> None:
    keep = ns.engine.start("trivial", x=1, search_attributes={"region": "eu"})
    ns.engine.start("trivial", x=2, search_attributes={"region": "eu"})  # different (no match on name? same name)
    ns.driver.run_until_idle()

    runs = ns.engine.list_runs(status="COMPLETED", name="trivial", search_attributes={"region": "eu"})
    assert keep in _ids(runs)
    assert _ids(ns.engine.list_runs(status="FAILED", search_attributes={"region": "eu"})) == set()


def test_upsert_from_within_workflow(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("lifecycle")
    ns.driver.run_until_idle()
    assert ns.engine.get_search_attributes(run_id) == {"stage": "created"}
    assert _ids(ns.engine.list_runs(search_attributes={"stage": "created"})) == {run_id}

    ns.engine.signal(run_id, "advance", "shipped")
    ns.driver.run_until_idle()
    # The key was replaced, not duplicated.
    assert ns.engine.get_search_attributes(run_id) == {"stage": "shipped"}
    assert _ids(ns.engine.list_runs(search_attributes={"stage": "shipped"})) == {run_id}
    assert ns.engine.list_runs(search_attributes={"stage": "created"}) == []


def test_no_attributes_and_no_match(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("trivial", x=1)
    ns.driver.run_until_idle()
    assert ns.engine.get_search_attributes(run_id) == {}
    assert ns.engine.list_runs(search_attributes={"region": "nowhere"}) == []
