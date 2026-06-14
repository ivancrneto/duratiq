"""engine.list_runs / count_runs — the read side for enumerating runs.

Filter by status and/or workflow name, page with limit/offset, newest first."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()

    @activity(name="boom", registry=reg)
    def boom() -> None:
        raise RuntimeError("nope")

    @workflow(name="ok", registry=reg)
    def ok(ctx) -> str:
        return "done"

    @workflow(name="bad", registry=reg)
    def bad(ctx) -> str:
        ctx.activity(boom)
        return "unreachable"

    @workflow(name="waits", registry=reg)
    def waits(ctx) -> str:
        return ctx.wait_signal("go")

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine)


def test_empty_store_lists_nothing(ns: SimpleNamespace) -> None:
    assert ns.engine.list_runs() == []
    assert ns.engine.count_runs() == 0


def test_list_filters_by_status_and_name(ns: SimpleNamespace) -> None:
    completed = ns.engine.start("ok")
    failed = ns.engine.start("bad")
    suspended = ns.engine.start("waits")
    ns.engine.driver.run_until_idle()

    assert ns.engine.get(completed).status == "COMPLETED"
    assert ns.engine.get(failed).status == "FAILED"
    assert ns.engine.get(suspended).status == "SUSPENDED"

    assert ns.engine.count_runs() == 3
    assert {r.id for r in ns.engine.list_runs()} == {completed, failed, suspended}

    # Single-status filter.
    assert [r.id for r in ns.engine.list_runs(status="FAILED")] == [failed]
    assert ns.engine.count_runs(status="COMPLETED") == 1

    # Multi-status filter.
    ids = {r.id for r in ns.engine.list_runs(status=["FAILED", "SUSPENDED"])}
    assert ids == {failed, suspended}
    assert ns.engine.count_runs(status=["FAILED", "SUSPENDED"]) == 2

    # Name filter.
    assert [r.id for r in ns.engine.list_runs(name="ok")] == [completed]
    assert ns.engine.count_runs(name="waits") == 1

    # Combined filters that match nothing.
    assert ns.engine.list_runs(status="COMPLETED", name="bad") == []


def test_ordering_and_pagination(ns: SimpleNamespace) -> None:
    ids = [ns.engine.start("ok") for _ in range(5)]
    ns.engine.driver.run_until_idle()

    newest = ns.engine.list_runs()
    # Default order is newest first, so the last started comes first.
    assert newest[0].id == ids[-1]
    oldest = ns.engine.list_runs(newest_first=False)
    assert oldest[0].id == ids[0]

    # Pagination: two pages of 2 then a final page of 1, no overlap, full coverage.
    page1 = ns.engine.list_runs(limit=2, offset=0)
    page2 = ns.engine.list_runs(limit=2, offset=2)
    page3 = ns.engine.list_runs(limit=2, offset=4)
    assert [len(page1), len(page2), len(page3)] == [2, 2, 1]
    paged = [r.id for r in (*page1, *page2, *page3)]
    assert paged == [r.id for r in newest]  # same order, fully covered
    assert len(set(paged)) == 5


def test_limit_is_clamped(ns: SimpleNamespace) -> None:
    for _ in range(3):
        ns.engine.start("ok")
    ns.engine.driver.run_until_idle()
    # Absurd limits are clamped into [1, 1000] rather than rejected.
    assert len(ns.engine.list_runs(limit=10_000)) == 3
    assert len(ns.engine.list_runs(limit=0)) == 1
