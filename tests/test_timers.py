"""Durable timers: ``ctx.sleep`` suspends until its deadline, fires once, and
survives a crash mid-sleep because the deadline lives in Postgres, not memory.

Timers are advanced explicitly via ``engine.fire_due_timers(now=...)`` — the same
call a periodic scanner would make — so these tests fast-forward time instead of
sleeping.
"""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.context import duration_seconds
from duratiq.drivers.local import LocalDriver
from duratiq.models import utcnow


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()
    calls = {"after": 0}

    @activity(name="after_sleep", registry=reg)
    def after_sleep(x: int) -> int:
        calls["after"] += 1
        return x + 1

    @workflow(name="waiter", registry=reg)
    def waiter(ctx, start: int) -> dict:
        ctx.sleep("PT1H")
        nxt = ctx.activity(after_sleep, start)
        return {"value": nxt}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver, calls=calls)


def test_sleep_suspends_until_deadline(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("waiter", start=10)
    ns.driver.run_until_idle()

    # The timer is pending: the run is parked and the post-sleep activity hasn't run.
    assert ns.store.get_run(run_id).status == "SUSPENDED"
    assert ns.calls["after"] == 0

    # Scanning before the deadline fires nothing.
    assert ns.engine.fire_due_timers(now=utcnow()) == 0
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    # Once the deadline has elapsed, the timer fires and the run drives to completion.
    fired = ns.engine.fire_due_timers(now=utcnow() + timedelta(hours=2))
    assert fired == 1
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"value": 11}
    assert ns.calls["after"] == 1


def test_timer_fires_exactly_once(ns: SimpleNamespace) -> None:
    ns.engine.start("waiter", start=10)
    ns.driver.run_until_idle()

    future = utcnow() + timedelta(hours=2)
    assert ns.engine.fire_due_timers(now=future) == 1
    # A second scan past the same deadline finds nothing new (fired_at guard).
    assert ns.engine.fire_due_timers(now=future) == 0


def test_crash_mid_sleep_resumes_on_fresh_engine(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("waiter", start=10)
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    # CRASH: drop the engine + its queue. The deadline is already in the store.
    engine2 = Engine(ns.reg, ns.store)
    driver2 = LocalDriver(engine2)

    fired = engine2.fire_due_timers(now=utcnow() + timedelta(hours=2))
    assert fired == 1
    driver2.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"value": 11}
    assert ns.calls["after"] == 1


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (30, 30.0),
        (1.5, 1.5),
        ("PT10M", 600.0),
        ("PT1H", 3600.0),
        ("P1DT6H", 108_000.0),
        ("PT0.5S", 0.5),
    ],
)
def test_duration_seconds(value: float | str, expected: float) -> None:
    assert duration_seconds(value) == expected


@pytest.mark.parametrize("bad", ["", "P", "PT", "10m", "PXM", "1H"])
def test_duration_seconds_rejects_garbage(bad: str) -> None:
    with pytest.raises(ValueError):
        duration_seconds(bad)
