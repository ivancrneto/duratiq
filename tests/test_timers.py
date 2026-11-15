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
        ("P30D", 2_592_000.0),
        ("PT1H30M", 5400.0),
        ("PT1.5H", 5400.0),
        ("P1DT12H", 129_600.0),
        ("PT0S", 0.0),
        # Developer shorthand.
        ("45s", 45.0),
        ("10m", 600.0),
        ("2h", 7200.0),
        ("30d", 2_592_000.0),
        ("2w", 1_209_600.0),
        ("250ms", 0.25),
        ("1.5h", 5400.0),
        ("1h30m", 5400.0),
        ("1d12h", 129_600.0),
        ("1m30s", 90.0),
        ("1m5ms", 60.005),  # "m" then "ms", not "m" then "s"
        ("0s", 0.0),
    ],
)
def test_duration_seconds(value: float | str, expected: float) -> None:
    assert duration_seconds(value) == expected


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "P",
        "PT",
        "PXM",
        "10",  # a bare number string has no unit — pass a number for seconds
        "1H",  # shorthand units are lowercase...
        "5M",  # ...so "M" can never be misread as months
        "10x",
        "m",
        "ms",
        "1m1h",  # largest unit first
        "1h1h",  # each unit at most once
        "-5m",
        "1 h",
        "1h 30m",
        " 30m",
        "1.5.2h",
        "10mm",
    ],
)
def test_duration_seconds_rejects_garbage(bad: str) -> None:
    with pytest.raises(ValueError, match="invalid duration"):
        duration_seconds(bad)


# Each ISO-8601 duration next to its shorthand spelling. Both styles are supported
# side by side; these pairs pin that they mean exactly the same thing.
EQUIVALENT_DURATIONS = [
    ("PT45S", "45s"),
    ("PT10M", "10m"),
    ("PT2H", "2h"),
    ("P30D", "30d"),
    ("P14D", "2w"),
    ("PT0.25S", "250ms"),
    ("PT1.5H", "1.5h"),
    ("PT1H30M", "1h30m"),
    ("P1DT12H", "1d12h"),
    ("PT1M30S", "1m30s"),
]


@pytest.mark.parametrize(("iso", "shorthand"), EQUIVALENT_DURATIONS)
def test_iso_and_shorthand_are_equivalent(iso: str, shorthand: str) -> None:
    assert duration_seconds(iso) == duration_seconds(shorthand)


@pytest.mark.parametrize(
    ("sleep", "wait_timeout", "select_timer"),
    [("P30D", "PT1H30M", "P14D"), ("30d", "1h30m", "2w")],
    ids=["iso", "shorthand"],
)
def test_every_duration_entry_point_accepts_both_styles(sleep: str, wait_timeout: str, select_timer: str) -> None:
    """ctx.sleep, wait_signal(timeout=...), and select's defer_timer all parse durations."""
    from duratiq.testing import WorkflowEnvironment

    reg = Registry()

    @workflow(name="durations", registry=reg)
    def durations(ctx) -> object:
        ctx.sleep(sleep)
        timed_out = ctx.wait_signal("nudge", timeout=wait_timeout)
        index, _ = ctx.select(ctx.defer_signal("never"), ctx.defer_timer(select_timer))
        return {"timed_out": repr(timed_out), "select_winner": index}

    with WorkflowEnvironment(reg) as env:
        start = env.now()
        assert env.execute("durations") == {"timed_out": "duratiq.TIMEOUT", "select_winner": 1}
        assert env.now() - start == timedelta(days=30, hours=1, minutes=30, weeks=2)


def test_readme_durations_table_matches_the_parser() -> None:
    """The README's durations reference must stay true: every cell in a row is the same length."""
    import pathlib
    import re

    readme = (pathlib.Path(__file__).resolve().parents[1] / "README.md").read_text()
    start = readme.index("| Duration ")
    rows = readme[start:].split("\n\n")[0].splitlines()[2:]  # skip header + separator
    assert len(rows) >= 5
    for row in rows:
        cells = [cell.strip() for cell in row.strip("|").split("|")]
        seconds = float(cells[1].strip("`"))
        spellings = re.findall(r'"([^"]+)"', cells[2] + cells[3])
        assert len(spellings) >= 2, row  # at least one ISO-8601 and one shorthand form
        for spelling in spellings:
            assert duration_seconds(spelling) == seconds, (row, spelling)
