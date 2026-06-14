"""The Scanner drives the engine's three periodic scans.

``run_once`` is exercised against a real engine + LocalDriver (a sleeping workflow
and a due schedule), so it proves the integration. ``run_forever``'s loop — cadence,
error-resilience, clean stop — is exercised against a recording fake engine, since
the loop's job is *calling* the scans, not what they do.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, Scanner, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import utcnow

UTC = timezone.utc


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()
    calls = {"after": 0, "report": 0}

    @activity(name="after_sleep", registry=reg)
    def after_sleep(x: int) -> int:
        calls["after"] += 1
        return x + 1

    @workflow(name="waiter", registry=reg)
    def waiter(ctx, start: int) -> dict:
        ctx.sleep("PT1H")
        return {"value": ctx.activity(after_sleep, start)}

    @activity(name="build_report", registry=reg)
    def build_report(region: str) -> str:
        calls["report"] += 1
        return f"report::{region}"

    @workflow(name="daily_report", registry=reg)
    def daily_report(ctx, region: str) -> dict:
        return {"report": ctx.activity(build_report, region)}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver, calls=calls)


# --------------------------------------------------------------------- run_once
def test_run_once_fires_due_timer(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("waiter", start=10)
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    scanner = Scanner(ns.engine)
    counts = scanner.run_once(now=utcnow() + timedelta(hours=2))
    assert counts["timers"] == 1

    ns.driver.run_until_idle()  # pump the tick the scan re-queued
    assert ns.store.get_run(run_id).status == "COMPLETED"
    assert ns.calls["after"] == 1


def test_run_once_starts_due_schedule(ns: SimpleNamespace) -> None:
    ns.engine.create_schedule(
        "daily_report", "0 9 * * 1-5", schedule_id="eu", now=datetime(2026, 6, 15, 0, 0, tzinfo=UTC), region="eu"
    )
    scanner = Scanner(ns.engine)
    counts = scanner.run_once(now=datetime(2026, 6, 15, 9, 0, tzinfo=UTC))
    assert counts["schedules"] == 1

    ns.driver.run_until_idle()
    assert ns.calls["report"] == 1


def test_run_once_idle_returns_zeros(ns: SimpleNamespace) -> None:
    assert Scanner(ns.engine).run_once() == {
        "timers": 0,
        "schedules": 0,
        "activity_timeouts": 0,
        "recovery": 0,
    }


# ------------------------------------------------------------------ validation
def test_nonpositive_interval_rejected(ns: SimpleNamespace) -> None:
    with pytest.raises(ValueError, match="positive"):
        Scanner(ns.engine, timer_interval=0)


# ----------------------------------------------------------------- run_forever
class _FakeEngine:
    """Records scan calls; optionally raises from one scan to test resilience."""

    def __init__(self, *, timers_raise: bool = False) -> None:
        self.counts = {"timers": 0, "schedules": 0, "activity_timeouts": 0, "recovery": 0}
        self.timers_raise = timers_raise

    def fire_due_timers(self, *, now=None, limit=100) -> int:
        self.counts["timers"] += 1
        if self.timers_raise:
            raise RuntimeError("boom")
        return 0

    def fire_due_schedules(self, *, now=None, limit=100) -> int:
        self.counts["schedules"] += 1
        return 0

    def fire_due_activity_timeouts(self, *, now=None, limit=100) -> int:
        self.counts["activity_timeouts"] += 1
        return 0

    def recover_stalled(self, *, older_than_seconds=60, now=None, limit=100) -> int:
        self.counts["recovery"] += 1
        return 0


def _run_in_thread(scanner: Scanner, predicate, *, timeout: float = 2.0) -> None:
    t = threading.Thread(target=scanner.run_forever, daemon=True)
    t.start()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not predicate():
        time.sleep(0.01)
    scanner.stop()
    t.join(timeout=2.0)
    assert not t.is_alive(), "run_forever did not stop"


def test_run_forever_drives_all_scans() -> None:
    fake = _FakeEngine()
    scanner = Scanner(fake, timer_interval=0.01, schedule_interval=0.01, recovery_interval=0.01)
    _run_in_thread(scanner, lambda: all(v > 0 for v in fake.counts.values()))
    assert all(v > 0 for v in fake.counts.values()), fake.counts


def test_run_forever_survives_scan_errors() -> None:
    # The timer scan raises every time; the loop must keep running the others.
    fake = _FakeEngine(timers_raise=True)
    scanner = Scanner(fake, timer_interval=0.01, schedule_interval=0.01, recovery_interval=0.01)
    _run_in_thread(scanner, lambda: fake.counts["recovery"] > 2)
    assert fake.counts["timers"] > 1  # kept being attempted despite raising
    assert fake.counts["recovery"] > 1  # and the other scans kept running


# ----------------------------------------------------------------- _load_engine
def make_test_engine() -> Engine:
    """Module-level factory used to exercise the 'module:callable' CLI loader."""
    store = SqlStore()
    store.create_all()
    engine = Engine(Registry(), store)
    LocalDriver(engine)
    return engine


def test_load_engine_imports_factory() -> None:
    from duratiq.scanner import _load_engine

    engine = _load_engine(f"{__name__}:make_test_engine")
    assert isinstance(engine, Engine)


def test_load_engine_rejects_bad_reference() -> None:
    from duratiq.scanner import _load_engine

    with pytest.raises(ValueError, match="module:callable"):
        _load_engine("not_a_factory_ref")
