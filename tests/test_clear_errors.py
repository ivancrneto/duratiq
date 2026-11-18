"""Misconfiguration fails loudly, before any state is written.

An Engine with no driver cannot advance runs, so every entry point that would
persist state and then need a tick must refuse up front: raising after the write
leaves orphan PENDING runs or consumes timers that never re-tick their run.
"""

from __future__ import annotations

import functools
from datetime import timedelta

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver


def _engine() -> tuple[Engine, SqlStore]:
    reg = Registry()

    @workflow(name="noop", registry=reg)
    def noop(ctx) -> int:
        return 1

    @workflow(name="napper", registry=reg)
    def napper(ctx) -> str:
        ctx.sleep(60)
        return "woke"

    store = SqlStore()
    store.create_all()
    return Engine(reg, store), store


def test_start_without_driver_raises_and_creates_no_run() -> None:
    engine, store = _engine()
    with pytest.raises(RuntimeError, match="no driver"):
        engine.start("noop")
    assert store.list_runs() == []


def test_signal_with_start_without_driver_raises_and_creates_no_run() -> None:
    engine, store = _engine()
    with pytest.raises(RuntimeError, match="no driver"):
        engine.signal_with_start("noop", signal="go")
    assert store.list_runs() == []


def test_fire_due_timers_without_driver_leaves_timer_pending() -> None:
    engine, _ = _engine()
    driver = LocalDriver(engine)
    run_id = engine.start("napper")
    driver.run_until_idle()

    engine.driver = None
    later = engine.now() + timedelta(seconds=120)
    with pytest.raises(RuntimeError, match="no driver"):
        engine.fire_due_timers(now=later)

    engine.driver = driver
    assert engine.fire_due_timers(now=later) == 1
    driver.run_until_idle()
    assert engine.get(run_id).status == "COMPLETED"


@pytest.mark.parametrize("decorator", [activity, workflow])
def test_nameless_callable_needs_explicit_name(decorator) -> None:
    def add(x: int, y: int) -> int:
        return x + y

    with pytest.raises(TypeError, match="pass name="):
        decorator(functools.partial(add, 1), registry=Registry())


def test_nameless_callable_with_explicit_name_registers() -> None:
    def add(x: int, y: int) -> int:
        return x + y

    act = activity(functools.partial(add, 1), name="add_one", registry=Registry())
    assert act.name == "add_one"
    assert act.fn(2) == 3
