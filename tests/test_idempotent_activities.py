"""Idempotent activities: a stable per-activity idempotency key (activity_info) and
a dedup-table-backed run_once that keeps an effect from repeating across retries.

The headline case is the retry test: an activity whose external effect is wrapped in
run_once fails *after* the effect on its first attempt; on the inline retry the
effect is not repeated, but the activity still completes with the recorded value."""

from __future__ import annotations


import pytest

from duratiq import Engine, Registry, SqlStore, activity, activity_info, run_once, workflow
from duratiq.activity_runtime import activity_scope
from duratiq.drivers.local import LocalDriver


def test_helpers_require_activity_scope() -> None:
    with pytest.raises(RuntimeError):
        activity_info()
    with pytest.raises(RuntimeError):
        run_once("k", lambda: 1)


def test_activity_info_exposes_stable_key() -> None:
    store = SqlStore()
    store.create_all()
    with activity_scope("abc123", 3, store):
        info = activity_info()
    assert (info.run_id, info.seq, info.idempotency_key) == ("abc123", 3, "abc123:3")


def test_run_once_records_then_replays() -> None:
    store = SqlStore()
    store.create_all()
    calls = {"n": 0}

    def effect() -> str:
        calls["n"] += 1
        return "value"

    # Two separate executions reusing the same key: the second skips the effect.
    with activity_scope("run1", 0, store):
        first = run_once("k", effect)
    with activity_scope("run1", 1, store):
        second = run_once("k", effect)

    assert first == "value" and second == "value"
    assert calls["n"] == 1  # effect ran exactly once


def test_run_once_skips_effect_on_retry() -> None:
    reg = Registry()
    counters = {"attempts": 0, "effect": 0}

    def _charge(order_id: str) -> str:
        counters["effect"] += 1
        return f"charged:{order_id}"

    @activity(name="flaky_charge", registry=reg, max_retries=3)
    def flaky_charge(order_id: str) -> str:
        counters["attempts"] += 1
        result = run_once(activity_info().idempotency_key, lambda: _charge(order_id))
        if counters["attempts"] == 1:
            raise RuntimeError("flaky failure *after* the charge already happened")
        return result

    @workflow(name="buy", registry=reg)
    def buy(ctx, order_id: str) -> str:
        return ctx.activity(flaky_charge, order_id)

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("buy", order_id="A1")
    driver.run_until_idle()

    run = store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "charged:A1"
    assert counters["attempts"] == 2  # failed once, retried once
    assert counters["effect"] == 1  # but the charge ran only once


def test_run_once_inside_a_normal_activity_end_to_end() -> None:
    reg = Registry()
    seen = {"keys": []}

    @activity(name="emit", registry=reg)
    def emit(x: int) -> dict:
        info = activity_info()
        seen["keys"].append(info.idempotency_key)
        doubled = run_once(info.idempotency_key, lambda: x * 2)
        return {"key": info.idempotency_key, "doubled": doubled}

    @workflow(name="wf", registry=reg)
    def wf(ctx) -> dict:
        return ctx.activity(emit, 21)

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)

    run_id = engine.start("wf")
    engine.driver.run_until_idle()

    run = store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"]["doubled"] == 42
    # The key is the stable run_id:seq for this invocation.
    assert run.result["value"]["key"] == f"{run_id}:0"
    assert store.get_dedup(f"{run_id}:0") is not None


def test_put_dedup_is_insert_if_absent() -> None:
    store = SqlStore()
    store.create_all()
    assert store.put_dedup(key="k", run_id="r", seq=0, result={"value": 1}) is True
    # A second writer with the same key does not overwrite.
    assert store.put_dedup(key="k", run_id="r", seq=1, result={"value": 2}) is False
    assert store.get_dedup("k").result == {"value": 1}
