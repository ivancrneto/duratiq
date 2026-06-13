"""Per-activity retry policy: a failing activity is retried up to ``max_retries``
times before its step is recorded FAILED, and the recovered run carries on. The
LocalDriver retries inline; the Dramatiq driver leans on the broker's own retries.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver


def _make(reg: Registry) -> SimpleNamespace:
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(store=store, engine=engine, driver=driver)


def test_retry_then_succeed() -> None:
    reg = Registry()
    attempts = {"n": 0}

    @activity(name="flaky", registry=reg, max_retries=3)
    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    @workflow(name="wf", registry=reg)
    def wf(ctx) -> str:
        return ctx.activity(flaky)

    ns = _make(reg)
    run_id = ns.engine.start("wf")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "ok"
    assert attempts["n"] == 3  # two failures + one success
    # The step records that it took two retries.
    (step,) = ns.store.get_steps(run_id)
    assert step.attempt == 2


def test_retries_exhausted_marks_failed() -> None:
    reg = Registry()
    attempts = {"n": 0}

    @activity(name="always_fails", registry=reg, max_retries=2)
    def always_fails() -> None:
        attempts["n"] += 1
        raise ValueError("nope")

    @workflow(name="wf", registry=reg)
    def wf(ctx) -> str:
        ctx.activity(always_fails)
        return "unreachable"

    ns = _make(reg)
    run_id = ns.engine.start("wf")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "ActivityFailed"
    assert attempts["n"] == 3  # max_retries=2 -> 3 total attempts
    (step,) = ns.store.get_steps(run_id)
    assert step.status == "FAILED"
    assert step.attempt == 2


def test_no_retries_fails_on_first_attempt() -> None:
    reg = Registry()
    attempts = {"n": 0}

    @activity(name="fails", registry=reg, max_retries=0)
    def fails() -> None:
        attempts["n"] += 1
        raise ValueError("nope")

    @workflow(name="wf", registry=reg)
    def wf(ctx) -> str:
        ctx.activity(fails)
        return "unreachable"

    ns = _make(reg)
    run_id = ns.engine.start("wf")
    ns.driver.run_until_idle()

    assert ns.store.get_run(run_id).status == "FAILED"
    assert attempts["n"] == 1


def test_retry_via_dramatiq() -> None:
    dramatiq = pytest.importorskip("dramatiq")
    from dramatiq.brokers.stub import StubBroker
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool

    from duratiq.drivers.dramatiq import DramatiqDriver

    broker = StubBroker()
    reg = Registry()
    attempts = {"n": 0}

    # Tiny backoff so the broker redelivers near-instantly during the test.
    @activity(name="flaky", registry=reg, max_retries=3, min_backoff_ms=1, max_backoff_ms=1)
    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    @workflow(name="wf", registry=reg)
    def wf(ctx) -> str:
        return ctx.activity(flaky)

    db = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool, future=True)
    store = SqlStore(engine=db)
    store.create_all()
    engine = Engine(reg, store)
    driver = DramatiqDriver(engine, broker=broker)

    worker = dramatiq.Worker(broker, worker_threads=1)
    worker.start()
    try:
        run_id = engine.start("wf")
        # Drain the queue repeatedly: retried messages reappear via the delay queue.
        for _ in range(100):
            broker.join(driver.queue_name)
            worker.join()
            if store.get_run(run_id).status == "COMPLETED":
                break
    finally:
        worker.stop()

    run = store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "ok"
    assert attempts["n"] == 3
