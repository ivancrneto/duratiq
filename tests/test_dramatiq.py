"""End-to-end through a real Dramatiq broker (StubBroker + a worker thread).

Uses a single shared in-memory SQLite database (StaticPool) and a single worker
thread for determinism.
"""

from __future__ import annotations

import pytest

dramatiq = pytest.importorskip("dramatiq")

from dramatiq.brokers.stub import StubBroker  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from duratiq import Engine, Registry, SqlStore, activity, workflow  # noqa: E402
from duratiq.drivers.dramatiq import DramatiqDriver  # noqa: E402


def test_end_to_end_via_dramatiq() -> None:
    broker = StubBroker()
    reg = Registry()
    calls = {"n": 0}

    @activity(name="inc", registry=reg)
    def inc(x: int) -> int:
        calls["n"] += 1
        return x + 1

    @workflow(name="twice", registry=reg)
    def twice(ctx, start: int) -> int:
        a = ctx.activity(inc, start)
        b = ctx.activity(inc, a)
        return b

    db = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool, future=True)
    store = SqlStore(engine=db)
    store.create_all()
    engine = Engine(reg, store)
    driver = DramatiqDriver(engine, broker=broker)

    worker = dramatiq.Worker(broker, worker_threads=1)
    worker.start()
    try:
        run_id = engine.start("twice", start=1)
        broker.join(driver.queue_name)
        worker.join()
    finally:
        worker.stop()

    run = store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == 3
    assert calls["n"] == 2


def test_child_workflow_end_to_end_via_dramatiq() -> None:
    broker = StubBroker()
    reg = Registry()

    @activity(name="double", registry=reg)
    def double(x: int) -> int:
        return x * 2

    @workflow(name="doubler", registry=reg)
    def doubler(ctx, x: int) -> int:
        return ctx.activity(double, x)

    @workflow(name="parent", registry=reg)
    def parent(ctx, x: int) -> dict:
        doubled = ctx.child_workflow("doubler", x=x)
        return {"doubled": doubled}

    db = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool, future=True)
    store = SqlStore(engine=db)
    store.create_all()
    engine = Engine(reg, store)
    driver = DramatiqDriver(engine, broker=broker)

    worker = dramatiq.Worker(broker, worker_threads=1)
    worker.start()
    try:
        run_id = engine.start("parent", x=21)
        broker.join(driver.queue_name)
        worker.join()
    finally:
        worker.stop()

    run = store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"doubled": 42}
    # The child run completed and is linked back to the parent.
    child_step = next(s for s in store.get_steps(run_id) if s.kind == "CHILD_WORKFLOW")
    child = store.find_child_run(run_id, child_step.seq)
    assert child is not None and child.status == "COMPLETED"
