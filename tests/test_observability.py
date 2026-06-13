"""Lifecycle observability: Engine(listener=...) emits run/activity events.

Events are emitted only after the state they describe is committed, and a listener
that raises must never affect the run."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, WorkflowEvent, activity, workflow
from duratiq.drivers.local import LocalDriver


def _build(listener) -> SimpleNamespace:
    reg = Registry()

    @activity(name="step", registry=reg)
    def step(x: int) -> int:
        return x + 1

    @activity(name="boom", registry=reg)
    def boom() -> None:
        raise ValueError("kaboom")

    @workflow(name="wf", registry=reg)
    def wf(ctx, start: int) -> dict:
        return {"v": ctx.activity(step, start)}

    @workflow(name="failing", registry=reg)
    def failing(ctx) -> str:
        ctx.activity(boom)
        return "unreachable"

    @workflow(name="waiter", registry=reg)
    def waiter(ctx) -> str:
        return ctx.wait_signal("go")

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store, listener=listener)
    LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine)


def test_completed_run_emits_full_lifecycle() -> None:
    seen: list[WorkflowEvent] = []
    ns = _build(seen.append)

    run_id = ns.engine.start("wf", start=10)
    ns.engine.driver.run_until_idle()

    types = [e.type for e in seen]
    assert types == [
        "run.started",
        "activity.scheduled",
        "run.suspended",
        "activity.completed",
        "run.completed",
    ]
    # Every event carries the run id.
    assert {e.run_id for e in seen} == {run_id}
    started = seen[0]
    assert started.name == "wf"
    scheduled = seen[1]
    assert scheduled.name == "step" and scheduled.seq == 0
    completed = seen[-1]
    assert completed.name == "wf" and completed.result == {"v": 11}


def test_failed_run_emits_activity_and_run_failure() -> None:
    seen: list[WorkflowEvent] = []
    ns = _build(seen.append)

    ns.engine.start("failing")
    ns.engine.driver.run_until_idle()

    types = [e.type for e in seen]
    assert types[-2:] == ["activity.failed", "run.failed"]
    activity_failed = next(e for e in seen if e.type == "activity.failed")
    assert activity_failed.error["type"] == "ValueError"
    run_failed = seen[-1]
    assert run_failed.name == "failing" and run_failed.error["type"] == "ActivityFailed"


def test_cancel_emits_run_cancelled() -> None:
    seen: list[WorkflowEvent] = []
    ns = _build(seen.append)

    run_id = ns.engine.start("waiter")  # queued, not pumped
    assert ns.engine.cancel(run_id) is True

    types = [e.type for e in seen]
    assert types == ["run.started", "run.cancelled"]
    assert seen[-1].run_id == run_id and seen[-1].name == "waiter"


def test_suspend_then_complete_via_signal() -> None:
    seen: list[WorkflowEvent] = []
    ns = _build(seen.append)

    run_id = ns.engine.start("waiter")
    ns.engine.driver.run_until_idle()
    assert [e.type for e in seen] == ["run.started", "run.suspended"]

    ns.engine.signal(run_id, "go", "approved")
    ns.engine.driver.run_until_idle()
    assert seen[-1].type == "run.completed"
    assert seen[-1].result == "approved"


def test_listener_exception_does_not_break_the_run() -> None:
    def angry_listener(_event: WorkflowEvent) -> None:
        raise RuntimeError("listener blew up")

    ns = _build(angry_listener)
    run_id = ns.engine.start("wf", start=1)
    ns.engine.driver.run_until_idle()

    # Despite the listener raising on every event, the run completed normally.
    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"v": 2}


def test_no_listener_is_a_noop() -> None:
    ns = _build(None)
    run_id = ns.engine.start("wf", start=5)
    ns.engine.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "COMPLETED"
