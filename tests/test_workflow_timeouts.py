"""Tests for workflow-level execution and run timeouts (Groups 3, 6, 7)."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, Scanner, SqlStore
from duratiq.decorators import workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import utcnow


def _ns() -> SimpleNamespace:
    reg = Registry()
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, driver=driver, engine=engine)


# ------------------------------------------------------------------ execution_timeout
def test_execution_timeout_fails_run() -> None:
    ns = _ns()

    @workflow(name="noop_et", registry=ns.reg)
    def noop(ctx):
        ctx.sleep(3600)

    run_id = ns.engine.start("noop_et", execution_timeout=0.001)
    ns.driver.step()  # tick to set up the timer
    run = ns.store.get_run(run_id)
    assert run.execution_timeout_at is not None

    far_future = utcnow() + timedelta(hours=1)
    failed = ns.engine.fire_due_execution_timeouts(now=far_future)
    assert failed == 1

    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "ExecutionTimeout"


def test_execution_timeout_skips_completed_run() -> None:
    ns = _ns()

    @workflow(name="instant_et", registry=ns.reg)
    def instant(ctx):
        return "done"

    run_id = ns.engine.start("instant_et", execution_timeout=100.0)
    ns.driver.step()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"

    far_future = utcnow() + timedelta(hours=1)
    failed = ns.engine.fire_due_execution_timeouts(now=far_future)
    assert failed == 0


# ------------------------------------------------------------------ run_timeout
def test_run_timeout_fails_run() -> None:
    ns = _ns()

    @workflow(name="noop_rt", registry=ns.reg)
    def noop(ctx):
        ctx.sleep(3600)

    run_id = ns.engine.start("noop_rt", run_timeout=0.001)
    ns.driver.step()
    run = ns.store.get_run(run_id)
    assert run.run_timeout_at is not None

    far_future = utcnow() + timedelta(hours=1)
    failed = ns.engine.fire_due_run_timeouts(now=far_future)
    assert failed == 1

    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"
    assert run.error["type"] == "RunTimeout"


def test_run_timeout_decorator_default() -> None:
    ns = _ns()

    @workflow(name="slow_wf", run_timeout=0.001, registry=ns.reg)
    def slow_wf(ctx):
        ctx.sleep(3600)

    run_id = ns.engine.start("slow_wf")
    run = ns.store.get_run(run_id)
    assert run.run_timeout_at is not None

    far_future = utcnow() + timedelta(hours=1)
    failed = ns.engine.fire_due_run_timeouts(now=far_future)
    assert failed == 1


def test_execution_timeout_via_scanner() -> None:
    ns = _ns()

    @workflow(name="noop_sc", registry=ns.reg)
    def noop(ctx):
        ctx.sleep(3600)

    run_id = ns.engine.start("noop_sc", execution_timeout=0.001)
    ns.driver.step()
    far_future = utcnow() + timedelta(hours=1)
    counts = Scanner(ns.engine).run_once(now=far_future)
    assert counts["execution_timeouts"] >= 1

    run = ns.store.get_run(run_id)
    assert run.status == "FAILED"


# ------------------------------------------------------------------ memo
def test_get_memo_returns_dict() -> None:
    ns = _ns()

    @workflow(name="instant_memo", registry=ns.reg)
    def instant(ctx):
        return 1

    run_id = ns.engine.start("instant_memo", memo={"customer": "acme", "priority": 1})
    memo = ns.engine.get_memo(run_id)
    assert memo == {"customer": "acme", "priority": 1}


def test_get_memo_none_when_not_set() -> None:
    ns = _ns()

    @workflow(name="instant_nomemo", registry=ns.reg)
    def instant(ctx):
        return 1

    run_id = ns.engine.start("instant_nomemo")
    assert ns.engine.get_memo(run_id) is None


def test_ctx_info_includes_memo() -> None:
    ns = _ns()
    captured_info: dict = {}

    @workflow(name="wf_info_memo", registry=ns.reg)
    def wf(ctx):
        info = ctx.info()
        captured_info["memo"] = info.memo
        captured_info["name"] = info.name

    ns.engine.start("wf_info_memo", memo={"env": "prod"})
    ns.driver.step()

    assert captured_info["memo"] == {"env": "prod"}
    assert captured_info["name"] == "wf_info_memo"


# ------------------------------------------------------------------ workflow_id reuse policy
def test_workflow_id_allow_duplicate() -> None:
    ns = _ns()

    @workflow(name="instant_wid", registry=ns.reg)
    def instant(ctx):
        return 1

    run_id1 = ns.engine.start("instant_wid", workflow_id="order-123")
    run_id2 = ns.engine.start("instant_wid", workflow_id="order-123", workflow_id_reuse_policy="ALLOW_DUPLICATE")
    assert run_id1 != run_id2


def test_workflow_id_reject_duplicate_raises() -> None:
    ns = _ns()

    @workflow(name="instant_rj", registry=ns.reg)
    def instant(ctx):
        return 1

    ns.engine.start("instant_rj", workflow_id="user-456")
    with pytest.raises(ValueError, match="workflow_id"):
        ns.engine.start("instant_rj", workflow_id="user-456", workflow_id_reuse_policy="REJECT_DUPLICATE")


def test_workflow_id_allow_duplicate_failed_only() -> None:
    ns = _ns()

    @workflow(name="instant_fonly", registry=ns.reg)
    def instant(ctx):
        return 1

    run_id1 = ns.engine.start("instant_fonly", workflow_id="job-789")
    # Run is PENDING (not FAILED), should raise
    with pytest.raises(ValueError, match="non-failed"):
        ns.engine.start("instant_fonly", workflow_id="job-789", workflow_id_reuse_policy="ALLOW_DUPLICATE_FAILED_ONLY")

    # After terminating (marking FAILED), can start a new one
    ns.engine.terminate(run_id1)
    run_id2 = ns.engine.start("instant_fonly", workflow_id="job-789", workflow_id_reuse_policy="ALLOW_DUPLICATE_FAILED_ONLY")
    assert run_id1 != run_id2


def test_workflow_id_terminate_if_running() -> None:
    ns = _ns()

    @workflow(name="noop_tir", registry=ns.reg)
    def noop(ctx):
        ctx.sleep(3600)

    run_id1 = ns.engine.start("noop_tir", workflow_id="entity-abc")
    ns.driver.step()
    run_id2 = ns.engine.start("noop_tir", workflow_id="entity-abc", workflow_id_reuse_policy="TERMINATE_IF_RUNNING")
    assert run_id1 != run_id2

    old_run = ns.store.get_run(run_id1)
    assert old_run.status == "FAILED"
    assert old_run.error["type"] == "WorkflowTerminated"


def test_find_runs_by_workflow_id() -> None:
    ns = _ns()

    @workflow(name="instant_frbw", registry=ns.reg)
    def instant(ctx):
        return 1

    run_id1 = ns.engine.start("instant_frbw", workflow_id="customer-99")
    run_id2 = ns.engine.start("instant_frbw", workflow_id="customer-99")
    runs = ns.store.find_runs_by_workflow_id("customer-99")
    assert {r.id for r in runs} == {run_id1, run_id2}
