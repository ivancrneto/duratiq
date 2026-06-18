"""Tests for dynamic (catch-all) workflow and activity handlers (Group 8)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, WorkflowNotFound
from duratiq.decorators import activity, workflow
from duratiq.drivers.local import LocalDriver


def _ns() -> SimpleNamespace:
    reg = Registry()
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, driver=driver, engine=engine)


# ------------------------------------------------------------------ dynamic workflow
def test_dynamic_workflow_handles_unregistered_name() -> None:
    ns = _ns()

    @workflow.dynamic(registry=ns.reg)
    def catch_all(ctx):
        return "handled"

    run_id = ns.engine.start("any_name_at_all")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "handled"


def test_dynamic_workflow_does_not_shadow_explicit() -> None:
    ns = _ns()

    @workflow(name="explicit_wf", registry=ns.reg)
    def explicit_wf(ctx):
        return "explicit"

    @workflow.dynamic(registry=ns.reg)
    def catch_all(ctx):
        return "dynamic"

    run_id = ns.engine.start("explicit_wf")
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).result["value"] == "explicit"


def test_missing_workflow_raises_when_no_dynamic() -> None:
    ns = _ns()
    with pytest.raises(WorkflowNotFound):
        ns.engine.start("no_such_workflow")


def test_dynamic_workflow_receives_workflow_context() -> None:
    ns = _ns()
    received: dict = {}

    @workflow.dynamic(registry=ns.reg)
    def catch_all(ctx):
        received["run_id"] = ctx.run_id
        return "ok"

    run_id = ns.engine.start("anything")
    ns.driver.run_until_idle()

    assert received["run_id"] == run_id


# ------------------------------------------------------------------ dynamic activity
def test_dynamic_activity_handles_unregistered_name() -> None:
    ns = _ns()

    @activity.dynamic(registry=ns.reg)
    def catch_all_act():
        return "dynamic-activity"

    @workflow(name="wf_da", registry=ns.reg)
    def wf(ctx):
        act = ns.reg.get_activity("unknown_activity")
        return ctx.activity(act)

    run_id = ns.engine.start("wf_da")
    ns.driver.run_until_idle()

    assert ns.store.get_run(run_id).result["value"] == "dynamic-activity"


def test_dynamic_activity_does_not_shadow_explicit() -> None:
    ns = _ns()

    @activity(name="real_act", registry=ns.reg)
    def real_act():
        return "real"

    @activity.dynamic(registry=ns.reg)
    def catch_all_act():
        return "dynamic"

    @workflow(name="wf_da_explicit", registry=ns.reg)
    def wf(ctx):
        return ctx.activity(real_act)

    run_id = ns.engine.start("wf_da_explicit")
    ns.driver.run_until_idle()

    assert ns.store.get_run(run_id).result["value"] == "real"


def test_missing_activity_raises_when_no_dynamic() -> None:
    ns = _ns()
    with pytest.raises(KeyError):
        ns.reg.get_activity("no_such_activity")
