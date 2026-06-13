"""ctx.patched — gate a workflow-code change so in-flight runs replay deterministically.

New runs take the patched (new) path and record a marker; runs that already executed
past the patch point under the old code keep taking the old path without a
DeterminismError. The crux is the "old in-flight run" test, which swaps the
registered workflow body mid-run and proves the suspended run still completes."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.registry import Workflow


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()

    @activity(name="step_a", registry=reg)
    def step_a() -> str:
        return "a"

    @activity(name="step_b", registry=reg)
    def step_b() -> str:
        return "b"

    @activity(name="step_c", registry=reg)
    def step_c() -> str:
        return "c"

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver,
                           step_a=step_a, step_b=step_b, step_c=step_c)


def test_patched_true_on_new_run(ns: SimpleNamespace) -> None:
    @workflow(name="gated", registry=ns.reg)
    def gated(ctx) -> str:
        if ctx.patched("use-c"):
            return ctx.activity(ns.step_c)
        return ctx.activity(ns.step_b)

    run_id = ns.engine.start("gated")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "c"  # took the patched path
    # The patch marker was recorded as a PATCH step.
    kinds = [(s.kind, s.name) for s in ns.store.get_steps(run_id)]
    assert ("PATCH", "use-c") in kinds


def test_patched_false_for_old_inflight_run(ns: SimpleNamespace) -> None:
    # An in-flight run recorded under code WITHOUT the patch must keep the old path
    # after the patch is deployed — no DeterminismError, no skipped/duplicated steps.

    # 1) Old code: a -> b -> wait, registered as "evolve".
    def evolve_old(ctx) -> dict:
        a = ctx.activity(ns.step_a)         # seq 0
        b = ctx.activity(ns.step_b)         # seq 1
        d = ctx.wait_signal("finish")       # seq 2 (suspends here)
        return {"mode": "old", "a": a, "b": b, "d": d}

    ns.reg.add_workflow(Workflow(fn=evolve_old, name="evolve"))
    run_id = ns.engine.start("evolve")
    ns.driver.run_until_idle()  # runs a, b; suspends on the signal at seq 2

    run = ns.store.get_run(run_id)
    assert run.status == "SUSPENDED"
    # History holds real commands at seq 0/1 — exactly where a marker would sit.
    assert [s.kind for s in ns.store.get_steps(run_id)] == ["ACTIVITY", "ACTIVITY", "SIGNAL_WAIT"]

    # 2) Deploy new code: insert a patch gate after step_a. New runs would do step_c
    #    instead of step_b; old runs must still do step_b.
    def evolve_new(ctx) -> dict:
        a = ctx.activity(ns.step_a)             # seq 0
        if ctx.patched("swap-b-for-c"):         # peeks seq 1 -> real ACTIVITY -> False
            c = ctx.activity(ns.step_c)
            d = ctx.wait_signal("finish")
            return {"mode": "new", "a": a, "c": c, "d": d}
        b = ctx.activity(ns.step_b)             # seq 1 (realigns with history)
        d = ctx.wait_signal("finish")           # seq 2
        return {"mode": "old", "a": a, "b": b, "d": d}

    ns.reg.add_workflow(Workflow(fn=evolve_new, name="evolve"))

    # 3) The suspended old run resumes under the new code and finishes the old way.
    ns.engine.signal(run_id, "finish", "ok")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"mode": "old", "a": "a", "b": "b", "d": "ok"}
    # No marker was recorded for the old run.
    assert all(s.kind != "PATCH" for s in ns.store.get_steps(run_id))

    # 4) A brand-new run under the new code takes the patched path.
    new_run = ns.engine.start("evolve")
    # drive it to the signal wait, then finish it
    ns.driver.run_until_idle()
    ns.engine.signal(new_run, "finish", "ok")
    ns.driver.run_until_idle()
    nr = ns.store.get_run(new_run)
    assert nr.status == "COMPLETED"
    assert nr.result["value"]["mode"] == "new"
    assert nr.result["value"]["c"] == "c"
    assert ("PATCH", "swap-b-for-c") in [(s.kind, s.name) for s in ns.store.get_steps(new_run)]


def test_patch_marker_is_stable_across_crash(ns: SimpleNamespace) -> None:
    @workflow(name="gated_wait", registry=ns.reg)
    def gated_wait(ctx) -> str:
        taken = "new" if ctx.patched("p1") else "old"
        ctx.wait_signal("go")  # suspend after the patch decision is recorded
        return taken

    run_id = ns.engine.start("gated_wait")
    ns.driver.run_until_idle()  # records the marker, suspends on the signal
    assert ns.store.get_run(run_id).status == "SUSPENDED"
    assert ("PATCH", "p1") in [(s.kind, s.name) for s in ns.store.get_steps(run_id)]

    # CRASH: fresh engine on the same store; the marker must replay stably.
    engine2 = Engine(ns.reg, ns.store)
    driver2 = LocalDriver(engine2)
    driver2.request_tick(run_id)
    driver2.run_until_idle()
    engine2.signal(run_id, "go", None)
    driver2.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "new"
    # Still exactly one marker — replay did not duplicate it.
    assert sum(1 for s in ns.store.get_steps(run_id) if s.kind == "PATCH") == 1
