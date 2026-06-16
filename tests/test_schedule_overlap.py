"""Tests for schedule overlap policies (Group 5)."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace


from duratiq import Engine, Registry, SqlStore, workflow
from duratiq.drivers.local import LocalDriver

UTC = timezone.utc
T0 = datetime(2026, 1, 1, 8, 59, tzinfo=UTC)
T1 = datetime(2026, 1, 1, 9, 0, tzinfo=UTC)
T2 = datetime(2026, 1, 2, 9, 0, tzinfo=UTC)


def _ns() -> SimpleNamespace:
    reg = Registry()
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine)


# ------------------------------------------------------------------ ALLOW (default)
def test_overlap_allow_always_starts_new_run() -> None:
    ns = _ns()

    @workflow(name="long_al", registry=ns.reg)
    def long_al(ctx):
        ctx.sleep(3600)

    sid = ns.engine.create_schedule("long_al", "0 9 * * *", now=T0, overlap_policy="ALLOW")
    # First fire
    ns.engine.fire_due_schedules(now=T1)
    ns.engine.driver.step()  # tick first run
    sch = ns.store.get_schedule(sid)
    first_run_id = sch.last_run_id
    assert first_run_id is not None
    assert ns.store.get_run(first_run_id).status in ("PENDING", "SUSPENDED")

    # Second fire — ALLOW should start a new run even though first is still active
    started = ns.engine.fire_due_schedules(now=T2)
    assert started == 1
    sch = ns.store.get_schedule(sid)
    assert sch.last_run_id != first_run_id

    first_run = ns.store.get_run(first_run_id)
    assert first_run.status not in ("CANCELLED", "FAILED")  # old run untouched


# ------------------------------------------------------------------ SKIP
def test_overlap_skip_skips_when_last_run_active() -> None:
    ns = _ns()

    @workflow(name="long_sk", registry=ns.reg)
    def long_sk(ctx):
        ctx.sleep(3600)

    sid = ns.engine.create_schedule("long_sk", "0 9 * * *", now=T0, overlap_policy="SKIP")
    ns.engine.fire_due_schedules(now=T1)
    ns.engine.driver.step()  # tick first run into PENDING (waiting for sleep)
    sch = ns.store.get_schedule(sid)
    first_run_id = sch.last_run_id

    # Second fire — last run is still active, should skip
    started = ns.engine.fire_due_schedules(now=T2)
    assert started == 0

    # last_run_id should NOT change (skip didn't start anything)
    sch2 = ns.store.get_schedule(sid)
    assert sch2.last_run_id == first_run_id


def test_overlap_skip_starts_if_last_run_completed() -> None:
    ns = _ns()

    @workflow(name="quick_sk", registry=ns.reg)
    def quick_sk(ctx):
        return "done"

    sid = ns.engine.create_schedule("quick_sk", "0 9 * * *", now=T0, overlap_policy="SKIP")
    ns.engine.fire_due_schedules(now=T1)
    ns.engine.driver.run_until_idle()  # completes first run

    sch = ns.store.get_schedule(sid)
    assert ns.store.get_run(sch.last_run_id).status == "COMPLETED"

    # Second fire — last run is COMPLETED, should start
    started = ns.engine.fire_due_schedules(now=T2)
    assert started == 1


# ------------------------------------------------------------------ REPLACE
def test_overlap_replace_cancels_running_then_starts() -> None:
    ns = _ns()

    @workflow(name="long_rp", registry=ns.reg)
    def long_rp(ctx):
        ctx.sleep(3600)

    sid = ns.engine.create_schedule("long_rp", "0 9 * * *", now=T0, overlap_policy="REPLACE")
    ns.engine.fire_due_schedules(now=T1)
    ns.engine.driver.step()  # tick into PENDING

    sch = ns.store.get_schedule(sid)
    first_run_id = sch.last_run_id

    # Second fire — should cancel the running run then start a new one
    started = ns.engine.fire_due_schedules(now=T2)
    assert started == 1

    first_run = ns.store.get_run(first_run_id)
    assert first_run.status == "CANCELLED"

    sch2 = ns.store.get_schedule(sid)
    assert sch2.last_run_id != first_run_id


# ------------------------------------------------------------------ TERMINATE
def test_overlap_terminate_terminates_running_then_starts() -> None:
    ns = _ns()

    @workflow(name="long_tm", registry=ns.reg)
    def long_tm(ctx):
        ctx.sleep(3600)

    sid = ns.engine.create_schedule("long_tm", "0 9 * * *", now=T0, overlap_policy="TERMINATE")
    ns.engine.fire_due_schedules(now=T1)
    ns.engine.driver.step()  # tick into PENDING

    sch = ns.store.get_schedule(sid)
    first_run_id = sch.last_run_id

    # Second fire — should terminate the running run then start a new one
    started = ns.engine.fire_due_schedules(now=T2)
    assert started == 1

    first_run = ns.store.get_run(first_run_id)
    assert first_run.status == "FAILED"
    assert first_run.error["type"] == "WorkflowTerminated"

    sch2 = ns.store.get_schedule(sid)
    assert sch2.last_run_id != first_run_id


# ------------------------------------------------------------------ default is ALLOW
def test_default_overlap_policy_is_allow() -> None:
    ns = _ns()

    @workflow(name="quick_def", registry=ns.reg)
    def quick_def(ctx):
        return 1

    # No overlap_policy → defaults to ALLOW
    sid = ns.engine.create_schedule("quick_def", "0 9 * * *", now=T0)
    ns.engine.fire_due_schedules(now=T1)
    ns.engine.driver.run_until_idle()
    sch = ns.store.get_schedule(sid)
    assert sch.overlap_policy == "ALLOW"
