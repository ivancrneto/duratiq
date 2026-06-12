"""Engine.cancel / Engine.retry — control operations over a run's lifecycle."""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver


def _build():
    reg = Registry()
    flag = {"fail": True}

    @activity(name="double", registry=reg)
    def double(x: int) -> int:
        return x * 2

    @activity(name="boom", registry=reg)
    def boom() -> str:
        if flag["fail"]:
            raise RuntimeError("kaboom")
        return "recovered"

    @workflow(name="wf", registry=reg)
    def wf(ctx):  # noqa: ANN001
        a = ctx.activity(double, 21)
        b = ctx.activity(boom)
        return {"a": a, "b": b}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)
    return engine, flag


def test_cancel_pending_run():
    engine, _ = _build()
    run_id = engine.start("wf")  # queued tick, not yet pumped -> PENDING

    assert engine.cancel(run_id) is True
    assert engine.get(run_id).status == "CANCELLED"

    # Pumping must not advance a cancelled run.
    engine.driver.run_until_idle()
    assert engine.get(run_id).status == "CANCELLED"


def test_cancel_completed_run_is_noop():
    engine, flag = _build()
    flag["fail"] = False
    run_id = engine.start("wf")
    engine.driver.run_until_idle()
    assert engine.get(run_id).status == "COMPLETED"

    assert engine.cancel(run_id) is False
    assert engine.get(run_id).status == "COMPLETED"


def test_retry_failed_run_recovers():
    engine, flag = _build()
    run_id = engine.start("wf")
    engine.driver.run_until_idle()
    assert engine.get(run_id).status == "FAILED"
    # The boom step is recorded FAILED.
    assert any(s.status == "FAILED" for s in engine.store.get_steps(run_id))

    flag["fail"] = False  # the activity will succeed on the retry
    assert engine.retry(run_id) is True
    assert engine.get(run_id).status == "PENDING"  # re-armed, tick queued

    engine.driver.run_until_idle()
    run = engine.get(run_id)
    assert run.status == "COMPLETED"
    assert run.result == {"value": {"a": 42, "b": "recovered"}}


def test_retry_non_failed_run_is_noop():
    engine, flag = _build()
    flag["fail"] = False
    run_id = engine.start("wf")
    engine.driver.run_until_idle()
    assert engine.get(run_id).status == "COMPLETED"

    assert engine.retry(run_id) is False
