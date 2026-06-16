"""Tests for CancellationScope and ctx.set_signal_handler (Group 10)."""

from __future__ import annotations

from types import SimpleNamespace


from duratiq import CancellationScope, Engine, Registry, SqlStore
from duratiq.decorators import activity, workflow
from duratiq.drivers.local import LocalDriver


def _ns() -> SimpleNamespace:
    reg = Registry()
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, driver=driver, engine=engine)


# ------------------------------------------------------------------ CancellationScope basics
def test_cancellation_scope_is_context_manager() -> None:
    ns = _ns()

    @workflow(name="wf_scope_basic", registry=ns.reg)
    def wf(ctx):
        with ctx.cancellation_scope():
            pass
        return "done"

    run_id = ns.engine.start("wf_scope_basic")
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).result["value"] == "done"


def test_cancellation_scope_cancel_skips_activity() -> None:
    ns = _ns()

    @activity(name="skip_me", registry=ns.reg)
    def skip_me():
        return "should not run"

    side_effects: list = []

    @workflow(name="wf_scope_cancel", registry=ns.reg)
    def wf(ctx):
        with ctx.cancellation_scope() as scope:
            scope.cancel()  # cancel immediately
            ctx.activity(skip_me)  # should be skipped
            side_effects.append("reached")
        return "after scope"

    run_id = ns.engine.start("wf_scope_cancel")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "after scope"
    assert side_effects == []  # never executed


def test_cancellation_scope_can_be_cancelled_false_initially() -> None:
    ns = _ns()

    @workflow(name="wf_scope_false", registry=ns.reg)
    def wf(ctx):
        with ctx.cancellation_scope() as scope:
            assert not scope.cancelled
            scope.cancel()
            assert scope.cancelled
        return "ok"

    run_id = ns.engine.start("wf_scope_false")
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "COMPLETED"


def test_cancellation_scope_is_exported() -> None:
    assert CancellationScope is not None


# ------------------------------------------------------------------ signal handler
def test_set_signal_handler_non_blocking() -> None:
    ns = _ns()
    logged: list = []

    @activity(name="act_sh", registry=ns.reg)
    def act_sh():
        return "done"

    @workflow(name="wf_sh_nonblock", registry=ns.reg)
    def wf(ctx):
        ctx.set_signal_handler("info", lambda payload: logged.append(payload))
        result = ctx.activity(act_sh)
        return result

    run_id = ns.engine.start("wf_sh_nonblock")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "done"


def test_set_signal_handler_called_when_signal_arrives() -> None:
    ns = _ns()
    logged: list = []

    @workflow(name="wf_sh_fires", registry=ns.reg)
    def wf(ctx):
        ctx.set_signal_handler("notify", lambda payload: logged.append(payload))
        ctx.sleep(3600)

    run_id = ns.engine.start("wf_sh_fires")
    ns.driver.step()  # tick: registers handler, sleeps

    # Send signal
    ns.engine.signal(run_id, "notify", {"msg": "hello"})
    ns.driver.step()  # re-tick: handler fires with payload

    assert logged == [{"msg": "hello"}]


# ------------------------------------------------------------------ scope + signal handler (the key combo)
def test_scope_cancelled_by_signal_stops_sleep() -> None:
    ns = _ns()

    @workflow(name="wf_scope_sig", registry=ns.reg)
    def wf(ctx):
        with ctx.cancellation_scope() as scope:
            ctx.set_signal_handler("abort", lambda _: scope.cancel())
            ctx.sleep(3600)
        return "aborted"

    run_id = ns.engine.start("wf_scope_sig")
    ns.driver.step()  # tick: registers handler + sleep

    ns.engine.signal(run_id, "abort", None)
    ns.driver.step()  # re-tick: handler fires, scope cancelled, sleep raises _ScopeCancelled
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "aborted"


def test_scope_cancelled_by_signal_stops_activity() -> None:
    ns = _ns()

    @activity(name="long_act_sc", registry=ns.reg)
    def long_act_sc():
        return "activity-result"

    @workflow(name="wf_scope_sig_act", registry=ns.reg)
    def wf(ctx):
        with ctx.cancellation_scope() as scope:
            ctx.set_signal_handler("abort", lambda _: scope.cancel())
            ctx.activity(long_act_sc)
        return "scope-done"

    run_id = ns.engine.start("wf_scope_sig_act")
    ns.driver.step()  # tick: registers handler + activity scheduled

    ns.engine.signal(run_id, "abort", None)
    ns.driver.step()  # re-tick: handler fires, scope cancelled → activity call raises _ScopeCancelled
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "scope-done"


def test_scope_without_cancellation_runs_normally() -> None:
    ns = _ns()

    @activity(name="act_scope_ok", registry=ns.reg)
    def act_scope_ok():
        return "activity-ran"

    @workflow(name="wf_scope_ok", registry=ns.reg)
    def wf(ctx):
        with ctx.cancellation_scope():
            ctx.set_signal_handler("irrelevant", lambda _: None)
            result = ctx.activity(act_scope_ok)
        return result

    run_id = ns.engine.start("wf_scope_ok")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "activity-ran"


def test_multiple_scopes_independent() -> None:
    ns = _ns()

    @activity(name="act_m1", registry=ns.reg)
    def act1():
        return "first"

    @activity(name="act_m2", registry=ns.reg)
    def act2():
        return "second"

    @workflow(name="wf_multi_scope", registry=ns.reg)
    def wf(ctx):
        with ctx.cancellation_scope() as s1:
            s1.cancel()
            ctx.activity(act1)  # skipped

        # Second scope is unaffected
        result = None
        with ctx.cancellation_scope():
            result = ctx.activity(act2)

        return result

    run_id = ns.engine.start("wf_multi_scope")
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == "second"
