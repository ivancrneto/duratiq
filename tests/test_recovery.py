"""The recovery scanner: re-tick non-terminal runs whose advancing tick was lost.

A worker can commit a step (a matched signal, a fired timer) and then die before
its follow-up ``request_tick`` is processed — leaving a run parked with a resolved
frontier and nobody to move it. ``engine.recover_stalled`` re-ticks such runs;
because replay is idempotent, a genuinely-waiting run simply re-suspends.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()
    calls = {"finalize": 0}

    @activity(name="finalize", registry=reg)
    def finalize(order_id: str) -> str:
        calls["finalize"] += 1
        return f"done_{order_id}"

    @workflow(name="gated", registry=reg)
    def gated(ctx, order_id: str) -> dict:
        ctx.wait_signal("go")
        return {"outcome": ctx.activity(finalize, order_id)}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver, calls=calls)


def test_recover_advances_run_whose_tick_was_lost(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("gated", order_id="A1")
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "SUSPENDED"

    # Simulate a lost tick: match the signal at the store level (committing the
    # COMPLETED wait step) without the re-tick that engine.signal would request.
    with ns.store.locked_run(run_id) as session:
        ns.store.add_signal(run_id, "go", {"ok": True}, session=session)
        ns.store.match_signals(run_id, session=session)
    assert ns.store.get_run(run_id).status == "SUSPENDED"  # still parked — tick was "lost"

    assert ns.engine.recover_stalled(older_than_seconds=0) == 1
    ns.driver.run_until_idle()

    run = ns.store.get_run(run_id)
    assert run.status == "COMPLETED"
    assert run.result["value"] == {"outcome": "done_A1"}


def test_recover_ignores_fresh_runs(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("gated", order_id="A2")
    ns.driver.run_until_idle()

    # The run was just updated, so a generous threshold treats it as healthy.
    assert ns.engine.recover_stalled(older_than_seconds=3600) == 0
    assert ns.store.get_run(run_id).status == "SUSPENDED"


def test_recover_ignores_terminal_runs(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("gated", order_id="A3")
    ns.driver.run_until_idle()
    ns.engine.signal(run_id, "go", {"ok": True})
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "COMPLETED"

    assert ns.engine.recover_stalled(older_than_seconds=0) == 0


def test_recover_retick_of_genuinely_waiting_run_is_noop(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("gated", order_id="A4")
    ns.driver.run_until_idle()  # parked on the wait, no signal delivered

    assert ns.engine.recover_stalled(older_than_seconds=0) == 1
    ns.driver.run_until_idle()

    # Re-ticking a run with no resolved frontier just re-suspends it; the gated
    # activity never runs.
    assert ns.store.get_run(run_id).status == "SUSPENDED"
    assert ns.calls["finalize"] == 0
