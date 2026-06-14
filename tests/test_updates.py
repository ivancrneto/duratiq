"""Updates: synchronous, mutating requests that return a result.

``engine.update`` queues an update; the workflow consumes it at a ``ctx.wait_update``
point, runs the registered handler (which mutates state and returns a value), and the
result is recorded for ``engine.get_update_result``. An optional validator runs
read-only first and can reject before anything mutates.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from duratiq import UPDATE_PENDING, Engine, Registry, SqlStore, UpdateFailed, WorkflowNotFound, workflow
from duratiq.drivers.local import LocalDriver


@pytest.fixture
def ns() -> SimpleNamespace:
    reg = Registry()

    @workflow(name="account", registry=reg)
    def account(ctx) -> dict:
        balance = [0]

        def deposit(amount: int) -> int:
            balance[0] += amount
            return balance[0]  # the new balance, returned to the caller

        def validate_deposit(amount: int) -> None:
            if amount <= 0:
                raise ValueError("amount must be positive")

        def boom() -> None:
            raise RuntimeError("handler blew up")

        ctx.set_update_handler("deposit", deposit)
        ctx.set_update_validator("deposit", validate_deposit)
        ctx.set_update_handler("boom", boom)
        ctx.set_update_handler("close", lambda: balance[0])
        ctx.set_query_handler("balance", lambda: balance[0])

        while True:
            if ctx.wait_update() == "close":
                return {"final": balance[0]}

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(reg=reg, store=store, engine=engine, driver=driver)


def test_update_mutates_state_and_returns_result(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("account")
    ns.driver.run_until_idle()

    uid1 = ns.engine.update(run_id, "deposit", 100)
    ns.driver.run_until_idle()
    assert ns.engine.get_update_result(run_id, uid1) == 100
    assert ns.engine.query(run_id, "balance") == 100

    uid2 = ns.engine.update(run_id, "deposit", 50)
    ns.driver.run_until_idle()
    assert ns.engine.get_update_result(run_id, uid2) == 150
    # The first update's recorded result is unchanged (stable across replays).
    assert ns.engine.get_update_result(run_id, uid1) == 100
    assert ns.engine.query(run_id, "balance") == 150


def test_result_is_pending_until_applied(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("account")
    ns.driver.run_until_idle()

    uid = ns.engine.update(run_id, "deposit", 10)
    # The tick hasn't been pumped yet, so the handler hasn't run.
    assert ns.engine.get_update_result(run_id, uid) is UPDATE_PENDING
    ns.driver.run_until_idle()
    assert ns.engine.get_update_result(run_id, uid) == 10


def test_validator_rejects_before_mutating(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("account")
    ns.driver.run_until_idle()

    with pytest.raises(ValueError, match="positive"):
        ns.engine.update(run_id, "deposit", -5)

    # Nothing was queued or applied — balance is still zero.
    ns.driver.run_until_idle()
    assert ns.engine.query(run_id, "balance") == 0


def test_handler_error_surfaces_as_update_failed(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("account")
    ns.driver.run_until_idle()

    uid = ns.engine.update(run_id, "boom")
    ns.driver.run_until_idle()
    with pytest.raises(UpdateFailed) as exc:
        ns.engine.get_update_result(run_id, uid)
    assert exc.value.error["type"] == "RuntimeError"


def test_unknown_handler_is_recorded_failed(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("account")
    ns.driver.run_until_idle()

    uid = ns.engine.update(run_id, "nope")  # no handler, no validator -> accepted, then fails
    ns.driver.run_until_idle()
    with pytest.raises(UpdateFailed) as exc:
        ns.engine.get_update_result(run_id, uid)
    assert exc.value.error["type"] == "UpdateHandlerNotFound"


def test_update_on_terminal_run_raises(ns: SimpleNamespace) -> None:
    run_id = ns.engine.start("account")
    ns.driver.run_until_idle()
    ns.engine.update(run_id, "deposit", 5)
    ns.driver.run_until_idle()
    ns.engine.update(run_id, "close")
    ns.driver.run_until_idle()
    assert ns.store.get_run(run_id).status == "COMPLETED"

    with pytest.raises(ValueError, match="cannot accept updates"):
        ns.engine.update(run_id, "deposit", 1)


def test_unknown_run_and_update(ns: SimpleNamespace) -> None:
    with pytest.raises(WorkflowNotFound):
        ns.engine.update("nope", "deposit", 1)
    run_id = ns.engine.start("account")
    ns.driver.run_until_idle()
    with pytest.raises(WorkflowNotFound):
        ns.engine.get_update_result(run_id, "no-such-update")
