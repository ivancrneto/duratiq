"""End-to-end: a real Dramatiq worker pool and the real scanner loop, in wall-clock time.

Each test runs the deployment-shaped stack in one process:

* ``DramatiqDriver`` on a ``StubBroker`` with real ``dramatiq.Worker`` threads, so
  ticks and activities are encoded as messages, retried by Dramatiq's ``Retries``
  middleware with backoff, and processed concurrently;
* ``Scanner.run_forever`` on a background thread firing real timers, activity
  timeouts, and stalled-run recovery — nothing is fast-forwarded;
* a store on SQLite (file-backed) and, when ``DURATIQ_TEST_DATABASE_URL`` is set, on
  PostgreSQL, where run serialisation is the real ``pg_advisory_xact_lock``.

The workflow under test is an order saga that touches most of the engine at once:
a parallel fan-out, an activity that retries with backoff, compensation on failure,
a durable timer, a signal with a timeout, and a child workflow.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

dramatiq = pytest.importorskip("dramatiq")

from dramatiq.brokers.stub import StubBroker  # noqa: E402

from duratiq import TIMEOUT, ActivityFailed, Engine, Registry, Scanner, SqlStore, activity, workflow  # noqa: E402
from duratiq.drivers.dramatiq import DramatiqDriver  # noqa: E402

COOLING_OFF_SECONDS = 0.3
TERMINAL = ("COMPLETED", "FAILED", "CANCELLED")


class Ledger:
    """Thread-safe record of every activity execution, across worker threads."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.calls: list[tuple[str, str]] = []

    def record(self, activity_name: str, order_id: str) -> int:
        with self._lock:
            self.calls.append((activity_name, order_id))
            return sum(1 for call in self.calls if call == (activity_name, order_id))

    def count(self, activity_name: str, order_id: str) -> int:
        with self._lock:
            return self.calls.count((activity_name, order_id))

    def per_activity(self) -> Counter:
        with self._lock:
            return Counter(name for name, _ in self.calls)


def build_registry(ledger: Ledger) -> Registry:
    reg = Registry()

    @activity(name="reserve_stock", registry=reg)
    def reserve_stock(order_id: str) -> str:
        ledger.record("reserve_stock", order_id)
        return f"res-{order_id}"

    @activity(name="send_email", registry=reg)
    def send_email(order_id: str, template: str) -> str:
        ledger.record("send_email", order_id)
        return template

    @activity(name="charge_card", registry=reg, max_retries=3, min_backoff_ms=10, max_backoff_ms=50)
    def charge_card(order_id: str, amount: int) -> str:
        attempt = ledger.record("charge_card", order_id)
        if amount < 0:
            raise ValueError(f"invalid amount {amount}")
        if order_id.startswith("flaky") and attempt < 3:
            raise ConnectionError(f"gateway timeout (attempt {attempt})")
        return f"pay-{order_id}"

    @activity(name="refund", registry=reg)
    def refund(order_id: str) -> str:
        ledger.record("refund", order_id)
        return f"refund-{order_id}"

    @activity(name="ship", registry=reg)
    def ship(order_id: str) -> str:
        ledger.record("ship", order_id)
        return f"track-{order_id}"

    @activity(name="ping", registry=reg, start_to_close_ms=300, max_retries=2)
    def ping(order_id: str) -> str:
        ledger.record("ping", order_id)
        return "pong"

    @workflow(name="fulfil", registry=reg)
    def fulfil(ctx, order_id: str) -> str:
        return ctx.activity(ship, order_id)

    @workflow(name="order", registry=reg)
    def order(ctx, order_id: str, amount: int, approval_timeout: float = 10.0) -> dict:
        reservation, _ = ctx.gather(ctx.defer(reserve_stock, order_id), ctx.defer(send_email, order_id, "received"))
        try:
            payment = ctx.activity(charge_card, order_id, amount)
        except ActivityFailed:
            ctx.activity(refund, order_id)
            return {"status": "payment_failed"}
        ctx.sleep(COOLING_OFF_SECONDS)  # a real durable timer, fired by the scanner thread
        decision = ctx.wait_signal("approval", timeout=approval_timeout)
        if decision is TIMEOUT or not decision.get("approved"):
            ctx.activity(refund, order_id)
            return {"status": "rejected", "timed_out": decision is TIMEOUT}
        tracking = ctx.child_workflow(fulfil, order_id=order_id)
        return {"status": "shipped", "reservation": reservation, "payment": payment, "tracking": tracking}

    @workflow(name="pinger", registry=reg)
    def pinger(ctx, order_id: str) -> str:
        return ctx.activity(ping, order_id)

    return reg


@contextmanager
def running_stack(
    store: SqlStore, *, worker_threads: int, recovery_older_than: float = 60.0
) -> Iterator[SimpleNamespace]:
    """Wire engine + Dramatiq worker pool + scanner thread; tear everything down after."""
    ledger = Ledger()
    engine = Engine(build_registry(ledger), store)
    broker = StubBroker()
    driver = DramatiqDriver(engine, broker=broker)
    worker = dramatiq.Worker(broker, worker_threads=worker_threads, worker_timeout=50)
    scanner = Scanner(
        engine,
        timer_interval=0.05,
        activity_timeout_interval=0.05,
        workflow_timeout_interval=0.05,
        schedule_interval=0.05,
        recovery_interval=0.1,
        recovery_older_than=recovery_older_than,
    )
    scanner_thread = threading.Thread(target=scanner.run_forever, name="duratiq-scanner", daemon=True)
    worker.start()
    scanner_thread.start()
    try:
        yield SimpleNamespace(engine=engine, driver=driver, broker=broker, store=store, ledger=ledger)
    finally:
        scanner.stop()
        scanner_thread.join(timeout=10)
        worker.stop()
        broker.close()


def _threads(backend_name: str) -> int:
    # Postgres serialises a run's ticks with an advisory lock, so go wide. SQLite's
    # in-process lock also handles several threads, but keep it modest: it's the
    # dev/test backend, and the file lock serialises writers anyway.
    return 8 if backend_name == "postgres" else 2


def _status(stack: SimpleNamespace, run_id: str) -> str:
    return stack.store.get_run(run_id).status


def _waiting_for_approval(stack: SimpleNamespace, run_id: str) -> bool:
    return any(step.kind == "SIGNAL_WAIT" and step.status == "SCHEDULED" for step in stack.store.get_steps(run_id))


def _finish(stack: SimpleNamespace, run_id: str, wait_for: Callable[..., None]) -> dict:
    wait_for(lambda: _status(stack, run_id) in TERMINAL, what=f"run {run_id} to finish")
    run = stack.store.get_run(run_id)
    assert run.status == "COMPLETED", run.error
    return run.result["value"]


def test_order_saga_happy_path(backend, wait_for) -> None:
    name, store = backend
    with running_stack(store, worker_threads=_threads(name)) as stack:
        started = time.monotonic()
        run_id = stack.engine.start("order", order_id="A1", amount=100)

        wait_for(lambda: _waiting_for_approval(stack, run_id), what="the run to park on the approval signal")
        # It only got there by sleeping through the real cooling-off timer.
        assert time.monotonic() - started >= COOLING_OFF_SECONDS
        assert _status(stack, run_id) == "SUSPENDED"

        assert stack.engine.signal(run_id, "approval", {"approved": True})
        result = _finish(stack, run_id, wait_for)

    assert result == {"status": "shipped", "reservation": "res-A1", "payment": "pay-A1", "tracking": "track-A1"}
    assert stack.ledger.per_activity() == {"reserve_stock": 1, "send_email": 1, "charge_card": 1, "ship": 1}
    kinds = [step.kind for step in store.get_steps(run_id)]
    assert kinds == ["ACTIVITY", "ACTIVITY", "ACTIVITY", "TIMER", "SIGNAL_WAIT", "TIMER", "CHILD_WORKFLOW"]


def test_flaky_payment_is_retried_with_backoff(backend, wait_for) -> None:
    name, store = backend
    with running_stack(store, worker_threads=_threads(name)) as stack:
        run_id = stack.engine.start("order", order_id="flaky-1", amount=100)
        wait_for(lambda: _waiting_for_approval(stack, run_id), what="the run to get past the payment")
        stack.engine.signal(run_id, "approval", {"approved": True})
        assert _finish(stack, run_id, wait_for)["status"] == "shipped"

    assert stack.ledger.count("charge_card", "flaky-1") == 3
    charge_step = store.get_steps(run_id)[2]
    assert (charge_step.name, charge_step.status, charge_step.attempt) == ("charge_card", "COMPLETED", 2)


def test_failed_payment_runs_compensation(backend, wait_for) -> None:
    name, store = backend
    with running_stack(store, worker_threads=_threads(name)) as stack:
        run_id = stack.engine.start("order", order_id="B2", amount=-1)
        assert _finish(stack, run_id, wait_for) == {"status": "payment_failed"}

    assert stack.ledger.count("charge_card", "B2") == 4  # 1 attempt + max_retries=3
    assert stack.ledger.count("refund", "B2") == 1
    assert stack.ledger.count("ship", "B2") == 0


def test_unanswered_approval_times_out(backend, wait_for) -> None:
    name, store = backend
    with running_stack(store, worker_threads=_threads(name)) as stack:
        run_id = stack.engine.start("order", order_id="C3", amount=5, approval_timeout=0.3)
        assert _finish(stack, run_id, wait_for) == {"status": "rejected", "timed_out": True}

    assert stack.ledger.count("refund", "C3") == 1
    # A late approval is stored but can no longer change the outcome.
    assert stack.engine.signal(run_id, "approval", {"approved": True}) is False


def test_lost_tick_is_recovered_by_the_scanner(backend, wait_for) -> None:
    name, store = backend
    with running_stack(store, worker_threads=_threads(name), recovery_older_than=0.2) as stack:
        # Drop the very first tick message: the run row is committed, but nothing
        # is on the broker to advance it (a crash between commit and enqueue).
        deliver = stack.driver.request_tick
        lost: list[str] = []

        def lossy_request_tick(run_id: str) -> None:
            if not lost:
                lost.append(run_id)
                return
            deliver(run_id)

        stack.driver.request_tick = lossy_request_tick
        run_id = stack.engine.start("pinger", order_id="D4")
        assert lost == [run_id]
        assert _finish(stack, run_id, wait_for) == "pong"


def test_lost_activity_message_is_redispatched_after_its_timeout(backend, wait_for) -> None:
    name, store = backend
    with running_stack(store, worker_threads=_threads(name)) as stack:
        dispatch = stack.driver.dispatch_activity
        lost: list[int] = []

        def lossy_dispatch(run_id: str, seq: int, *args, **kwargs) -> None:
            if not lost:
                lost.append(seq)
                return
            dispatch(run_id, seq, *args, **kwargs)

        stack.driver.dispatch_activity = lossy_dispatch
        started = time.monotonic()
        run_id = stack.engine.start("pinger", order_id="E5")
        assert _finish(stack, run_id, wait_for) == "pong"

    assert lost == [0]
    assert time.monotonic() - started >= 0.3  # only recovered once start_to_close elapsed
    assert stack.ledger.count("ping", "E5") == 1  # the lost attempt never ran


def test_many_concurrent_orders_each_run_every_activity_exactly_once(backend, wait_for) -> None:
    name, store = backend
    orders = [f"N{i:02d}" for i in range(24)]
    with running_stack(store, worker_threads=_threads(name)) as stack:
        run_ids = {order_id: stack.engine.start("order", order_id=order_id, amount=10) for order_id in orders}
        # Half the approvals race ahead of the wait (queued in the inbox), half arrive
        # after the run has parked on it. Both must be consumed exactly once.
        early, late = orders[::2], orders[1::2]
        for order_id in early:
            assert stack.engine.signal(run_ids[order_id], "approval", {"approved": True})
        for order_id in late:
            wait_for(lambda rid=run_ids[order_id]: _waiting_for_approval(stack, rid), what=f"{order_id} to park")
            assert stack.engine.signal(run_ids[order_id], "approval", {"approved": True})
        results = {order_id: _finish(stack, run_id, wait_for) for order_id, run_id in run_ids.items()}

    assert all(result["status"] == "shipped" for result in results.values()), results
    assert results["N07"]["tracking"] == "track-N07"
    n = len(orders)
    assert stack.ledger.per_activity() == {"reserve_stock": n, "send_email": n, "charge_card": n, "ship": n}
    for order_id in orders:
        for activity_name in ("reserve_stock", "send_email", "charge_card", "ship"):
            assert stack.ledger.count(activity_name, order_id) == 1, (activity_name, order_id)
