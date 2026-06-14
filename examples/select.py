"""``ctx.select`` — race several branches, take whichever resolves first.

A checkout that races three outcomes: the card charge succeeding, the customer
cancelling, or a payment window expiring. ``select`` returns the first to resolve and
cancels the losers, so exactly one outcome wins.

To show each winner deterministically we drive the engine by hand (a tiny driver that
ticks but doesn't auto-run activities), then make one branch resolve per run.

    cd duratiq && python -m examples.select
"""

from __future__ import annotations

from collections import deque
from datetime import timedelta

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.models import utcnow

reg = Registry()


@activity(name="charge", registry=reg)
def charge(order_id: str) -> str:
    return f"receipt::{order_id}"


@workflow(name="checkout", registry=reg)
def checkout(ctx, order_id: str) -> dict:
    idx, val = ctx.select(
        ctx.defer(charge, order_id),  # 0: the charge succeeds
        ctx.defer_signal("cancel"),  # 1: the customer cancels
        ctx.defer_timer("PT15M"),  # 2: the payment window expires
    )
    outcome = ["charged", "cancelled", "expired"][idx]
    return {"order_id": order_id, "outcome": outcome, "detail": val}


class HandDriver:
    """Ticks on demand; records (but does not auto-run) activity dispatches."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        engine.driver = self
        self.ticks: deque[str] = deque()
        self.dispatched: list[tuple] = []

    def request_tick(self, run_id: str) -> None:
        self.ticks.append(run_id)

    def dispatch_activity(self, run_id, seq, name, args, kwargs, max_retries) -> None:  # noqa: ANN001
        self.dispatched.append((run_id, seq, name, args))

    def run_ticks(self) -> None:
        while self.ticks:
            self.engine.tick(self.ticks.popleft())


def _start(engine: Engine, driver: HandDriver, order_id: str) -> str:
    run_id = engine.start("checkout", order_id=order_id)
    driver.run_ticks()  # arms all three branches; the run suspends
    return run_id


def main() -> None:
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = HandDriver(engine)

    # 1) The charge resolves first -> charged.
    r1 = _start(engine, driver, "A1")
    _, seq, _, args = driver.dispatched[-1]
    engine.report_activity_result(r1, seq, charge(*args), None)  # the worker reports success
    driver.run_ticks()
    print(f"  A1: charge wins   -> {engine.get(r1).result['value']['outcome']}")

    # 2) A cancel signal arrives first -> cancelled (the charge branch is dropped).
    r2 = _start(engine, driver, "A2")
    engine.signal(r2, "cancel", {"by": "customer"})
    driver.run_ticks()
    print(
        f"  A2: cancel wins   -> {engine.get(r2).result['value']['outcome']} {engine.get(r2).result['value']['detail']}"
    )

    # 3) Nothing happens for 15 minutes -> expired.
    r3 = _start(engine, driver, "A3")
    engine.fire_due_timers(now=utcnow() + timedelta(minutes=16))
    driver.run_ticks()
    print(f"  A3: timer wins    -> {engine.get(r3).result['value']['outcome']}")


if __name__ == "__main__":
    main()
