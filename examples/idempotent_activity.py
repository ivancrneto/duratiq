"""Idempotent activities: wrap a non-idempotent effect in ``run_once`` so a retry
doesn't repeat it.

A payment activity charges a card and then does a flaky post-charge step. The first
attempt charges successfully, then the flaky step fails — so the activity is retried.
On the retry the charge is *not* repeated (``run_once`` returns the recorded result);
only the flaky step runs again, and this time it succeeds. The card is charged once
even though the activity ran twice.

    cd duratiq && python -m examples.idempotent_activity
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, activity, activity_info, run_once, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()

charges: list[str] = []  # every real charge appended here — should end length 1
attempts = {"n": 0}


def _charge_card(order_id: str) -> str:
    charges.append(order_id)
    print(f"  💳 charged {order_id} (charge #{len(charges)})")
    return f"pay_{order_id}"


@activity(name="pay", registry=reg, max_retries=3)
def pay(order_id: str) -> str:
    attempts["n"] += 1
    info = activity_info()
    # The charge runs at most once per (run_id, seq), even across retries.
    payment_id = run_once(info.idempotency_key, lambda: _charge_card(order_id))
    # A flaky post-charge step that fails the first time and retries the activity.
    if attempts["n"] == 1:
        print(f"  ⚠️  attempt {attempts['n']}: post-charge step failed; will retry")
        raise RuntimeError("receipt service timed out")
    print(f"  ✅ attempt {attempts['n']}: post-charge step succeeded")
    return payment_id


@workflow(name="order", registry=reg)
def order(ctx, order_id: str) -> dict:
    return {"payment": ctx.activity(pay, order_id)}


def main() -> None:
    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("order", order_id="A123")
    driver.run_until_idle()

    run = engine.get(run_id)
    print(f"\nrun is {run.status}: {run.result['value']}")
    print(f"activity attempts: {attempts['n']}, real charges: {len(charges)}")
    assert run.status == "COMPLETED"
    assert attempts["n"] == 2 and len(charges) == 1
    print("\n✓ the activity retried, but the card was charged exactly once. ✅")


if __name__ == "__main__":
    main()
