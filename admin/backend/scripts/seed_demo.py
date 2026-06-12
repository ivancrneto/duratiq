"""Seed a file-backed SQLite DB with a few duratiq runs, for demoing the admin.

    uv run python scripts/seed_demo.py [./duratiq.db]

Runs the checkout workflow a couple of times (LocalDriver, no broker) so the
admin has real runs + steps to display.
"""

from __future__ import annotations

import sys

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@activity(name="charge_card", registry=reg)
def charge_card(order_id: str, amount: int) -> str:
    return f"pay_{order_id}"


@activity(name="email_receipt", registry=reg)
def email_receipt(order_id: str, payment_id: str) -> str:
    return f"emailed:{order_id}"


@activity(name="charge_card_flaky", registry=reg)
def charge_card_flaky(order_id: str) -> str:
    raise RuntimeError("payment gateway timeout")


@workflow(name="checkout", registry=reg)
def checkout(ctx, order_id: str):  # noqa: ANN001
    payment_id = ctx.activity(charge_card, order_id, 1999)
    ctx.activity(email_receipt, order_id, payment_id)
    return {"order_id": order_id, "payment_id": payment_id}


@workflow(name="checkout_flaky", registry=reg)
def checkout_flaky(ctx, order_id: str):  # noqa: ANN001
    ctx.activity(charge_card_flaky, order_id)
    return {"order_id": order_id}


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else "./duratiq.db"
    store = SqlStore(url=f"sqlite:///{path}")
    store.create_all()
    engine = Engine(reg, store)
    LocalDriver(engine)

    # Happy-path runs -> COMPLETED.
    for order_id in ("A123", "B456", "C789"):
        run_id = engine.start("checkout", order_id=order_id)
        engine.driver.run_until_idle()
        print(f"{order_id}: {engine.get(run_id).status}")

    # A failing run -> FAILED (retryable in the admin).
    failed = engine.start("checkout_flaky", order_id="D999")
    engine.driver.run_until_idle()
    print(f"D999: {engine.get(failed).status}")

    # A partially-pumped run -> SUSPENDED (cancellable in the admin): process just
    # the first tick (which schedules an activity) and leave the activity un-run.
    suspended = engine.start("checkout", order_id="E000")
    engine.driver.step()
    print(f"E000: {engine.get(suspended).status}")

    print(f"\nSeeded {path}. Start the admin with DATABASE_URL=sqlite:///{path}")


if __name__ == "__main__":
    main()
