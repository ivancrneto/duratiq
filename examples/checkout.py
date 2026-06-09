"""Runnable example on the synchronous LocalDriver (no broker needed).

    cd duratiq && python -m examples.checkout
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@activity(name="charge_card", registry=reg)
def charge_card(order_id: str, amount: int) -> str:
    print(f"  [activity] charging {amount} for {order_id}")
    return f"pay_{order_id}"


@activity(name="send_receipt", registry=reg)
def send_receipt(order_id: str, payment_id: str) -> bool:
    print(f"  [activity] emailing receipt for {order_id} ({payment_id})")
    return True


@workflow(name="checkout", registry=reg)
def checkout(ctx, order_id: str) -> dict:
    payment_id = ctx.activity(charge_card, order_id, 1999)
    ctx.activity(send_receipt, order_id, payment_id)
    return {"order_id": order_id, "payment_id": payment_id}


def main() -> None:
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("checkout", order_id="A123")
    driver.run_until_idle()

    run = engine.get(run_id)
    print(f"status: {run.status}")
    print(f"result: {run.result}")


if __name__ == "__main__":
    main()
