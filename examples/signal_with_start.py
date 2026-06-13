"""Signal-with-start: ``engine.signal_with_start`` starts a run if it doesn't exist
yet, then delivers a signal to it — deduping on an idempotency key.

A per-customer cart workflow. The first add-to-cart starts the cart and delivers
the item; every later add-to-cart signals the *same* run (same idempotency key ->
same run id). The cart workflow collects items until a "checkout" signal closes it.
The signal is queued before the first tick, so the run's ``ctx.wait_signal`` finds
it already waiting — no race against the start.

    cd duratiq && python -m examples.signal_with_start
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@activity(name="place_order", registry=reg)
def place_order(items: list) -> str:
    print(f"  placing order for {items}")
    return f"order of {len(items)} item(s)"


@workflow(name="cart", registry=reg)
def cart(ctx, customer_id: str) -> dict:
    items: list = []
    while True:
        event = ctx.wait_signal("cart_event")
        if event.get("checkout"):
            break
        items.append(event["sku"])
    return {"customer_id": customer_id, "result": ctx.activity(place_order, items)}


def main() -> None:
    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    key = "cart:cust-42"

    # First add-to-cart: no run exists yet, so this one starts the cart workflow.
    run_id = engine.signal_with_start(
        "cart", signal="cart_event", payload={"sku": "A1"}, idempotency_key=key, customer_id="cust-42"
    )
    driver.run_until_idle()
    print(f"first add-to-cart started run {run_id[:8]}… (status {engine.get(run_id).status})")

    # More add-to-carts: same idempotency key -> the same running cart, just signalled.
    again = engine.signal_with_start("cart", signal="cart_event", payload={"sku": "B2"}, idempotency_key=key)
    assert again == run_id
    engine.signal_with_start("cart", signal="cart_event", payload={"sku": "C3"}, idempotency_key=key)
    driver.run_until_idle()
    print(f"two more add-to-carts went to the same run ({again[:8]}…)")

    # Checkout closes the cart.
    engine.signal_with_start("cart", signal="cart_event", payload={"checkout": True}, idempotency_key=key)
    driver.run_until_idle()

    run = engine.get(run_id)
    print(f"\nrun is {run.status}: {run.result['value']}")
    assert run.status == "COMPLETED"
    assert run.result["value"]["result"] == "order of 3 item(s)"
    print("\n✓ one workflow, started once and signalled many times, deduped on its key. ✅")


if __name__ == "__main__":
    main()
