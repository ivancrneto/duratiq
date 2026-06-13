"""Versioning with patches: evolve a deployed workflow without breaking the runs
already in flight.

A run is started under the *old* code (charge, then notify) and suspends partway
through. Then the code is "redeployed" with a ``ctx.patched`` gate that adds a new
step. The in-flight run keeps taking the old path on resume — no DeterminismError —
while a brand-new run takes the patched path. Same registry, swapped workflow body,
just like a real deploy.

    cd duratiq && python -m examples.patched_versioning
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, activity
from duratiq.drivers.local import LocalDriver
from duratiq.registry import Workflow

reg = Registry()


@activity(name="charge", registry=reg)
def charge(order_id: str) -> str:
    print(f"  charged {order_id}")
    return f"pay_{order_id}"


@activity(name="notify", registry=reg)
def notify(order_id: str) -> str:
    print(f"  notified {order_id}")
    return "notified"


@activity(name="loyalty_points", registry=reg)
def loyalty_points(order_id: str) -> str:
    print(f"  awarded loyalty points for {order_id}")
    return "points+10"


def checkout_old(ctx, order_id: str) -> dict:
    pay = ctx.activity(charge, order_id)          # seq 0
    note = ctx.activity(notify, order_id)          # seq 1  (in-flight run records this)
    ctx.wait_signal("ship")                        # seq 2  (suspends here)
    return {"mode": "old", "pay": pay, "note": note}


def checkout_new(ctx, order_id: str) -> dict:
    pay = ctx.activity(charge, order_id)          # seq 0
    if ctx.patched("award-loyalty"):               # peek seq 1: old run has a real cmd -> False
        pts = ctx.activity(loyalty_points, order_id)
        note = ctx.activity(notify, order_id)
        ctx.wait_signal("ship")
        return {"mode": "new", "pay": pay, "pts": pts, "note": note}
    note = ctx.activity(notify, order_id)          # seq 1 (old runs realign here)
    ctx.wait_signal("ship")                        # seq 2
    return {"mode": "old", "pay": pay, "note": note}


def main() -> None:
    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    # Deploy v1 and start a run; it charges, then waits for "ship".
    reg.add_workflow(Workflow(fn=checkout_old, name="checkout"))
    inflight = engine.start("checkout", order_id="A1")
    driver.run_until_idle()
    print(f"in-flight run is {engine.get(inflight).status} under the OLD code\n")

    # Redeploy v2 with the patch gate while the run is parked.
    reg.add_workflow(Workflow(fn=checkout_new, name="checkout"))
    print("redeployed with ctx.patched('award-loyalty')\n")

    # The in-flight run resumes — and keeps the OLD behaviour (no loyalty points).
    print("resuming the in-flight run:")
    engine.signal(inflight, "ship", None)
    driver.run_until_idle()
    old_result = engine.get(inflight).result["value"]
    print(f"  -> {old_result}\n")
    assert old_result["mode"] == "old"

    # A brand-new run under v2 takes the patched path.
    print("starting a fresh run under v2:")
    fresh = engine.start("checkout", order_id="B2")
    driver.run_until_idle()
    engine.signal(fresh, "ship", None)
    driver.run_until_idle()
    new_result = engine.get(fresh).result["value"]
    print(f"  -> {new_result}\n")
    assert new_result["mode"] == "new" and new_result["pts"] == "points+10"

    print("✓ old runs kept the old path; new runs took the patch. No replay divergence. ✅")


if __name__ == "__main__":
    main()
