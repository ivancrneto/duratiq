"""Updates: synchronous, mutating requests that return a result.

A signal is fire-and-forget; an **update** carries a response. The workflow registers
a handler (which mutates state and returns a value) and an optional validator (run
read-only first — it can reject bad input before anything changes). ``engine.update``
queues the request and the workflow applies it at a ``ctx.wait_update`` point;
``engine.get_update_result`` reads the handler's return value back.

    cd duratiq && python -m examples.updates
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, UpdateFailed, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@workflow(name="account", registry=reg)
def account(ctx, owner: str) -> dict:
    balance = [0]

    def deposit(amount: int) -> int:
        balance[0] += amount
        return balance[0]  # new balance, returned to the caller

    def withdraw(amount: int) -> int:
        balance[0] -= amount
        return balance[0]

    def check_funds(amount: int) -> None:  # validator for withdraw
        if amount > balance[0]:
            raise ValueError(f"insufficient funds: {amount} > {balance[0]}")

    ctx.set_update_handler("deposit", deposit)
    ctx.set_update_handler("withdraw", withdraw)
    ctx.set_update_validator("withdraw", check_funds)
    ctx.set_update_handler("close", lambda: balance[0])
    ctx.set_query_handler("balance", lambda: balance[0])

    while True:
        if ctx.wait_update() == "close":
            return {"owner": owner, "closing_balance": balance[0]}


def main() -> None:
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("account", owner="ivan")
    driver.run_until_idle()

    def apply(name: str, *args: int) -> None:
        uid = engine.update(run_id, name, *args)
        driver.run_until_idle()
        result = engine.get_update_result(run_id, uid)
        print(f"  {name}({', '.join(map(str, args))}) -> balance {result}")

    print("account opened; applying updates:\n")
    apply("deposit", 100)
    apply("withdraw", 30)

    # The validator rejects this before the handler runs — nothing changes.
    try:
        engine.update(run_id, "withdraw", 999)
    except ValueError as exc:
        print(f"  withdraw(999) rejected: {exc}")

    print(f"  balance query: {engine.query(run_id, 'balance')}")

    # An unknown update name is accepted (no validator) but fails when handled.
    uid = engine.update(run_id, "teleport", 5)
    driver.run_until_idle()
    try:
        engine.get_update_result(run_id, uid)
    except UpdateFailed as exc:
        print(f"  teleport(5) -> {exc}")

    apply("close")
    print(f"\nfinal status: {engine.get(run_id).status}")


if __name__ == "__main__":
    main()
