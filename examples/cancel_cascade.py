"""Cancellation cascade: cancelling a parent workflow takes its running children
(and grandchildren) down with it.

A top workflow runs a child, which runs a grandchild; each parks on a signal so the
whole tree is alive at once. Cancelling the top cancels all three.

    cd duratiq && python -m examples.cancel_cascade
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@workflow(name="grandchild", registry=reg)
def grandchild(ctx) -> str:
    return ctx.wait_signal("go")  # parks


@workflow(name="child", registry=reg)
def child(ctx) -> str:
    return ctx.child_workflow("grandchild")


@workflow(name="top", registry=reg)
def top(ctx) -> str:
    return ctx.child_workflow("child")


def main() -> None:
    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    top_id = engine.start("top")
    driver.run_until_idle()

    child_run = store.find_child_run(top_id, 0)
    grandchild_run = store.find_child_run(child_run.id, 0)
    tree = {"top": top_id, "child": child_run.id, "grandchild": grandchild_run.id}

    print("before cancel:")
    for label, rid in tree.items():
        print(f"  {label:<11} {store.get_run(rid).status}")

    engine.cancel(top_id)

    print("\nafter cancel(top):")
    for label, rid in tree.items():
        print(f"  {label:<11} {store.get_run(rid).status}")

    assert all(store.get_run(rid).status == "CANCELLED" for rid in tree.values())
    print("\n✓ cancelling the top brought the whole sub-tree down. ✅")


if __name__ == "__main__":
    main()
