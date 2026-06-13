"""Listing runs: enumerate and filter workflow runs for an ops/admin view.

Starts a mix of runs — some complete, some fail, some park on a signal — then uses
``engine.list_runs`` / ``engine.count_runs`` to slice them by status and name.

    cd duratiq && python -m examples.list_runs
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@activity(name="explode", registry=reg)
def explode() -> None:
    raise RuntimeError("boom")


@workflow(name="quick", registry=reg)
def quick(ctx, n: int) -> int:
    return n


@workflow(name="broken", registry=reg)
def broken(ctx) -> str:
    ctx.activity(explode)
    return "unreachable"


@workflow(name="approval", registry=reg)
def approval(ctx) -> str:
    return ctx.wait_signal("decision")


def main() -> None:
    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    for n in range(3):
        engine.start("quick", n=n)
    engine.start("broken")
    engine.start("approval")  # will park on its signal
    driver.run_until_idle()

    print(f"total runs:           {engine.count_runs()}")
    print(f"completed:            {engine.count_runs(status='COMPLETED')}")
    print(f"failed:               {engine.count_runs(status='FAILED')}")
    print(f"in flight (suspended):{engine.count_runs(status='SUSPENDED')}")

    print("\nmost recent 2 runs:")
    for r in engine.list_runs(limit=2):
        print(f"  {r.name:<10} {r.status}")

    print("\nonly 'quick' runs, oldest first:")
    for r in engine.list_runs(name="quick", newest_first=False):
        print(f"  {r.name:<10} {r.status}  input={r.input}")

    print("\neverything not yet done:")
    for r in engine.list_runs(status=["PENDING", "RUNNING", "SUSPENDED"]):
        print(f"  {r.name:<10} {r.status}")

    assert engine.count_runs() == 5
    assert engine.count_runs(status="COMPLETED") == 3
    assert engine.count_runs(name="quick") == 3
    print("\n✓ runs enumerated and filtered by status and name. ✅")


if __name__ == "__main__":
    main()
