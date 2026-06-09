"""Crash recovery across *real OS processes*, on a file-backed SQLite store.

The engine's durability claim is: if a worker dies mid-workflow, a fresh worker
backed by the same store resumes exactly where it left off — already-completed
activities are replayed from history, never re-executed.

The in-process unit test proves this by discarding a driver. This example proves
the stronger thing: a process that **hard-exits** (``os._exit`` — no cleanup, like
``kill -9``) after one activity, and a second, independent ``python`` process that
picks the run up from the SQLite file and finishes it.

Each activity appends one line (with its PID) to a side-effect log. That log is the
proof: after recovery it contains each activity exactly once, and the survivor's
PID differs from the crasher's.

    cd duratiq && python -m examples.crash_recovery
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver

# The side-effect log path travels to the activities via the environment so both
# the crasher and the survivor write to the same file.
SIDEFX = os.environ.get("DURATIQ_DEMO_SIDEFX", "")

reg = Registry()


def _record(name: str) -> None:
    """Append proof that this activity actually executed, in this process."""
    with open(SIDEFX, "a") as f:
        f.write(f"{name} ran in pid {os.getpid()}\n")


@activity(name="charge_card", registry=reg)
def charge_card(order_id: str, amount: int) -> str:
    _record("charge_card")
    print(f"  [pid {os.getpid()}] charged {amount} for {order_id}")
    return f"pay_{order_id}"


@activity(name="reserve_inventory", registry=reg)
def reserve_inventory(order_id: str) -> str:
    _record("reserve_inventory")
    print(f"  [pid {os.getpid()}] reserved inventory for {order_id}")
    return f"resv_{order_id}"


@activity(name="ship_order", registry=reg)
def ship_order(order_id: str, payment_id: str, reservation_id: str) -> str:
    _record("ship_order")
    print(f"  [pid {os.getpid()}] shipped {order_id} ({payment_id}, {reservation_id})")
    return f"track_{order_id}"


@workflow(name="fulfil", registry=reg)
def fulfil(ctx, order_id: str) -> dict:
    payment_id = ctx.activity(charge_card, order_id, 1999)
    reservation_id = ctx.activity(reserve_inventory, order_id)
    tracking = ctx.activity(ship_order, order_id, payment_id, reservation_id)
    return {"order_id": order_id, "payment_id": payment_id, "tracking": tracking}


def _engine(db_path: str) -> tuple[Engine, LocalDriver]:
    store = SqlStore(f"sqlite:///{db_path}")
    store.create_all()
    engine = Engine(reg, store)
    return engine, LocalDriver(engine)


# --------------------------------------------------------------------------- #
# Child role 1: start the run, finish exactly ONE activity, then hard-crash.
# --------------------------------------------------------------------------- #
def phase_crash(db_path: str) -> None:
    engine, driver = _engine(db_path)
    run_id = engine.start("fulfil", order_id="A123")
    print(f"RUN_ID={run_id}", flush=True)

    driver.step()  # tick: schedules charge_card, run SUSPENDS
    driver.step()  # activity: runs charge_card once, records its result

    run = engine.get(run_id)
    print(f"  [pid {os.getpid()}] crashing now — run status is {run.status}", flush=True)
    # Hard kill: no atexit, no flush of anything still buffered, no clean shutdown.
    # The pending tick living in this process's in-memory queue is lost forever.
    os._exit(42)


# --------------------------------------------------------------------------- #
# Child role 2: a brand-new process resumes the run from the same DB file.
# --------------------------------------------------------------------------- #
def phase_resume(db_path: str, run_id: str) -> None:
    engine, driver = _engine(db_path)

    before = engine.get(run_id)
    print(f"  [pid {os.getpid()}] picked up run in status {before.status}; re-ticking", flush=True)

    driver.request_tick(run_id)  # what a recovery scanner would do for a stale SUSPENDED run
    driver.run_until_idle()

    run = engine.get(run_id)
    print(f"  [pid {os.getpid()}] finished: {run.status} -> {run.result['value']}", flush=True)


# --------------------------------------------------------------------------- #
# Orchestrator: spawns the two child processes and inspects the durable state.
# --------------------------------------------------------------------------- #
def main() -> None:
    tmp = tempfile.mkdtemp(prefix="duratiq_crash_")
    db_path = os.path.join(tmp, "runs.db")
    sidefx = os.path.join(tmp, "side_effects.log")
    open(sidefx, "w").close()

    env = {**os.environ, "DURATIQ_DEMO_SIDEFX": sidefx}
    me = [sys.executable, __file__]

    print("=== phase 1: worker starts the run, then crashes mid-flight ===")
    p1 = subprocess.run([*me, "crash", db_path], env=env, capture_output=True, text=True)
    print(p1.stdout, end="")
    run_id = next(line.split("=", 1)[1] for line in p1.stdout.splitlines() if line.startswith("RUN_ID="))
    assert p1.returncode == 42, f"expected hard-exit 42, got {p1.returncode}"
    print(f"  (process exited with code {p1.returncode} — a real crash, not a clean return)\n")

    # Inspect the durable state left behind, from the orchestrator process.
    engine, _ = _engine(db_path)
    run = engine.get(run_id)
    steps = engine.store.get_steps(run_id)
    print("=== durable state after the crash (read by a third process) ===")
    print(f"  run {run_id[:8]} status={run.status}")
    for s in steps:
        print(f"    step seq={s.seq} {s.name:<18} status={s.status}")
    print()

    print("=== phase 2: a fresh, independent worker resumes the run ===")
    p2 = subprocess.run([*me, "resume", db_path, run_id], env=env, capture_output=True, text=True)
    print(p2.stdout, end="")
    if p2.returncode != 0:
        print(p2.stderr, file=sys.stderr)
        sys.exit(1)
    print()

    print("=== proof: side-effect log (one line per real execution) ===")
    lines = [ln for ln in open(sidefx).read().splitlines() if ln]
    for ln in lines:
        print(f"  {ln}")

    names = [ln.split(" ran ")[0] for ln in lines]
    crash_pid = next(ln for ln in p1.stdout.splitlines() if "crashing now" in ln).split("pid ")[1].split("]")[0]
    print()
    if names.count("charge_card") == 1 and sorted(names) == ["charge_card", "reserve_inventory", "ship_order"]:
        print(f"  ✓ charge_card executed exactly ONCE despite the crash (in pid {crash_pid}),")
        print("  ✓ and was replayed — not re-run — by the surviving worker. Durable. ✅")
    else:
        print(f"  ✗ unexpected execution history: {names}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "crash":
        phase_crash(sys.argv[2])
    elif len(sys.argv) >= 4 and sys.argv[1] == "resume":
        phase_resume(sys.argv[2], sys.argv[3])
    else:
        main()
