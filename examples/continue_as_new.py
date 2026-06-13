"""Continue-as-new: a looping workflow that restarts with fresh history instead of
growing it without bound.

A queue-draining loop. Each iteration consumes one item via a signal, processes it,
then ``ctx.continue_as_new`` restarts the run from seq 0 with the running tally —
discarding the previous iteration's step history. Items queued ahead of time carry
over across each restart, so nothing is dropped. The run id stays the same
throughout; only the history is truncated.

    cd duratiq && python -m examples.continue_as_new
"""

from __future__ import annotations

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()


@activity(name="process", registry=reg)
def process(item: str) -> str:
    print(f"  processed {item}")
    return item.upper()


@workflow(name="worker", registry=reg)
def worker(ctx, done: list) -> dict:
    item = ctx.wait_signal("job")  # park until the next job arrives
    if item == "DRAIN":
        return {"processed": done}
    result = ctx.activity(process, item)
    # Restart fresh, carrying the tally — history does not accumulate across jobs.
    ctx.continue_as_new(done=done + [result])


def main() -> None:
    store = SqlStore()  # in-memory SQLite
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("worker", done=[])
    # Queue several jobs up front; they sit unconsumed and carry across each restart.
    for item in ("alpha", "beta", "gamma"):
        engine.signal(run_id, "job", item)
    driver.run_until_idle()

    run = engine.get(run_id)
    print(f"\nafter 3 jobs the run is {run.status}; history holds "
          f"{len(store.get_steps(run_id))} step(s) — not 3+ iterations' worth")
    assert run.status == "SUSPENDED"
    # The whole history is just the current iteration's single pending wait.
    assert len(store.get_steps(run_id)) == 1

    engine.signal(run_id, "job", "DRAIN")
    driver.run_until_idle()
    run = engine.get(run_id)
    print(f"\nrun is {run.status}: {run.result['value']}")
    assert run.status == "COMPLETED"
    assert run.result["value"]["processed"] == ["ALPHA", "BETA", "GAMMA"]
    print("\n✓ a forever-loop ran many jobs with a bounded, ever-fresh history. ✅")


if __name__ == "__main__":
    main()
