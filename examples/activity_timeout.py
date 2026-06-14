"""Activity start-to-close timeouts: a hung activity can't wedge its run forever.

An activity given ``start_to_close_ms`` carries a deadline. If a dispatched attempt
never reports back — the worker hung, or the message was lost — the activity-timeout
scanner retries it (a fresh dispatch + new deadline) while the retry budget lasts,
then fails it so the workflow sees ``ActivityFailed``.

Here we drive the ``LocalDriver`` by hand and *never run* the dispatched activity —
exactly the "worker took the message and never came back" case — then fast-forward
``now`` through the scanner, the same call a periodic scanner makes.

    cd duratiq && python -m examples.activity_timeout
"""

from __future__ import annotations

from datetime import timedelta

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import utcnow

reg = Registry()


@activity(name="call_flaky_api", registry=reg, max_retries=2, start_to_close_ms=30_000)
def call_flaky_api(order_id: str) -> str:
    # In this demo the worker never actually runs this — we're simulating a hang.
    return f"ok::{order_id}"


@workflow(name="checkout", registry=reg)
def checkout(ctx, order_id: str) -> dict:
    return {"result": ctx.activity(call_flaky_api, order_id)}


def main() -> None:
    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    run_id = engine.start("checkout", order_id="A123")
    driver.step()  # process only the tick: it schedules + dispatches the activity...
    driver.queue.clear()  # ...and we drop the dispatch, so the activity never reports

    step = store.get_step(run_id, 0)
    print(f"activity dispatched; deadline at {step.timeout_at} (status {step.status})\n")

    now = utcnow()
    for label, advance in [("T+10s", 10), ("T+40s", 40), ("T+80s", 120), ("T+200s", 200)]:
        driver.queue.clear()  # drop the previous (un-run) retry dispatch — still "hung"
        fired = engine.fire_due_activity_timeouts(now=now + timedelta(seconds=advance))
        step = store.get_step(run_id, 0)
        print(f"  scan at {label}: timed_out={fired}  attempt={step.attempt}  status={step.status}")

    driver.run_until_idle()  # the failure re-ticked the run; let it replay
    run = store.get_run(run_id)
    print(f"\nrun -> {run.status} ({run.error['type']}: {run.error['message']})")


if __name__ == "__main__":
    main()
