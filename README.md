# Duratiq

**Durable workflows for [Dramatiq](https://dramatiq.io/).** "Temporal, but for
Dramatiq" — durable execution that runs on the stack you already have (Dramatiq
actors, your broker, Postgres), with no separate orchestration cluster.

> This is the **W1–W3 (partial)** slice from [`DURATIQ_MVP_PLAN.md`](../aizap/DURATIQ_MVP_PLAN.md):
> activities, replay, memoization, crash recovery, and durable timers
> (`ctx.sleep`). Signals, `gather`, retries-policy wiring, and a stale-lease
> recovery scanner are the next milestones.

## The idea

- **Workflows** are deterministic orchestrator functions. They touch the outside
  world only through `ctx`, so they can be replayed reproducibly.
- **Activities** are ordinary functions dispatched as Dramatiq messages.
- State lives in Postgres (`workflow_runs` + `workflow_steps`). Each tick replays
  the workflow from the top; completed steps return their **memoized** result, and
  the first not-ready point **suspends** the run. When an activity completes the
  run is re-ticked and advances. A crash just means the run is re-ticked later —
  it resumes exactly where it left off.

## Quick look

```python
from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver

reg = Registry()

@activity(name="charge_card", registry=reg)
def charge_card(order_id, amount):
    return f"pay_{order_id}"

@workflow(name="checkout", registry=reg)
def checkout(ctx, order_id):
    payment_id = ctx.activity(charge_card, order_id, 1999)
    return {"order_id": order_id, "payment_id": payment_id}

store = SqlStore(); store.create_all()
engine = Engine(reg, store)
LocalDriver(engine)                      # synchronous, no broker
run_id = engine.start("checkout", order_id="A123")
engine.driver.run_until_idle()
print(engine.get(run_id).status)          # COMPLETED
```

Run the example and the tests:

```bash
cd duratiq
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
python -m examples.checkout
pytest -q
```

## Drivers

- **`LocalDriver`** — synchronous, in-process, explicitly pumped. For dev,
  examples, and tests (including simulating crashes).
- **`DramatiqDriver`** — maps ticks and activity dispatches onto two Dramatiq
  actors. Single-process form here; see the module docstring for the multi-worker
  shape.

## Production note

`SqlStore.locked_run` serialises ticks per run. On **Postgres** it uses a
transaction-scoped advisory lock (`pg_advisory_xact_lock`) — the real guarantee.
On **SQLite** it uses an in-process lock, which is single-process dev/test only.

## Durable timers

`ctx.sleep(duration)` parks a run until a deadline, then resumes it — durably:

```python
@workflow(name="reminder", registry=reg)
def reminder(ctx, order_id):
    ctx.sleep("PT10M")          # seconds (a number) or ISO-8601 ("PT10M", "P1DT6H")
    return ctx.activity(send_followup, order_id)
```

The deadline is computed once and stored, so it survives replay and crashes. A
periodic **timer scanner** drives it — call `engine.fire_due_timers()` from cron
or `periodiq`; it delivers every elapsed timer and re-ticks the runs they unblock.
Tests pass `now=...` to fast-forward without sleeping.

## What's next (from the plan)

`ctx.wait_signal` + signal delivery, `ctx.gather` (parallel barrier), per-activity
retry policy wired to Dramatiq retries, and a recovery scanner for stale leases.
