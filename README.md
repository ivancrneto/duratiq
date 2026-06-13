# Duratiq

**Durable workflows for [Dramatiq](https://dramatiq.io/).** "Temporal, but for
Dramatiq" — durable execution that runs on the stack you already have (Dramatiq
actors, your broker, Postgres), with no separate orchestration cluster.

> This is the **W1–W4** slice from [`DURATIQ_MVP_PLAN.md`](../aizap/DURATIQ_MVP_PLAN.md)
> — the core MVP engine: activities with per-activity retries, replay, memoization,
> crash recovery, durable timers (`ctx.sleep`), signals (`ctx.wait_signal`), side
> effects (`ctx.side_effect`), a parallel barrier (`ctx.gather`), and a recovery
> scanner for stalled runs. Fast-follow items (child workflows, `continue-as-new`,
> `ctx.patched` versioning) remain.

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

## Signals

`ctx.wait_signal(name)` parks a run until an outside actor delivers a matching
signal — a human approval, a webhook, another service:

```python
@workflow(name="review_order", registry=reg)
def review_order(ctx, order_id):
    decision = ctx.wait_signal("review")     # suspends, holding no worker
    if decision["approved"]:
        return ctx.activity(fulfil_order, order_id)
    return ctx.activity(reject_order, order_id)

# elsewhere — minutes or days later:
engine.signal(run_id, "review", {"approved": True})
```

Signals are stored in `workflow_signals` independently of the waits that consume
them, so one that arrives *before* its wait is queued and matched FIFO by name —
no race. The consumed payload is memoized, so replay returns it without re-waiting.

## Side effects

Workflow code must be deterministic, so it can't call `now()`, `uuid4()`, or
`random()` directly — replay would produce a different value. `ctx.side_effect`
runs such a function **once** and records the result; every later replay returns
the stored value:

```python
@workflow(name="with_id", registry=reg)
def with_id(ctx):
    request_id = ctx.side_effect(lambda: uuid4().hex)   # generated once, stable forever
    return ctx.activity(charge, request_id)
```

Unlike the awaiting calls, `side_effect` doesn't suspend — the value is available
immediately and recorded atomically with the rest of the tick. The result must be
JSON-serialisable.

## Parallel fan-out

`ctx.gather` runs independent activities at once and waits for all of them. Build
each branch with `ctx.defer` (which captures the call without starting it), then
hand them to `gather`:

```python
@workflow(name="fulfil", registry=reg)
def fulfil(ctx, order_id):
    receipt, reservation = ctx.gather(
        ctx.defer(make_receipt, order_id),
        ctx.defer(reserve_inventory, order_id),
    )
    return {"receipt": receipt, "reservation": reservation}
```

All branches are dispatched in a single tick, so they run concurrently; the
workflow resumes only once every branch has completed, and results come back in
call order. If a branch fails, `gather` fails fast with that error. (A plain
`ctx.activity` can't be nested in `gather` — it would suspend on the first call;
that's why `defer` exists.)

## Retries

A failing activity is retried before it sinks the workflow. `@activity` carries the
policy:

```python
@activity(name="charge", registry=reg, max_retries=5, min_backoff_ms=200, max_backoff_ms=30_000)
def charge(order_id):
    ...
```

It runs at most `max_retries + 1` times; only once the budget is exhausted is the
step recorded FAILED and the error raised into the workflow (where it can be caught
or fails the run). The `DramatiqDriver` delegates to Dramatiq's own Retries
middleware — re-raising on a retryable attempt so the broker re-enqueues it with
exponential backoff, and recording FAILED only on the final attempt (no
dead-lettering). The `LocalDriver` retries inline without backoff. Because retries
(and crash redelivery) can re-run an activity, **activities must be idempotent**.

## Recovery

A tick is atomic under a per-run advisory lock, so a worker that dies mid-tick
rolls back cleanly. The residual risk is a *lost tick*: a worker commits a step (a
matched signal, a fired timer) and dies before its follow-up re-tick runs, leaving
the run parked with nobody to advance it. `engine.recover_stalled()` is the
backstop — call it periodically (cron/`periodiq`); it re-ticks non-terminal runs
idle past a threshold. Replay is idempotent, so a genuinely-waiting run just
re-suspends. (Lost *activity* messages are recovered by the broker's own
redelivery.)

## What's next (from the plan)

`ctx.gather` (parallel barrier) and per-activity retry policy wired to Dramatiq
retries.
