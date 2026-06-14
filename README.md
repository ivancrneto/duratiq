# Duratiq

**Durable workflows for [Dramatiq](https://dramatiq.io/).** "Temporal, but for
Dramatiq" — durable execution that runs on the stack you already have (Dramatiq
actors, your broker, Postgres), with no separate orchestration cluster.

> This is the **W1–W4** slice from [`DURATIQ_MVP_PLAN.md`](../aizap/DURATIQ_MVP_PLAN.md)
> — the core MVP engine: activities with per-activity retries, replay, memoization,
> crash recovery, durable timers (`ctx.sleep`), signals (`ctx.wait_signal`), side
> effects (`ctx.side_effect`), a parallel barrier (`ctx.gather`), child workflows
> (`ctx.child_workflow`), and a recovery scanner for stalled runs, plus
> `continue-as-new` (`ctx.continue_as_new`) for long-running loops, `ctx.patched`
> versioning for safely evolving deployed workflow code, and recurring cron schedules
> (`engine.create_schedule`).

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

## Recurring schedules

Start a workflow on a cron cadence. `engine.create_schedule` registers it; a
periodic **schedule scanner** — `engine.fire_due_schedules()`, called from
cron/`periodiq` alongside `fire_due_timers` — starts a run each time the schedule
comes due:

```python
# 9am every weekday
sid = engine.create_schedule("daily_report", "0 9 * * 1-5", region="eu")

# in your once-a-minute scanner:
engine.fire_due_schedules()        # starts due runs, advances each to its next cron time
```

The cron parser supports the standard 5 fields (`* */n a-b a,b,c`, day-of-week
`0`/`7` = Sunday, and the Vixie rule that a restricted day-of-month **or**
day-of-week matches). Each due schedule is *claimed* — its next fire time advanced —
before its run starts, so concurrent scanners don't double-fire and a missed tick is
skipped rather than backfilled. Pass `schedule_id=` to make registration idempotent;
`pause_schedule` / `resume_schedule` / `delete_schedule` manage the lifecycle. Tests
pass `now=...` to fast-forward without waiting on the clock.

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

**Signal-with-start.** `engine.signal_with_start(name, signal=..., payload=...,
idempotency_key=...)` delivers a signal to a run, starting it first if it doesn't
exist yet. Dedupe on `idempotency_key`: the first call starts the workflow, every
later call just signals the running one. It's the right primitive for "ensure a
per-entity workflow is running, then nudge it" — e.g. a per-customer cart workflow
you signal on every add-to-cart, starting it on the first:

```python
# first add-to-cart starts the cart workflow and delivers the item;
# every later one signals the same run (same idempotency_key -> same run id).
run_id = engine.signal_with_start(
    "cart", signal="add_item", payload={"sku": "A1"}, idempotency_key=f"cart:{customer_id}",
)
```

The signal is queued before the first tick, so the run's `ctx.wait_signal` finds it
already waiting — no race against the start.

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

## Continue-as-new

Each tick replays from the top, so a workflow that loops forever — an event loop
draining a queue, a long poll — accumulates step history without bound.
`ctx.continue_as_new(**kwargs)` ends the current iteration and restarts the run
*as if freshly started* with new input and an **empty history**, keeping the same
run id:

```python
@workflow(name="poller", registry=reg)
def poller(ctx, cursor, processed):
    batch = ctx.activity(fetch_since, cursor)
    for item in batch:
        ctx.activity(handle, item)
    ctx.sleep("PT1M")
    # Restart with a fresh history instead of growing it forever.
    ctx.continue_as_new(cursor=batch.next_cursor, processed=processed + len(batch))
```

Reaching the call means every prior `ctx` step in this iteration already completed,
so there is nothing in flight to lose. The engine truncates the run's steps, fired
timers, and consumed signals, then re-ticks it from seq 0 with the carried input.
**Signals that haven't been consumed yet carry over** to the next iteration, so a
queue-draining loop never drops a queued event across the restart. Like the other
control-flow points, it survives a crash: the reset commits atomically with the
tick, so recovery just resumes the new iteration.

## Child workflows

`ctx.child_workflow` runs another workflow as a sub-run and returns its result —
durable composition. The child is a full workflow in its own right (it can run
activities, sleep, and wait on signals); the parent suspends while it runs and
resumes with the result once it completes:

```python
@workflow(name="process_order", registry=reg)
def process_order(ctx, order_id):
    shipment = ctx.child_workflow("ship_order", order_id=order_id)   # or pass the @workflow function
    return {"order_id": order_id, "shipment": shipment}
```

The child run links back to the parent's step (`parent_run_id` / `parent_seq`);
when it reaches a terminal state the engine resolves that step and re-ticks the
parent, so the result is memoized and survives replay. A child that fails — or is
cancelled — raises `ChildWorkflowFailed` in the parent, where it can be caught or
left to fail the parent (just like a failed `ctx.activity`). Starting a child is
idempotent on `(parent_run_id, parent_seq)`, so a crash between committing the step
and starting the sub-run is recovered without spawning a duplicate. Cancelling a
parent **cascades**: its still-running children (and theirs) are cancelled too;
cancelling a child directly instead fails the parent's `child_workflow` so it
doesn't wait forever.

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

## Versioning with patches

Because replay matches recorded history by position, changing a deployed workflow's
code can diverge its in-flight runs (`DeterminismError`). `ctx.patched` is the safe
way to ship a change: wrap the new behaviour, leave the old in the `else`.

```python
@workflow(name="checkout", registry=reg)
def checkout(ctx, order_id):
    payment = ctx.activity(charge_card, order_id)
    if ctx.patched("send-receipt-v2"):
        ctx.activity(send_receipt_v2, order_id)   # new runs take this
    else:
        ctx.activity(send_receipt, order_id)      # runs that predate the patch keep this
    return payment
```

The decision is fixed per call site and replayed stably. A **new run** records a
patch marker and returns `True`; a run that **already executed past this point**
under the old code has a real command where the marker would sit, so `patched`
returns `False` and — without consuming a position — lets the old branch realign
with history. Once every pre-patch run has drained you can delete the old branch;
removing the `patched` call entirely is safe only after that.

## Recovery

A tick is atomic under a per-run advisory lock, so a worker that dies mid-tick
rolls back cleanly. The residual risk is a *lost tick*: a worker commits a step (a
matched signal, a fired timer) and dies before its follow-up re-tick runs, leaving
the run parked with nobody to advance it. `engine.recover_stalled()` is the
backstop — call it periodically (cron/`periodiq`); it re-ticks non-terminal runs
idle past a threshold. Replay is idempotent, so a genuinely-waiting run just
re-suspends. (Lost *activity* messages are recovered by the broker's own
redelivery.)

## Observability

Pass a `listener` to the `Engine` to receive lifecycle events as runs and activities
change state — the seam for metrics, structured logs, and tracing, with no
dependency baked in:

```python
from duratiq import Engine, WorkflowEvent

def on_event(e: WorkflowEvent):
    log.info("duratiq", type=e.type, run_id=e.run_id, name=e.name)
    # or: increment a Prometheus counter, open an OpenTelemetry span, ...

engine = Engine(reg, store, listener=on_event)
```

Events: `run.started`, `run.suspended`, `run.completed` (carries `result`),
`run.failed` / `activity.failed` (carry `error`), `run.cancelled`, and
`activity.scheduled` / `activity.completed` (carry `seq`, `attempt`). They're emitted
**after** the state they describe is committed, and a listener that raises is
swallowed — observability never breaks a run.

## Payload codec

Every workflow input, result, step payload, and signal is memoized as JSON in
Postgres. Large payloads bloat that history, and some shouldn't live in the database
at all. A **payload codec** is the seam to intervene — compress them, or offload big
blobs to S3 and store only a reference — applied transparently at the storage layer,
so neither the engine nor workflow code changes:

```python
from duratiq import PayloadCodec, set_payload_codec

class S3OffloadingCodec:
    def encode(self, value):                 # on the way into the DB
        blob = json.dumps(value).encode()
        if len(blob) < 8_000:
            return value                      # small: store inline
        key = s3_put(blob)
        return {"__s3__": key}                # large: store a reference
    def decode(self, value):                  # on the way back out
        if isinstance(value, dict) and "__s3__" in value:
            return json.loads(s3_get(value["__s3__"]))
        return value

set_payload_codec(S3OffloadingCodec())        # once, at startup
```

The codec must round-trip (`decode(encode(v)) == v`) and `encode` must return
something JSON-serialisable. The default is a pass-through `IdentityCodec`, so
nothing changes until you install one. It's process-global — set it once before
starting the engine.

## What's next (from the plan)

Cross-process trace-context propagation (OpenTelemetry) builds on the listener hook
above.
