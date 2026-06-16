# Duratiq

[![test](https://github.com/ivancrneto/duratiq/actions/workflows/test.yml/badge.svg)](https://github.com/ivancrneto/duratiq/actions/workflows/test.yml)
[![build](https://github.com/ivancrneto/duratiq/actions/workflows/build.yml/badge.svg)](https://github.com/ivancrneto/duratiq/actions/workflows/build.yml)
[![PyPI](https://img.shields.io/pypi/v/duratiq.svg)](https://pypi.org/project/duratiq/)
[![Python versions](https://img.shields.io/pypi/pyversions/duratiq.svg)](https://pypi.org/project/duratiq/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Durable workflows for [Dramatiq](https://dramatiq.io/).** "Temporal, but for
Dramatiq" — durable execution that runs on the stack you already have (Dramatiq
actors, your broker, Postgres), with no separate orchestration cluster.

> **The engine is feature-complete.** It runs activities with per-activity retries,
> replay and memoization, crash recovery, and a recovery scanner; durable timers
> (`ctx.sleep`), signals (`ctx.wait_signal`, with timeouts), side effects
> (`ctx.side_effect`), a parallel barrier (`ctx.gather`), and racing branches
> (`ctx.select`); child workflows (`ctx.child_workflow`, with cancellation cascade),
> `continue-as-new`, `ctx.patched` versioning, recurring cron schedules, and
> idempotent activities (`activity_info` / `run_once`); activity start-to-close
> timeouts and heartbeats; queries and updates; typed search attributes; a pluggable
> payload codec; OpenTelemetry tracing and a lifecycle-event listener; Alembic
> migrations; a packaged scanner runner; and a read/act admin UI (see [`admin/`](admin/)).
> See the [CHANGELOG](CHANGELOG.md) for the full 0.1.0 surface.

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

## Migrations

`SqlStore.create_all()` builds the whole schema in one call — fine for tests and a
fresh dev database. For a database you'll **evolve over time**, use the bundled
Alembic migrations instead, so schema changes are versioned and reviewable:

```bash
pip install "duratiq[migrations]"
export DURATIQ_DATABASE_URL=postgresql+psycopg://user:pass@host/db
alembic -c alembic.ini upgrade head
```

The migrations live in `src/duratiq/migrations` and ship with the package; the URL
comes from `DURATIQ_DATABASE_URL` (falling back to `sqlalchemy.url` in `alembic.ini`).
A test (`tests/test_migrations.py`) asserts `upgrade head` produces exactly the schema
`duratiq.models` describes via Alembic's `compare_metadata` — so a model change that
ships without a matching migration fails CI. To add one after changing the models:

```bash
alembic -c alembic.ini revision --autogenerate -m "describe the change"
```

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

## The scanner

Three things have to run on a cadence for a deployment to make progress on its own:
`fire_due_timers` (deliver elapsed `ctx.sleep` timers), `fire_due_schedules` (start
due cron runs), and `recover_stalled` (re-tick runs whose tick was lost to a crash).
`Scanner` drives all three from one loop, on independent intervals — no APScheduler
or periodiq dependency, just a blocking loop you run under whatever process manager
you already have:

```python
from duratiq import Scanner

Scanner(engine).run_forever()   # blocks until SIGINT/SIGTERM or .stop()
```

For a standalone process, point the bundled CLI at a `module:callable` that builds
your engine (store + driver) — the same wiring your workers use:

```bash
duratiq-scanner myapp.workers:make_engine        # or: python -m duratiq.scanner ...
duratiq-scanner myapp.workers:make_engine --timer-interval 1 --schedule-interval 60
```

Each scan has its own interval (timers want sub-second responsiveness; cron only
changes per minute; recovery is a slower backstop), and a scan that raises is logged
and retried next pass — one transient DB error never kills the loop. `run_once()` (or
`--once`) runs each scan a single time, for driving the scanner from cron instead.

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

**Wait with a timeout.** `ctx.wait_signal(name, timeout=...)` (seconds, or an
ISO-8601 string like `"PT24H"`) races the signal against a durable timer — the "wait
for approval **or** give up after a day" pattern:

```python
@workflow(name="approval", registry=reg)
def approval(ctx, order_id):
    decision = ctx.wait_signal("review", timeout="PT24H")
    if decision is TIMEOUT:           # the sentinel, imported from duratiq
        return auto_reject(order_id)
    return fulfil(order_id) if decision["approved"] else reject(order_id)
```

If the signal arrives first you get its payload; if the timer fires first you get
the `TIMEOUT` sentinel (a distinct object — not `None` — so a `None` payload stays
unambiguous; test with `is TIMEOUT`). Whichever loses is **cancelled** in the same
tick: the timer is dropped if the signal won, and the wait is dropped if it timed
out — so a signal that lands *after* the timeout isn't silently swallowed by the
abandoned wait but left queued for the next one. The decision is recorded durably, so
replay and crash recovery resolve to the same outcome.

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

## Queries

Signals are write-only; a **query** reads a running workflow's computed state without
advancing it. The workflow registers read-only handlers with `ctx.set_query_handler`,
and `engine.query(run_id, name)` calls one:

```python
@workflow(name="cart", registry=reg)
def cart(ctx):
    items = []
    ctx.set_query_handler("item_count", lambda: len(items))
    while True:
        items.append(ctx.wait_signal("add"))

engine.query(run_id, "item_count")   # -> however many adds have been processed
```

`query` replays the workflow **side-effect-free** — completed steps return their
memoized results and the replay stops at the frontier (or where the run ended), so
nothing is scheduled, committed, or dispatched — then invokes the handler, which is
usually a closure over the workflow's locals and so reflects every step processed so
far. Registering a handler consumes no `seq` and never suspends, so it's free to call
at the top of a workflow. Queries work on completed runs too (the handlers re-register
on the replay-to-completion). An unknown handler raises `QueryNotFound`.

## Updates

A query reads; an **update** *mutates* and returns a result — a synchronous, validated
request into a running workflow:

```python
@workflow(name="account", registry=reg)
def account(ctx):
    balance = [0]
    def deposit(amount):
        balance[0] += amount
        return balance[0]                       # returned to the caller
    def validate(amount):
        if amount <= 0:
            raise ValueError("must be positive")
    ctx.set_update_handler("deposit", deposit)
    ctx.set_update_validator("deposit", validate)
    while True:
        ctx.wait_update()                       # apply one update per loop, in arrival order

uid = engine.update(run_id, "deposit", 100)     # validated, queued, applied on the next tick
engine.get_update_result(run_id, uid)            # -> the handler's return value (100)
```

`engine.update` first runs the registered **validator** read-only — if it raises, the
update is rejected and nothing is recorded (*validate before mutate*). Otherwise it's
queued and the workflow consumes it at a `ctx.wait_update()` point, where the
registered handler runs: it mutates the workflow's state and returns a value. Like a
query handler it's re-run on every replay (so it must be deterministic — mutate state
and return, no I/O), and its result is recorded on a `workflow_updates` row for the
caller. The tick is asynchronous like everything else: `update` returns an id, and
`get_update_result` returns the value once applied (or `UPDATE_PENDING` until then,
or raises `UpdateFailed` if the handler raised). Updating a terminal run raises.

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

## Select (race the first to resolve)

Where `gather` waits for **all**, `ctx.select` waits for the **first** of several
branches and returns it — an activity result, a signal, a timer (a timeout), or a
child workflow. It generalises `wait_signal(timeout=...)` to any mix:

```python
idx, value = ctx.select(
    ctx.defer(charge, order_id),        # 0: the charge succeeds -> its result
    ctx.defer_child("manual_review"),   # 1: a review sub-workflow -> its result
    ctx.defer_signal("cancel"),         # 2: the customer cancels  -> the payload
    ctx.defer_timer("PT15M"),           # 3: the window expires    -> None
)
```

All branches arm together; the workflow suspends until one resolves, then `select`
returns `(index, value)` (a winning activity or child that *failed* re-raises). Ties
break by branch order, and the still-pending losers are **cancelled** — the timer
dropped, the signal-wait abandoned (so a late signal isn't swallowed), the activity
step CANCELLED (its result discarded if it lands later), and a child branch's **sub-run
cancelled** (cascading to its own children). That makes the decision **fixed across
replays**: a result that arrives after the race resolved can't flip the winner. Since a
cancelled activity's message may still run on a worker, branches in a `select` must be
safe to abandon.

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

## Activity timeouts

Retries only fire when an activity *raises*. An activity that **hangs** — or whose
message is lost without the broker redelivering — would otherwise leave the run
suspended forever. A `start_to_close_ms` puts a deadline on each attempt:

```python
@activity(name="call_api", registry=reg, max_retries=3, start_to_close_ms=30_000)
def call_api(order_id):
    ...
```

When the activity is dispatched a deadline is stored on its step. The
**activity-timeout scanner** — `engine.fire_due_activity_timeouts()`, run by the
`Scanner` alongside the timer scan — finds activities that blew their deadline
without reporting back and **re-dispatches a fresh attempt** (with a new deadline)
while the retry budget lasts, then records the step FAILED so the workflow sees
`ActivityFailed`. The deadline is claimed under the run lock and re-checked, so a
result that lands in the same moment wins the race. Like any retry this can re-run a
still-running activity, so the **idempotency** rule above still applies. Activities
without a `start_to_close_ms` have no deadline (the previous behaviour, unchanged).

## Heartbeats

A `start_to_close_ms` is a fixed cap — too tight for an activity whose duration varies
(reindexing N rows, draining a queue). A **heartbeat timeout** instead bounds the time
*between* progress reports: declare `heartbeat_timeout_ms` and call `heartbeat()` from
inside the activity. Each beat pushes the deadline out, so an activity that keeps
beating runs as long as it needs, while one that goes silent is timed out and retried.

```python
from duratiq import activity, heartbeat, heartbeat_details

@activity(name="reindex", registry=reg, heartbeat_timeout_ms=60_000, max_retries=3)
def reindex(total):
    start = heartbeat_details() or 0      # resume where the last attempt left off
    for i in range(start, total):
        ...                               # a chunk of work
        heartbeat(i + 1)                  # report progress + stay alive
    return "done"
```

`heartbeat(details)` records the latest **progress** (any JSON value) and resets the
deadline; on a timeout the progress survives onto the retried attempt, so
`heartbeat_details()` lets it **resume instead of restarting**. It reuses the same
activity-timeout scanner — a missed heartbeat is just a timed-out attempt. A beat after
the step has finished is ignored (it can't revive a step the scanner already failed).

## Idempotent activities

Activities are **at-least-once** — a retry, a broker redelivery, or a crash can run
one more than once — so they must be idempotent. Two runtime helpers (importable
inside any activity body) make that practical:

```python
from duratiq import activity, activity_info, run_once

@activity(name="charge", registry=reg)
def charge(order_id):
    info = activity_info()                       # stable id for THIS invocation
    return run_once(
        info.idempotency_key,                    # f"{run_id}:{seq}" — same across retries
        lambda: stripe.charge(order_id, idempotency_key=info.idempotency_key),
    )
```

- **`activity_info()`** returns the current activity's `run_id`, `seq`, and a stable
  `idempotency_key` (`run_id:seq`) that doesn't change across retries, redelivery, or
  replay — pass it to an idempotent external API for true end-to-end exactly-once.
- **`run_once(key, fn)`** records `fn`'s result in a dedup table the first time and
  returns the stored value on every later call with the same key. So if an activity
  charges a card and then a *later* step in the same activity fails, the retry skips
  the charge and reuses the recorded result.

`run_once` dedupes re-execution within Duratiq's control (retries, sequential
redelivery). As with Temporal, a crash *between* the external effect landing and the
dedup row committing can still re-run it — which is exactly why the
`idempotency_key` exists: hand it to the downstream system for the hard guarantee.

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
re-suspends.

Lost *activity* messages are normally recovered by the broker's own redelivery, or —
for activities with a `start_to_close_ms`/`heartbeat_timeout_ms` — by the
activity-timeout scanner. The one case neither covers is an **untimed** activity
whose dispatch was lost in the gap between committing the step and enqueuing the
message (the broker has nothing to redeliver, and there's no deadline). Pass
`recover_stalled(redispatch_orphaned_activities=True)` to also re-dispatch those for
stale runs — making recovery self-sufficient at the cost of possibly re-dispatching a
slow-but-in-flight untimed activity (idempotency covers it; give long activities a
`start_to_close_ms` instead).

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

## OpenTelemetry tracing

`duratiq.otel.instrument` turns those events into OpenTelemetry spans — one line, no
change to workflow code:

```python
from duratiq.otel import instrument

instrument(engine)        # spans now flow to your configured OTLP backend
```

The key trick is **cross-process trace propagation with nothing stored**: every span
for a run is placed in one trace whose id is *derived from the durable `run_id`* (a
uuid4 hex is already a 128-bit W3C trace-id). The engine tick, an activity running in
another worker, and a re-tick after a crash all compute the same trace-id from the
run_id — so their spans land in one trace without threading any headers through
messages. Spans carry `duratiq.run_id`, the workflow/activity name, `seq`/`attempt`,
and an ERROR status with the error on failures. An existing `listener` is chained,
not replaced. Use `run_trace_context(run_id)` to parent your own spans (an HTTP
handler, an activity body) onto the same trace. Install with `pip install
"duratiq[otel]"`.

## Listing runs

Alongside `engine.get(run_id)`, `engine.list_runs` enumerates runs for an ops/admin
view — filter by status and/or workflow name, newest first, paginated:

```python
engine.list_runs()                                  # newest 50 runs
engine.list_runs(status="FAILED", limit=20)         # most recent failures
engine.list_runs(status=["RUNNING", "SUSPENDED"])   # everything in flight
engine.list_runs(name="checkout", offset=50, limit=50)  # page 2 of one workflow

engine.count_runs(status="FAILED")                  # total behind the page
```

`status` takes a single status or a list; `limit` is clamped to `[1, 1000]`. Pair
`list_runs` with `count_runs` (same filters, no paging) to drive pagination.

## Search attributes

Filtering by status and name only goes so far. **Search attributes** are typed,
indexed metadata you attach to a run — `region`, `customer`, `priority` — then filter
on, for an ops view like "FAILED high-priority EU orders":

```python
engine.start("order", order_id="A1", search_attributes={"region": "eu", "priority": 1})

# or from inside the workflow, set/update them as state changes:
def order(ctx, ...):
    ctx.upsert_search_attributes({"region": "eu", "stage": "shipped"})

engine.list_runs(status="FAILED", search_attributes={"region": "eu", "priority": 1})
engine.count_runs(search_attributes={"region": "eu"})
engine.get_search_attributes(run_id)        # -> {"region": "eu", "priority": 1}
```

Each `search_attributes` filter is an **equality** match and they **AND** together; a
value matches by type (`priority=1` ≠ `priority="1"`). Attributes are stored one
indexed row per `(run, key)` in `workflow_search_attributes`, so filtering is a real
indexed query, not a scan — and `upsert_search_attributes` replaces a key in place
(re-applied idempotently on every replay).

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
