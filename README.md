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
> (`ctx.side_effect`), deterministic time (`ctx.now`) and run metadata (`ctx.info`),
> a parallel barrier (`ctx.gather`), and racing branches (`ctx.select`); child
> workflows (`ctx.child_workflow`, with cancellation cascade), cancellation scopes
> (`ctx.cancellation_scope` + `ctx.set_signal_handler`), local activities
> (`ctx.local_activity`), `continue-as-new`, `ctx.patched` versioning, recurring cron
> schedules (with overlap policies), dynamic catch-all handlers, and idempotent
> activities (`activity_info` / `run_once`); activity start-to-close, schedule-to-start,
> and schedule-to-close timeouts and heartbeats; workflow-level execution/run timeouts;
> queries and updates; typed search attributes and immutable memo; workflow IDs with
> reuse policies; `terminate`, batch cancel/terminate, and `reset_to_step`; a pluggable
> payload codec; OpenTelemetry tracing and a lifecycle-event listener; Alembic
> migrations; a packaged scanner runner; and a read/act admin UI (see [`admin/`](admin/)).
> See the [CHANGELOG](CHANGELOG.md) for the full surface.

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

**Overlap policy.** When a schedule comes due while its previous run is still in
flight, `create_schedule(..., overlap_policy=...)` decides what happens:

```python
engine.create_schedule("nightly_sync", "0 2 * * *", overlap_policy="SKIP")
```

`ALLOW` (the default) always starts the new run; `SKIP` leaves the running one alone
and skips this fire; `REPLACE` cancels the previous run first; `TERMINATE` terminates
it first (terminal `FAILED` / `WorkflowTerminated`). The policy only matters while the
last run is non-terminal — once it's done, every policy just starts the next run.

## The scanner

Several things have to run on a cadence for a deployment to make progress on its own:
`fire_due_timers` (deliver elapsed `ctx.sleep` timers), `fire_due_schedules` (start
due cron runs), `fire_due_activity_timeouts` and `fire_due_schedule_to_start_timeouts`
(fail/retry activities past their deadlines), `fire_due_execution_timeouts` and
`fire_due_run_timeouts` (fail overrunning workflows), and `recover_stalled` (re-tick
runs whose tick was lost to a crash). `Scanner` drives them all from one loop, on
independent intervals — no APScheduler or periodiq dependency, just a blocking loop you
run under whatever process manager you already have:

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
changes per minute; the timeout and recovery scans are slower backstops) — tune them
with `--timer-interval`, `--schedule-interval`, `--activity-timeout-interval`,
`--workflow-timeout-interval`, and `--recovery-interval`. A scan that raises is logged
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

## Deterministic time and run info

`ctx.now()` is the determinism-safe `datetime.utcnow()` — it records the wall clock
the first time the workflow reaches it and returns that same instant on every later
replay, so a timestamp written into a result or compared against a deadline stays
stable across re-ticks and crashes:

```python
@workflow(name="sla", registry=reg)
def sla(ctx, order_id):
    started = ctx.now()                      # fixed at first execution, stable forever
    result = ctx.activity(process, order_id)
    return {"order_id": order_id, "took_ms": (ctx.now() - started)}
```

It's a thin wrapper over `side_effect`, so like it the value is available immediately
without suspending. `ctx.info()` returns a frozen `WorkflowInfo` snapshot of the
current run's metadata — `run_id`, `name`, `version`, `parent_run_id`, `attempt`
(the retry/reset counter), and `memo` — read from the run row already loaded for the
tick, so there's no DB round-trip during replay:

```python
@workflow(name="report", registry=reg)
def report(ctx):
    info = ctx.info()
    if info.attempt > 1:
        ...                                  # behave differently on a retried run
```

## Local activities

`ctx.local_activity(fn, *args)` runs a function **inline in the tick process** —
no Dramatiq dispatch, no broker round-trip — and memoizes the result like a regular
activity. It's the right tool for short, cheap work where a full message round-trip
would dominate the latency (a quick lookup, a pure transform):

```python
@workflow(name="enrich", registry=reg)
def enrich(ctx, order_id):
    raw = ctx.activity(fetch_order, order_id)     # broker round-trip — the slow call
    normalized = ctx.local_activity(normalize, raw)   # inline — no dispatch
    return normalized
```

The function executes synchronously inside the tick transaction and is recorded as a
`LOCAL_ACTIVITY` step, so on the re-tick it's already COMPLETED in history and the
stored value is returned without re-running. Failures retry in-process up to
`max_retries` (default `0`); once exhausted the step is FAILED and the workflow sees
`ActivityFailed`, exactly like a dispatched activity. Because it runs in the tick
process under the run lock, a local activity must be **fast and side-effect-light** —
anything slow, blocking, or that needs the broker's at-least-once redelivery belongs
in a regular `ctx.activity`.

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

## Cancellation scopes

`ctx.select` races branches you arm up front. A **cancellation scope** is the other
shape: run a block normally, but let an out-of-band event — usually a signal —
abandon whatever it's waiting on and fall through to cleanup. `ctx.set_signal_handler`
registers a *background* handler (unlike `wait_signal`, it doesn't suspend — the
workflow keeps going), and `ctx.cancellation_scope()` gives you a `with` block whose
`cancel()` unwinds it at the next `ctx.*` suspension point:

```python
@workflow(name="watch", registry=reg)
def watch(ctx, job_id):
    with ctx.cancellation_scope() as scope:
        ctx.set_signal_handler("abort", lambda _: scope.cancel())
        ctx.activity(long_running_step, job_id)   # abandoned if "abort" arrives
        ctx.sleep("PT1H")                          # ...or here
    return "aborted or finished"                   # execution always continues here
```

When the `abort` signal is delivered the handler runs on that tick and calls
`scope.cancel()`; the next awaiting `ctx.*` call inside the block raises an internal
`_ScopeCancelled`, which the scope suppresses at `__exit__` so control resumes after
the `with`. The decision is driven entirely by recorded history (signal delivery order
is fixed), so it replays deterministically. Handlers are also useful on their own — a
non-blocking `set_signal_handler` that just updates a flag or appends to a list lets a
workflow react to signals without parking at a `wait_signal`.

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

## Dynamic workflows and activities

Normally every workflow and activity name is registered up front. A **dynamic
handler** is a single catch-all that serves any name *not* explicitly registered —
useful when names are data (a plugin system, a per-tenant workflow, a generic
dispatcher):

```python
@workflow.dynamic(registry=reg)
def any_workflow(ctx):
    name = ctx.info().name        # the actual requested name
    ...

@activity.dynamic(registry=reg)
def any_activity():
    ...

engine.start("whatever_name")     # routed to the dynamic workflow
```

Registry lookup tries the exact name first and only falls through to the dynamic
handler when there's no match — so an explicit registration always shadows the
catch-all. Without a dynamic handler registered, an unknown workflow still raises
`WorkflowNotFound` and an unknown activity `KeyError`, exactly as before.

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

## Schedule-to-start and schedule-to-close timeouts

`start_to_close_ms` bounds a single attempt. Two further deadlines bound an activity's
place in the queue and its total budget:

```python
@activity(name="dispatch", registry=reg, max_retries=5,
          schedule_to_start_timeout_ms=30_000,    # must be picked up within 30s
          schedule_to_close_timeout_ms=300_000)   # whole thing done within 5m
def dispatch(order_id):
    ...
```

`schedule_to_start_timeout_ms` is the **queue-wait** ceiling: if the activity is still
SCHEDULED — never dequeued by a worker — past the deadline, the **schedule-to-start
scanner** (`engine.fire_due_schedule_to_start_timeouts()`, run by `Scanner`) fails the
step with a `ScheduleToStartTimeout`, no retry (a backed-up queue won't clear by
retrying). `schedule_to_close_timeout_ms` is the **total budget** across every retry:
once it's blown, the next timed-out attempt fails the step immediately rather than
re-dispatching, regardless of the remaining retry count. Both deadlines are stored on
the step when it's scheduled, so they survive replay and crashes.

## Workflow-level timeouts

Activity timeouts bound one step; **workflow timeouts** bound a whole run. Declare them
on the workflow or at `start`:

```python
@workflow(name="pipeline", registry=reg, execution_timeout=3600, run_timeout=600)
def pipeline(ctx):
    ...

engine.start("pipeline", execution_timeout=3600, run_timeout=600)   # seconds
```

`run_timeout` caps a **single run** and resets on `continue_as_new`; `execution_timeout`
caps the **entire chain** and carries across every `continue_as_new` iteration — so a
poll loop can give each iteration its own `run_timeout` while a single
`execution_timeout` bounds the whole thing. Each is stored as a deadline on the run and
enforced by a scanner — `engine.fire_due_execution_timeouts()` and
`engine.fire_due_run_timeouts()`, both driven by `Scanner` — which fail an overrunning
run with an `ExecutionTimeout` / `RunTimeout` error. As elsewhere, tests pass `now=...`
to fast-forward.

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

## Terminating, batch operations, and reset

`engine.cancel(run_id)` ends a run gracefully — terminal status `CANCELLED`, cascading
to its children. `engine.terminate(run_id, reason=...)` is the **hard** counterpart:
terminal status `FAILED` with a `WorkflowTerminated` error, also cascading. Use cancel
for an orderly stop, terminate to kill a run you consider broken:

```python
engine.cancel(run_id)                          # CANCELLED
engine.terminate(run_id, reason="bad deploy")  # FAILED / WorkflowTerminated
```

Both have **batch** forms that apply to every run matching a filter (the same
status / name / search-attribute filters as `list_runs`), returning the count
affected — for an ops "stop everything matching this" action:

```python
engine.batch_cancel(name="checkout", search_attributes={"region": "eu"})
engine.batch_terminate(status="SUSPENDED", reason="draining", limit=500)
```

`engine.reset_to_step(run_id, seq)` is the recovery tool: on a **FAILED** run it
deletes every step after `seq` (and their timers), clears the error, and re-ticks from
that checkpoint — so you can fix a bug and replay an in-flight run from before the
break, rather than from scratch. (`retry` re-runs only the failed steps;
`reset_to_step` rewinds to a chosen point.) `engine.update_with_start(...)` atomically
starts a run and enqueues an update before the first tick can race it — the
"ensure-running-then-mutate" primitive when you need the update applied on the very
first tick.

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

## Memo

Search attributes are indexed and mutable. **Memo** is the opposite: immutable,
unindexed metadata you attach once at `start` and read back from outside — a place to
stash context that travels with the run but never needs filtering on (the originating
request id, the user who kicked it off, a free-form note):

```python
engine.start("order", order_id="A1", memo={"requested_by": "ops@co", "ticket": "OPS-42"})

engine.get_memo(run_id)        # -> {"requested_by": "ops@co", "ticket": "OPS-42"}
```

It's also visible inside the workflow as `ctx.info().memo`. Because it isn't indexed
it costs nothing to attach and can't be filtered on — reach for search attributes when
you need to query, memo when you just need to carry.

## Workflow IDs and reuse policy

Every run has an internal UUID. A **workflow ID** is a *business* identifier you choose
— an order number, a customer id — with a policy governing what happens when you start
another run with the same ID:

```python
engine.start("order", workflow_id="order-A1",
             workflow_id_reuse_policy="REJECT_DUPLICATE")
```

- `ALLOW_DUPLICATE` (default) — always start a new run.
- `REJECT_DUPLICATE` — raise if *any* run already exists for that ID.
- `ALLOW_DUPLICATE_FAILED_ONLY` — start only if the most-recent run for that ID is
  FAILED (raise otherwise), so a finished-or-running job isn't re-run.
- `TERMINATE_IF_RUNNING` — terminate the most-recent non-terminal run, then start.

`store.find_runs_by_workflow_id(workflow_id)` returns every run for an ID, newest
first. Unlike `idempotency_key` (which dedupes a start into a single run), the
workflow ID groups a *series* of runs under one business key and the policy decides
whether a new one may join it.

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

## What's next

The engine now covers the Temporal Python SDK's surface across engine operations,
run metadata, and execution primitives. The remaining gap is reach, not features:
the [`admin/`](admin/) UI exposes cancel / retry / send-signal but not yet
`terminate` or memo display. See the [CHANGELOG](CHANGELOG.md) for the full surface.
