# Duratiq — Durable Workflows for Dramatiq (MVP Plan)

> "Temporal, but for Dramatiq." A durable-execution layer where **workflows are
> deterministic orchestrator functions** and **activities are ordinary Dramatiq
> actors**, with state persisted in Postgres and transport over your existing
> broker (RabbitMQ).

---

## 1. Landscape / Why build it

The capability (durable execution) is well-served in 2026 — Temporal, DBOS,
Hatchet, Restate, Inngest. But none of them *are* a durable layer on top of
**Dramatiq + your existing RabbitMQ + Postgres**:

- **[dramatiq-workflow](https://github.com/Outset-AI/dramatiq-workflow)** — closest
  existing thing. Chains/groups only. Per its own README: no result-passing, **no
  replay/recovery after crash**, no state queries, no UI, no durable store. It's a
  DAG scheduler, not a durable engine.
- **[DBOS Transact](https://github.com/dbos-inc/dbos-transact-py)** — the right
  *philosophy* (lightweight Postgres-backed library, resume-where-you-left-off),
  but ships its own queues/workers; not your Dramatiq fleet.
- **Temporal / Hatchet** — separate clusters/servers to operate.

**Build-vs-adopt honesty:** if you're willing to run separate infra, adopt
Temporal or Hatchet. If you want durable execution *inside the stack you already
run* (Dramatiq actors, RabbitMQ, Postgres, FastAPI/SQLAdmin) with no new daemon,
this MVP is the gap-filler. For aizap specifically (already Dramatiq + Postgres +
RabbitMQ), that's a strong fit.

---

## 2. Core idea (execution model)

Temporal's defining feature is **durable execution**: a function that resumes
exactly where it left off after a crash. Two ways to build it:

- **(A) Event-sourced command log + deterministic replay** — true Temporal. Most
  faithful, most code, hardest to get right (sandboxing, command/event matching).
- **(B) Checkpointed replay-from-top with DB-memoized steps** — DBOS-style. Each
  workflow "tick" re-runs the orchestrator from the top; completed steps return
  their **memoized** result from Postgres instead of re-executing. When the code
  reaches an async point that isn't ready (activity pending, timer not elapsed,
  signal not received), it **raises `Suspend`** and the worker is released. When
  the awaited thing completes, the run is re-enqueued and replays again.

**MVP picks (B).** It's far less code, maps cleanly onto Dramatiq (the workflow
*is* an actor), and still delivers the Temporal feel: durability, crash-resume,
durable timers, signals, retries. Same determinism constraints as Temporal apply
to workflow code.

```
start_workflow(name, input)
   └─ INSERT workflow_runs(RUNNING) ; enqueue _tick(run_id)

_tick(run_id):                          # a Dramatiq actor
   acquire per-run lock (pg advisory)   # never advance a run twice concurrently
   replay orchestrator(ctx):
       ctx.activity(actor,args) --> step recorded? return memoized result
                                    else dispatch Dramatiq actor, raise Suspend
       ctx.sleep(d)             --> timer fired? continue : schedule timer, Suspend
       ctx.wait_signal(name)    --> signal present? consume : Suspend
   outcomes:
       return value  --> COMPLETED, store result, resolve waiting parents
       raise Suspend --> SUSPENDED (release worker, no re-enqueue)
       raise Error   --> workflow retry policy -> RUNNING or FAILED

# Re-enqueue _tick(run_id) happens on:
#   - activity completion hook   - timer fire   - signal delivery   - recovery scan
```

---

## 3. Data model (Postgres / Alembic)

- **workflow_runs**: `id (uuid)`, `name`, `version`, `input (jsonb)`,
  `status` (PENDING/RUNNING/SUSPENDED/COMPLETED/FAILED/CANCELLED),
  `result (jsonb)`, `error (jsonb)`, `idempotency_key (unique nullable)`,
  `lease_owner`, `lease_expires_at`, `created_at`, `updated_at`.
- **workflow_steps** (the event history): `run_id`, `seq (int)` — deterministic
  command index, `kind` (ACTIVITY/TIMER/SIGNAL_WAIT/SIDE_EFFECT/GATHER),
  `name`, `input (jsonb)`, `status`, `result (jsonb)`, `error (jsonb)`,
  `attempt`, `scheduled_at`, `completed_at`. PK `(run_id, seq)`.
- **workflow_signals**: `id`, `run_id`, `name`, `payload (jsonb)`, `received_at`,
  `consumed_seq (nullable)`.
- **workflow_timers**: `id`, `run_id`, `seq`, `fire_at`, `fired_at (nullable)`.

Indexes: `workflow_runs(status, lease_expires_at)` for the recovery scan;
`workflow_timers(fired_at, fire_at)` for the timer scan;
`workflow_signals(run_id, consumed_seq)`.

---

## 4. Public API (developer surface)

```python
from duratiq import workflow, activity, WorkflowContext

@activity(max_retries=5, backoff="exponential", time_limit=30_000)
def charge_card(order_id: str, amount: int) -> str:        # a normal Dramatiq actor
    ...

@workflow(name="checkout", version=1)
def checkout(ctx: WorkflowContext, order_id: str):
    payment_id = ctx.activity(charge_card, order_id, 1999) # durable, retried
    ctx.sleep("PT10M")                                     # durable timer
    approved = ctx.wait_signal("manual_review", timeout="P1D")
    receipt, email = ctx.gather(                           # parallel barrier
        ctx.activity(make_receipt, order_id),
        ctx.activity(send_email, order_id),
    )
    return {"payment_id": payment_id, "receipt": receipt}

# Drive it:
run = duratiq.start("checkout", order_id="A123", idempotency_key="A123")
duratiq.signal(run.id, "manual_review", {"approved": True})
duratiq.get(run.id)            # status + history
duratiq.cancel(run.id)
```

`WorkflowContext` methods (all assigned a deterministic `seq`):
`activity()`, `gather()`, `sleep()`, `wait_signal()`, `side_effect(fn)` (records a
non-deterministic value once: `now`, `uuid`, `random`), `child_workflow()` (post-MVP),
`logger` (replay-aware, suppresses logs during replay of memoized steps).

---

## 5. Components to build

1. **Registry & decorators** — `@workflow`, `@activity`. `@activity` wraps a
   Dramatiq actor and attaches a completion-callback middleware.
2. **WorkflowContext / replay engine** — the seq counter, memoization lookups,
   `Suspend` signalling, side-effect recording, determinism guard.
3. **`_tick` actor** — load run+history, run orchestrator under the context,
   commit outcome. Holds a **per-run Postgres advisory lock** for the whole tick.
4. **Activity completion hook** — Dramatiq `after_process_message` middleware:
   write result into `workflow_steps`, enqueue `_tick(run_id)`. Maps Dramatiq's
   own retry exhaustion -> step FAILED -> workflow error path.
5. **Timer scanner** — periodic actor (cron/`periodiq`): timers with
   `fire_at <= now AND fired_at IS NULL` -> mark fired, enqueue `_tick`.
6. **Recovery scanner** — periodic actor: runs in RUNNING with expired lease
   (worker died mid-tick) -> reset to enqueueable, enqueue `_tick`. Safe because
   steps are memoized; activities must be idempotent (documented caveat, same as
   Temporal).
7. **Client API** — `start / signal / get / cancel / list`.
8. **Admin UI** — list runs, filter by status/name, inspect step history
   timeline, buttons: retry / cancel / terminate / send-signal. Scaffold inside
   the existing **SQLAdmin** panel for speed; a standalone React view is post-MVP.

---

## 6. Correctness invariants (the parts that bite)

- **Single-writer per run.** Two `_tick(run_id)` messages can be in flight
  (activity completion + recovery scan). The per-run advisory lock + status CAS
  guarantees only one advances; the loser no-ops. **This is the #1 correctness
  requirement.**
- **Determinism.** Workflow code must not do wall-clock/random/IO directly — only
  through `ctx`. Ship a guard that flags obvious violations in dev.
- **At-least-once activities.** Crash after side-effect but before recording =
  duplicate run on replay. Activities must be idempotent (Temporal has the same
  contract). Provide an `idempotency_key` helper.
- **Versioning.** Code changes break replay of in-flight runs. MVP: pin `version`
  at start; reject replay if registered version != run version (park as
  NEEDS_MIGRATION). `ctx.patched(id)` gating is post-MVP.
- **History size.** Memoized payloads live in Postgres; large results should be
  stored by reference (S3/blob) — offer a pluggable payload codec.

---

## 7. MVP scope (what ships first)

**In:** `@workflow`/`@activity`, sequential `ctx.activity`, `ctx.sleep`,
`ctx.wait_signal`, `ctx.gather`, per-activity retries, crash recovery, advisory
locking, client API, SQLAdmin run/history viewer + cancel/signal.

**Out (fast-follow):** child workflows, `continue-as-new` (history truncation for
long loops), `ctx.patched` versioning, search attributes / advanced query,
standalone React dashboard, signal-with-start, cron schedules, exactly-once
activity via dedup table.

---

## 8. Milestones (~5–6 weeks, 1 dev)

1. **W1 — Skeleton + persistence.** Models + Alembic migration; registry;
   `start()` inserts run + enqueues `_tick`; `_tick` runs a no-op orchestrator to
   COMPLETED. Advisory lock in place.
2. **W2 — Activities + replay.** `ctx.activity`, memoization, `Suspend`,
   completion hook re-enqueues `_tick`. End-to-end 2-activity sequential workflow
   survives a mid-run worker kill.
3. **W3 — Timers + signals.** `ctx.sleep`, timer scanner; `wait_signal` + signal
   delivery; `side_effect`. Recovery scanner for stale leases.
4. **W4 — Parallel + retries + cancel.** `ctx.gather` barrier; per-activity retry
   policy wired to Dramatiq retries; cancellation/termination.
5. **W5 — Observability.** SQLAdmin views, step timeline, action buttons;
   structured logs/metrics (OTel spans per step — you already run OpenTelemetry).
6. **W6 — Hardening.** Determinism guard, payload codec, docs, load test
   (1k concurrent runs), failure-injection tests.

---

## 9. Testing strategy

- **Determinism/replay tests:** run orchestrator, snapshot history, kill at every
  step boundary, assert resume produces identical history + result.
- **Concurrency:** fire activity-completion and recovery `_tick` simultaneously;
  assert exactly one advance (advisory lock).
- **Idempotency:** duplicate activity delivery -> single logical effect.
- **Timer/signal races:** signal arrives before vs after suspend; timer fires
  during a tick.
- Use the existing pytest + Factory Boy setup; `APP_ENV=test` NullPool async
  engine caveat applies if any async paths are added.

---

## 10. Open decisions (for you)

1. **Build vs adopt** — if running a separate Temporal/Hatchet cluster is
   acceptable, that's less code. This plan assumes you specifically want it *on
   Dramatiq* with no new daemon.
2. **Replay-from-top (B) vs event-sourced (A)** — MVP assumes (B). (A) only if you
   need strict Temporal-grade history semantics.
3. **Standalone library vs in-repo module** — package as `duratiq` for reuse
   across aizap + aizap-motos, or start as an internal module and extract later.
4. **Scheduler dependency** — timer/recovery scanners need a periodic trigger
   (`periodiq`, APScheduler, or cron). Pick one.
