# Duratiq — Durable Workflows for Dramatiq

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
this MVP is the gap-filler. For a stack that already runs Dramatiq + Postgres +
RabbitMQ, that's a strong fit.

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
   across projects, or start as an internal module and extract later.
4. **Scheduler dependency** — timer/recovery scanners need a periodic trigger
   (`periodiq`, APScheduler, or cron). Pick one.

---

## 11. Post-MVP roadmap (feature ideas)

The engine's primitives are close to Temporal's, so most gaps are operational.
Ordered by which one causes trouble first in production.

**Proposed for 0.3.0 (items 1–3)**

1. **History retention.** No code deletes finished runs, so runs, steps and
   signals grow without limit. Add `purge_completed(older_than=...)` on the
   store and call it from the scanner.
2. **Non-retryable errors.** Add `non_retryable=(ValueError, ...)` on
   `@activity` or a `NonRetryableError` exception, so that a permanent failure
   does not use up the whole retry budget.
3. **Wait for a result.** Add `Engine.result(run_id, timeout=...)`, so callers
   do not each write a polling loop around `get()`.

**Next**

4. **Per-activity queues.** Add `queue=` on `@activity`. `DramatiqDriver` puts
   ticks and all activities on `duratiq`. A separate queue gives slow or heavy
   activities their own workers and is the simplest way to limit concurrency.
5. ~~**Test helpers.**~~ Done: `duratiq.testing.WorkflowEnvironment`.
6. **Schedule gaps.** Add cron timezone support (today cron is UTC only), a
   catch-up policy for missed fires (today they are skipped), and a manual
   "trigger now".
7. **Admin UI.** Add a schedules page (list, pause, resume, delete) and
   endpoints for query, update, `reset_to_step` and batch operations. Update
   the README's "What's next" section, which still says terminate is missing.

**Smaller or speculative**

- `ctx.wait_condition(lambda: ...)`, to replace loops over `wait_signal`.
- Signals from one workflow to another (today this needs an activity).
- A saga/compensation example (an example, not an API).
- A replay check against stored production histories in CI, to catch
  `DeterminismError` before deploy.
- Metrics (counters and histograms) in addition to OTel tracing.

**Skip:** asyncio workflows and activities. Dramatiq is sync, so this would be
a rewrite with little benefit.

---

## 12. CI/CD and checks

Already in place: tests on Python 3.10–3.13, an e2e job against Postgres, the
90% coverage gate, the build and twine check, pre-commit with ruff, admin CI,
the OIDC release to PyPI, SHA-pinned actions and `cancel-in-progress`.

**Done.** Items 1–5 and the `publish.yml` fix landed in #77, #78, #79, #81, #82
and #83. `publish.yml` is now the single PyPI publisher.

**Original list, in priority order**

1. **Type checking.** Run ty or mypy in pre-commit and CI. The library ships
   `py.typed`, but nothing checks the types.
2. **Migration drift check.** Run `alembic upgrade head` and then
   `alembic check` against SQLite and Postgres.
3. **More ruff rules.** Set `select = ["E","F","I","B","UP","ASYNC","S"]`,
   which covers imports, bugbear, pyupgrade, blocking calls in async code and
   bandit's security checks. Today only the default E/F rules run. Expect
   cleanup first: under the snapshot's rule set `main` has hundreds of
   errors (mostly ANN and T201; in `src/`, `zip()` without `strict=` at
   `context.py:46, 873, 888`).
4. **`timeout-minutes` on every job.** No workflow sets one now. The
   `e2e-postgres` job also lacks a `merge_group` trigger and still pins the
   older checkout/setup-python v6 SHAs.
5. **Tidier Dependabot config.** Add `labels: [dependencies]` and a `chore`
   commit-message prefix, and group the pip entries.
6. Optional: `CODEOWNERS`, a PR template, a `@claude` workflow and a
   conflict labeler. They add little on a solo library.

**Fix:** `publish.yml` publishes when a GitHub Release is created, and
`release.yml` publishes when a `v*` tag is pushed. If both run, PyPI rejects the
second upload. `RELEASING.md` uses the tag flow, so remove `publish.yml` after
checking which workflow the PyPI trusted publisher names. `publish.yml` also
uses its actions by mutable tag while holding `id-token: write`; every other
workflow pins by SHA.

The unmerged snapshot reviewed in §13 already implements items 1–5 and the
`publish.yml` removal. Port those pieces onto `main` by hand; applying the
snapshot over `main` would delete `main`'s newer features and tests.

---

## 13. Adversarial review findings (2026-11-16)

These findings come from an adversarial review of a code snapshot
(`619f3d4d…_8_a_all_code.tar.gz`). That snapshot differs from `main` in
about 40 files, but the library logic is effectively unchanged. All 31
findings were re-checked against `main` on 2026-11-17 and still apply.
"Confirmed" means a repro passed on SQLite with `LocalDriver` or the Dramatiq
StubBroker. Nothing was run against Postgres.

**Fix first:** 1, 3, 4 and 6 make runs fail or hang permanently. A guard in
`complete_step` (now partly in place, see 1) would fix 1 and reduce 5 and 8:
it should reject results for a step that is not SCHEDULED or that came from
a superseded dispatch.

**Critical: runs become incorrect or hang forever**

1. [ ] **Late or redelivered activity results overwrite finished steps.**
   `complete_step` skips only CANCELLED steps. So FAILED can flip to
   COMPLETED (or back) after the workflow has moved on. After
   continue-as-new, a stale result can also land on the new iteration's step
   with the same seq. (`store.py` `complete_step`,
   `engine.report_activity_result`) Confirmed.
   **Partly fixed:** `complete_step` now accepts a result only while the step
   is SCHEDULED, so a finished step can't flip. Still open: the
   continue-as-new case, and a late result from an earlier attempt still
   resolves a retrying step. Both need a per-dispatch token, because the
   Dramatiq retry count and `step.attempt` don't agree on the attempt
   number.
2. [ ] **Children are never restarted after continue-as-new, `retry` or
   `reset_to_step`.** `_start_child` treats `(parent_run_id, parent_seq)` as
   unique, so it re-ticks the old, finished child and the parent stays
   SUSPENDED. (`engine._start_child`) Confirmed.
3. [ ] **A cancellation scope stays active after its `with` block.**
   `__exit__` never clears `_active_scope`, and the abandoned seq is never
   skipped. The next `ctx.*` call then raises `DeterminismError`, or
   `_ScopeCancelled`, which is a `BaseException` that `tick` does not catch.
   The tick then rolls back and the broker retries forever. Pending
   activities in the scope are not cancelled, despite the docstring.
   (`context.py`) Confirmed.
4. [ ] **Execution and run timeouts only set FAILED.** They do not call
   `_notify_parent`, cascade to children or emit an event. The parent's
   child step stays SCHEDULED forever and grandchildren keep running.
   (`engine.py` timeout scanners) Confirmed.
5. [ ] **The schedule-to-start timeout fails activities that are already
   running.** Nothing clears `schedule_to_start_at` when a worker picks the
   activity up. Confirmed.
6. [ ] **An admin cancel or terminate of a child strands its parent.** The
   admin cascades down to children but never notifies the parent. The
   engine's `_notify_parent` needs only the store and `enqueue_tick`, which
   the admin already has. (`admin/backend/app/actions.py`) Confirmed.
7. [ ] **On SQLite, the admin's lock does not exclude workers.** On SQLite,
   `locked_run` uses an in-process `threading.Lock`, which does not span
   processes. A worker tick can commit COMPLETED over an admin cancel or
   terminate. Confirmed.
8. [ ] **Retries stack on Dramatiq.** Dramatiq retries the actor while the
   timeout scanner re-dispatches it with a fresh budget, because
   `timeout_at` is never reset. With `max_retries=2` the repro ran the
   activity 9 times, and attempts kept running after the step was FAILED.
   (`drivers/dramatiq.py`, `engine._timeout_activity`) Confirmed.
9. [ ] **Exceptions that escape the actor's `except Exception` break runs.**
   Examples: `TimeLimitExceeded` (a `BaseException`, default limit 10 min),
   a DB error in `report_activity_result`, or a `get_activity` KeyError
   during a deploy. They re-run the side effect, send the message to the
   dead-letter queue and leave the run SUSPENDED forever. The only rescue is
   `redispatch_orphaned_activities`, and neither the Scanner nor the CLI
   exposes it: `Scanner` calls `recover_stalled` without it
   (`scanner.py:93,123`). Confirmed.

**High**

10. [ ] `Suspend` subclasses `Exception`. A workflow's own
    `try/except Exception` swallows it, so the run completes early and the
    child step is orphaned. Confirmed.
11. [ ] `retry()` deletes only FAILED steps. Steps from a compensation path
    stay in history, so replay raises `DeterminismError`. Confirmed from
    code.
12. [ ] `reset_to_step` deletes the wait steps but leaves their signals and
    updates consumed (`consumed_seq` still set). The replayed `wait_signal`
    then hangs. Confirmed.
13. [ ] `gather` and `select` drop the schedule-to-start and
    schedule-to-close timeouts. Confirmed.
14. [ ] `side_effect` returns the raw value on the first tick and the
    JSON-decoded value on replay. Int keys become strings and tuples become
    lists. Confirmed.
15. [ ] `LocalDriver` re-runs the activity body when saving the result
    fails, which diverges from `DramatiqDriver`. Confirmed.
16. [ ] One failing schedule start (for example, a removed workflow) aborts
    the rest of the claimed batch. Confirmed from code.
17. [ ] Duplicate activity or workflow names silently keep only the last
    registration. For example, `emails.send` and `sms.send` both register
    as `send`. Confirmed from code.
18. [ ] The admin commits to the database, then a broker failure returns
    500. A client retry stores the signal twice. For a run retry, the next
    attempt gets 409 and the run is stuck with no tick queued.
    (`routes_runs.py`) Confirmed.

**Security and deployment**

19. [ ] Compose defaults `ADMIN_TOKEN=change-me-please`. It also publishes
    the backend on `0.0.0.0:8080`, which bypasses nginx, and Postgres on
    5432 with user and password `duratiq`/`duratiq`.
20. [ ] An empty `ADMIN_TOKEN` disables auth, and nothing fails at startup.
    Fail closed unless an explicit dev flag is set.
21. [ ] The token check uses `!=`. Use `hmac.compare_digest`.
22. [ ] Compose never sets `DURATIQ_BROKER_URL`, so Retry and Signal always
    return 503 in the Docker stack.
23. [ ] nginx sends no security headers (CSP, `X-Frame-Options`,
    `X-Content-Type-Options`, `Referrer-Policy`). The token is kept in
    `localStorage`, and the backend container runs as root.

**Medium and low**

24. [ ] Frontend sign-in and sign-out probably need a reload: the probe
    query key does not include the token. Plausible, not run.
25. [ ] Cron `5-7` and `1-7` are rejected because day-of-week 7 is mapped
    to 0 by string replace. `*/2` in day-of-month wrongly triggers the
    day-of-month/day-of-week OR rule. Confirmed.
26. [ ] The recovery scan includes runs that are legitimately sleeping. With
    about 10k such runs, a lost tick waits about 50 minutes for recovery.
27. [ ] Signal `name` and `payload` have no length or size limits. An empty
    name is accepted, and a name over 255 characters returns 500 on
    Postgres.
28. [ ] The admin terminate differs from the engine's: the error messages
    differ (the admin's is `None` when no reason is given,
    `actions.py:70`) and no `RUN_CANCELLED` or `RUN_TERMINATED` events
    fire.
29. [ ] Races (plausible): `update()` checks the run status before taking
    the lock, and the idempotency-key find-then-create in `start` and
    `signal_with_start` can raise `IntegrityError`.
30. [ ] Timer firing has no row lock, so two scanners can fire the same
    timer. The extra ticks are harmless, but the docstring promises
    exactly-once. Plausible.
31. [ ] CI runs only `tests/e2e` (including the migrations test) on
    Postgres, not the full suite. `release.yml` runs `pytest -q` but gates on
    neither coverage nor the Postgres/migration job.

**Found comparing the snapshot with `main`**

32. [x] (Fixed in #80.) An engine without a driver fails with an opaque
    `'NoneType' object has no attribute 'request_tick'`. The snapshot's
    `_require_driver()` gives a clear error but checks only after
    `create_run` commits, leaving an orphan PENDING run that a later call
    with the same idempotency key returns. Check the driver before any
    write (`start`, `signal_with_start`, `update`, `fire_due_timers`).
    Confirmed.
33. [ ] Registering a `functools.partial` without `name=` raises a bare
    `AttributeError` (`registry.py`, `decorators.py`). Raise a `TypeError`
    asking for an explicit name. Confirmed from code.
34. [ ] Under `WorkflowEnvironment` the engine clock is virtual but store
    timestamps (`updated_at`, `completed_at`) use wall time. After
    `env.skip("30d")`, `recover_stalled_runs` sees every run as stalled.
    Plausible, not run.

Reviewed and clean: SQL injection (all params bound), the `limit` cap, auth
on every `/api` route, XSS, N+1 queries, TypeScript and Pydantic schema
parity, the migration chain and downgrades, and the wheel contents.
