# Changelog

All notable changes to **duratiq** are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] — 2026-06-15

First public release: a feature-complete durable-execution engine for Dramatiq.
Workflows are deterministic orchestrator functions; activities are ordinary
functions dispatched as Dramatiq messages; all state lives in Postgres (or SQLite
for dev), so a run resumes exactly where it left off after a crash.

### Added

**Core engine**

- Replay-from-top execution with DB-memoized steps: each tick replays the
  orchestrator, completed steps return their recorded result, and the first
  not-ready point suspends the run.
- Single-writer-per-run guarantee via a per-run lock (Postgres
  `pg_advisory_xact_lock`; an in-process lock on SQLite).
- Crash recovery and a recovery scanner (`recover_stalled`), with opt-in
  re-dispatch of orphaned untimed activities.

**Workflow context (`ctx`)**

- `activity` and `gather` (parallel barrier, via `defer`).
- `sleep` durable timers; `wait_signal` (with `timeout=`); `side_effect`.
- `select` — race the first of several timer / signal / child-workflow branches.
- `child_workflow` — durable sub-runs, with downward cancellation cascade.
- `continue_as_new` — bounded history for long-running loops.
- `patched` — gate workflow-code changes so in-flight runs replay deterministically.
- `set_query_handler` / `set_update_handler` — queries and validated updates.
- `upsert_search_attributes` — typed, indexed run metadata.

**Activities**

- Per-activity retry policy wired to Dramatiq's Retries middleware.
- Start-to-close timeouts and heartbeats, driven by an activity-timeout scanner.
- `activity_info` (a stable `run_id:seq` idempotency key) and `run_once`
  (dedup-table-backed exactly-once effects across retries/redelivery).

**Client API (`Engine`)**

- `start`, `signal`, `signal_with_start`, `get`, `list_runs` / `count_runs`
  (filter by status, name, and search attributes), `cancel` (cascading), `retry`.
- `query`, `update` / `get_update_result`, `get_search_attributes`.
- Recurring schedules: `create_schedule` (5-field cron) with
  `pause`/`resume`/`delete` and `fire_due_schedules`.
- Periodic scanners: `fire_due_timers`, `fire_due_schedules`,
  `fire_due_activity_timeouts`, `recover_stalled`.

**Persistence & infrastructure**

- `SqlStore` over SQLAlchemy (SQLite and PostgreSQL).
- Pluggable payload codec (`set_payload_codec`) for compressing or offloading large
  payloads out of the database.
- Alembic migrations with a baseline schema.
- `LocalDriver` (synchronous, for dev/tests) and `DramatiqDriver`.
- `duratiq-scanner` console entry point that runs the periodic scans.

**Observability**

- Lifecycle-event listener (`Engine(listener=...)`, `WorkflowEvent`).
- OpenTelemetry tracing built on the listener hook (`duratiq.otel`).

**Admin UI** (`admin/`)

- FastAPI + SQLAlchemy backend over duratiq's models and a React/TypeScript
  frontend: a filterable/paginated runs list (status, name, search attributes), a
  run-detail view with the step timeline (timeouts + heartbeat progress) and
  parent-run links, and cancel / retry / send-signal actions, gated by a token.

**Packaging**

- Typed (`py.typed`, PEP 561). Optional extras: `dramatiq`, `redis`, `postgres`,
  `migrations`, `otel`, `examples`, `dev`.

[Unreleased]: https://github.com/ivancrneto/duratiq/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ivancrneto/duratiq/releases/tag/v0.1.0
