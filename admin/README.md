# Duratiq Admin

A read-only web admin for duratiq workflow runs — modeled on the structure of
[full-stack-fastapi-template](https://github.com/fastapi/full-stack-fastapi-template),
but on **SQLAlchemy** (reusing duratiq's own models) and scoped to viewing runs
rather than user management.

- **`backend/`** — FastAPI + SQLAlchemy API over `workflow_runs` /
  `workflow_steps`. Reuses the `duratiq` package's models + `SqlStore`. Read
  endpoints plus two actions — **cancel** (store-only) and **retry** (resets a
  failed run + enqueues a tick via a broker). Gated by a single `ADMIN_TOKEN`.
- **`frontend/`** — React + TypeScript + Vite + shadcn/ui (Tailwind), matching
  izap-studio's stack. A runs list (filter by
  status / name, paginate, status counts) and a run-detail page with the full
  step timeline, input/result/error payloads, and Cancel / Retry buttons.

## Run it locally (two terminals)

**1. Backend** — seed a demo DB and serve the API:

```bash
cd backend
uv sync --extra dev
uv run python scripts/seed_demo.py ./duratiq.db        # a few demo runs
DATABASE_URL=sqlite:///./duratiq.db ADMIN_TOKEN= \
  uv run uvicorn app.main:app --reload --port 8080
```

(`ADMIN_TOKEN=` empty disables auth for local dev. Swagger UI: http://localhost:8080/docs)

**2. Frontend** — Vite dev server, proxies `/api` to the backend:

```bash
cd frontend
npm install
npm run dev          # http://localhost:5173
```

Point the admin at your real engine's database by setting `DATABASE_URL` to the
same Postgres/SQLite URL the duratiq engine writes to.

## Run it with Docker

```bash
cd admin
ADMIN_TOKEN=your-secret docker compose up --build
# frontend: http://localhost:8081   backend: http://localhost:8080
```

The admin is read-only and does not create the schema — point `DATABASE_URL` at a
database the duratiq engine already populates (see `docker-compose.yml`).

## What it shows

| View        | Contents                                                            |
|-------------|---------------------------------------------------------------------|
| Runs list   | Status counts, filter by status / name / **search attributes**, paginated. |
| Run detail  | Run metadata, **parent-run link**, **search attributes**, input/result/error, the step timeline (with timeouts + heartbeat progress), and actions. |

Filter the runs list by search attribute with a JSON object — `{"region":"eu"}` —
matching `engine.list_runs(search_attributes=…)` (ANDed equality).

### Actions

- **Cancel** (non-terminal runs) — writes `status=CANCELLED` and **cascades** to any
  still-running child workflows (matching `Engine.cancel`'s downward cascade); the
  engine's `tick()` honours it. No broker needed. (Failing a *directly* cancelled
  child's parent needs the registry, so that upward notification is left to the
  engine — the admin only cascades downward.)
- **Retry** (FAILED runs) — resets the run to `PENDING`, drops the failed step,
  and enqueues a `duratiq_tick` so a running worker resumes it.
- **Send signal** (non-terminal runs) — delivers a named signal (with optional JSON
  payload), pairing it with any waiting `ctx.wait_signal`, then enqueues a tick so the
  run advances. Like retry, requires `DURATIQ_BROKER_URL`; without it, returns 503.

The underlying `Engine.cancel()` / `Engine.retry()` / `Engine.signal()` also live in
duratiq core, so they're usable programmatically without the admin.
