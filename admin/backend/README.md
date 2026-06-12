# Duratiq Admin — backend

Read-only FastAPI API over duratiq's `workflow_runs` / `workflow_steps` tables.
Reuses the `duratiq` package's SQLAlchemy models and `SqlStore`; it never mutates
engine state.

## Run

```bash
cd admin/backend
uv sync --extra dev
cp .env.example .env          # set DATABASE_URL + ADMIN_TOKEN
uv run uvicorn app.main:app --reload --port 8080
```

Open http://localhost:8080/docs for Swagger UI. All `/api/*` routes require
`Authorization: Bearer $ADMIN_TOKEN` unless `ADMIN_TOKEN` is empty (auth disabled,
local only).

## Endpoints

| Method | Path                       | Description                              |
|--------|----------------------------|------------------------------------------|
| GET    | `/health`                  | Liveness probe (no auth).                |
| GET    | `/api/stats`               | Run counts grouped by status.            |
| GET    | `/api/runs`                | List runs. Filters: `status`, `name`; pagination: `limit` (≤200), `offset`. |
| GET    | `/api/runs/{run_id}`       | One run.                                 |
| GET    | `/api/runs/{run_id}/steps` | Ordered step history for a run.          |
| POST   | `/api/runs/{run_id}/cancel`| Mark a non-terminal run CANCELLED. 409 if already terminal. |
| POST   | `/api/runs/{run_id}/retry` | Re-arm a FAILED run and enqueue a tick. 409 if not FAILED, 503 if no broker. |

### Actions (cancel / retry)

Both are state-changing, so they require the admin token in any shared setting.

- **Cancel** is store-only: it writes `status=CANCELLED`, which the engine's
  `tick()` already honours. Works from the admin with no extra config.
- **Retry** resets the failed run to `PENDING` (dropping the failed step) and
  enqueues a `duratiq_tick` message so a running worker resumes it. The admin
  holds no workflow registry, so it can't tick itself — set `DURATIQ_BROKER_URL`
  (and `pip install '.[broker]'`) to point at the broker your workers consume.
  Without it, retry returns **503** and never mutates the run.

## Config (`.env`)

| Var                 | Default                   | Notes                                  |
|---------------------|---------------------------|----------------------------------------|
| `DATABASE_URL`      | `sqlite:///./duratiq.db`  | Point at the same DB the engine writes.|
| `ADMIN_TOKEN`       | `` (empty)                | Empty disables auth (local dev only).  |
| `CORS_ORIGINS`      | `http://localhost:5173`   | Comma-separated allowed origins.       |
| `DURATIQ_BROKER_URL`| `` (empty)                | `redis://…` or `amqp://…`. Required for Retry; empty => retry returns 503. |
| `DURATIQ_BROKER_QUEUE`| `duratiq`               | Queue the `duratiq_tick` actor listens on. |

## Tests

```bash
uv run pytest -q
```
