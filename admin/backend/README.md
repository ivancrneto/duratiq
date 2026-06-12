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

| Method | Path                      | Description                              |
|--------|---------------------------|------------------------------------------|
| GET    | `/health`                 | Liveness probe (no auth).                |
| GET    | `/api/stats`              | Run counts grouped by status.            |
| GET    | `/api/runs`               | List runs. Filters: `status`, `name`; pagination: `limit` (≤200), `offset`. |
| GET    | `/api/runs/{run_id}`      | One run.                                 |
| GET    | `/api/runs/{run_id}/steps`| Ordered step history for a run.          |

## Config (`.env`)

| Var            | Default                   | Notes                                  |
|----------------|---------------------------|----------------------------------------|
| `DATABASE_URL` | `sqlite:///./duratiq.db`  | Point at the same DB the engine writes.|
| `ADMIN_TOKEN`  | `` (empty)                | Empty disables auth (local dev only).  |
| `CORS_ORIGINS` | `http://localhost:5173`   | Comma-separated allowed origins.       |

## Tests

```bash
uv run pytest -q
```
