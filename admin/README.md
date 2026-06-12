# Duratiq Admin

A read-only web admin for duratiq workflow runs — modeled on the structure of
[full-stack-fastapi-template](https://github.com/fastapi/full-stack-fastapi-template),
but on **SQLAlchemy** (reusing duratiq's own models) and scoped to viewing runs
rather than user management.

- **`backend/`** — FastAPI + SQLAlchemy read API over `workflow_runs` /
  `workflow_steps`. Reuses the `duratiq` package's models + `SqlStore`; never
  mutates engine state. Gated by a single `ADMIN_TOKEN`.
- **`frontend/`** — React + TypeScript + Vite + Chakra UI. A runs list (filter by
  status / name, paginate, status counts) and a run-detail page with the full
  step timeline and input/result/error payloads.

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
| Runs list   | Status counts, filterable/paginated table of runs.                  |
| Run detail  | Run metadata, input/result/error, and the ordered step history.     |

This is the read half of the "SQLAdmin run/history viewer" called out as a next
step in the duratiq plan. Cancel/retry actions were intentionally left out (the
viewer never mutates engine state); they'd be the natural follow-up.
