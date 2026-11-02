# Agent Instructions

> **Note:** `CLAUDE.md` is a symlink to this file. Editing `AGENTS.md` updates both.

Optional, gitignored `AGENTS.local.md` holds per-developer overrides; it is imported below and is simply absent for anyone who has not created one.

@AGENTS.local.md

## Repository Overview

`duratiq` is a Python library (`src/duratiq/`) for durable, Temporal-style workflows on Dramatiq + SQLAlchemy (Postgres or SQLite). Built with hatchling; supports Python 3.10–3.13.

- `src/duratiq/` — the library (engine, store, drivers, scanner, migrations).
- `tests/` — pytest suite for the library.
- `examples/` — runnable examples.
- `admin/` — optional admin UI: `admin/backend` (FastAPI, uv project, own pytest suite) and `admin/frontend` (Vite + React + TypeScript, npm).
- `README.md`, `CHANGELOG.md`, `RELEASING.md` — docs and release process.

## Hard Rules (Non-Negotiable)

If a task appears to require breaking one of these, stop and ask the human first.

### Never push to `main`

- Do not `git push` to `main`, including `--force`, fast-forward merges, or pushes from a detached HEAD that resolves to `main`.
- Do not create a worktree whose upstream tracks `main`; branch off it (`git worktree add ../wt-foo -b feat/foo origin/main`).
- Changes land on `main` only through a reviewed pull request.
- Force-pushing a feature branch is fine (prefer `--force-with-lease`).
- Releases are cut by pushing a `v*` tag per [`RELEASING.md`](RELEASING.md) — only when the human asks.

### Never leave a process you started running

Every process you start is yours to stop: admin backend (`uvicorn`, port 8080), Vite dev server (port 5173), the admin `docker compose` stack, Dramatiq/scanner workers, watch-mode runners, one-off scripts.

- Plan the shutdown when you plan the startup; never start something you have no handle on.
- Prefer one-shot foreground commands (`pytest`, `npm run build`) over daemons, and give long commands a timeout.
- Check before starting (`lsof -ti :8080,:5173`, `docker ps`) and reuse what is already up.
- Before reporting done, verify nothing you started is alive: `lsof -ti :8080,:5173`, `docker ps`, `pgrep -fl 'uvicorn|vite|dramatiq|duratiq-scanner'`.
- Exception: the user explicitly asks you to leave something up — then say what is running, on which port, and how to stop it.

### New env vars / settings must fan out to every consumer

When you add or rename an environment variable or setting (e.g. admin `DATABASE_URL`, `ADMIN_TOKEN`), update every place that consumes it: [`.github/workflows/`](.github/workflows), [`admin/docker-compose.yml`](admin/docker-compose.yml), the admin `Dockerfile`s, the READMEs, and `CHANGELOG.md` when user-visible. For renames, `rg -n '<OLD_NAME>'` must return no hits.

## Code Style

- **No unnecessary comments.** Let names and types carry the meaning; comment only the non-obvious _why_, in one terse line.
- **Never commit secrets** or real credentials.
- **Reuse before adding.** After writing a helper, `rg` for existing code doing the same thing and consolidate.
- **Unique test fixtures.** Never hard-code shared ids/emails/workflow ids across tests; generate a unique value per fixture so state cannot leak between tests.
- Python formatting and lint are `ruff` (line length 120), enforced by pre-commit; keep the library typed (`py.typed`).

## Testing Policy

- **Tests before code.** Write the failing pytest test first, then the code; a test fitted to finished code only restates the implementation.
- **Prefer end-to-end tests** that run real workflows through the engine/driver (see `tests/` for the `LocalDriver` and Dramatiq patterns) over mock-heavy unit tests.
- For isolated tests, first list the ways it can fail, write a test per failure mode, then implement.
- No trivial tests: constants, defaults, `__repr__`, pass-through, or mocks returning what they were told to. Delete them when found.
- Library coverage is gated at 90% (branch) — a regression gate, not a goal.

## Preferred Commands

```bash
python -m pip install -e ".[dev]"                    # library + test deps
pytest -v                                            # library tests (testpaths = tests)
pytest tests/test_cron.py                            # a single file
pytest --cov=duratiq --cov-report=term-missing       # coverage gate (fail_under = 90)
ruff check . && ruff format --check .                # lint + format (also via pre-commit)
pre-commit run --all-files                           # full pre-commit gate
python -m build && python -m twine check dist/*      # sdist + wheel check (CI: build.yml)

# admin backend (from admin/backend)
uv sync --extra dev && uv run pytest
uv run python scripts/seed_demo.py ./duratiq.db      # demo data

# admin frontend (from admin/frontend)
npm ci && npm run lint                               # tsc -b --noEmit
npm run build                                        # typecheck + build (CI)
```

## Commit and PR Guidance

- Keep commits atomic, focused, and descriptive.
- **Before every push, run the gate and let it pass:** `pre-commit run --all-files` and `pytest`; for `admin/backend` changes `uv run pytest`, for `admin/frontend` changes `npm run build`. Never push with failing or skipped checks.
- Install hooks with `pre-commit install`.
- Update `CHANGELOG.md` under `[Unreleased]` for user-visible changes.
- **Never credit an agent.** No `Co-Authored-By:` trailers and no "Generated with …" lines in commits or PR titles/bodies; they must read as the human's own work, even when a harness default would add one.
- **Never commit screenshots or PR media.** Save them outside the git tree and share the path so the human attaches them to the PR.

## AI Assistant Guidance

- **Change only what the task requires, and finish it.** Match neighboring patterns; leave unrelated cleanups for their own PR, but don't hand back half-done work.
- Validate assumptions against this repo's manifests (`pyproject.toml`, `admin/backend/pyproject.toml`, `admin/frontend/package.json`) rather than other repositories' conventions.

### Subagent Model Selection

Right-size the model to the task instead of inheriting the most capable tier.

- **Haiku** — mechanical, narrow work: file lookups, grep/find, running a command and reporting output, rename/format/lint-fix edits.
- **Sonnet** — moderate work with a settled design: multi-file edits, writing tests for an understood module, reviewing a bounded diff, drafting a PR description.
- **Opus** — real thinking only: architecture and planning, non-obvious debugging, data-model/API design, cross-subsystem refactors, security review across trust boundaries.

When in doubt between two tiers, pick the lower one.
