"""The Alembic migrations on a real PostgreSQL database.

``tests/test_migrations.py`` proves the migration chain matches the models on SQLite,
but production runs Postgres, where types, indexes, and batch-mode ALTERs differ. This
replays the chain there — and then runs a workflow on the migrated schema, so a
migration that is structurally "equal" but unusable (say, a missing default) fails too.
"""

from __future__ import annotations

import pathlib

import pytest
from sqlalchemy import create_engine, inspect, text

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.models import Base

alembic = pytest.importorskip("alembic")

from alembic.autogenerate import compare_metadata  # noqa: E402
from alembic.command import downgrade, upgrade  # noqa: E402
from alembic.config import Config  # noqa: E402
from alembic.runtime.migration import MigrationContext  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def empty_pg(pg_url: str, monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.delenv("DURATIQ_DATABASE_URL", raising=False)
    engine = create_engine(pg_url)
    try:
        Base.metadata.drop_all(engine)
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
    finally:
        engine.dispose()
    return pg_url


def _config(url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "src" / "duratiq" / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def test_upgrade_head_on_postgres_matches_models_and_runs_a_workflow(empty_pg: str) -> None:
    upgrade(_config(empty_pg), "head")

    engine = create_engine(empty_pg)
    try:
        with engine.connect() as conn:
            diffs = compare_metadata(MigrationContext.configure(conn), Base.metadata)
    finally:
        engine.dispose()
    assert diffs == [], f"alembic head has drifted from duratiq.models on PostgreSQL: {diffs}"

    reg = Registry()

    @activity(name="double", registry=reg)
    def double(x: int) -> int:
        return x * 2

    @workflow(name="migrated", registry=reg)
    def migrated(ctx, x: int) -> dict:
        ctx.upsert_search_attributes({"region": "eu"})
        value = ctx.activity(double, x)
        ctx.sleep(0)
        return {"value": value, "now": ctx.now().isoformat()}

    store = SqlStore(empty_pg)
    try:
        engine_ = Engine(reg, store)
        driver = LocalDriver(engine_)
        run_id = engine_.start("migrated", x=21, memo={"source": "e2e"}, workflow_id="wf-1")
        driver.run_until_idle()
        engine_.fire_due_timers()
        driver.run_until_idle()
        run = store.get_run(run_id)
        assert run.status == "COMPLETED", run.error
        assert run.result["value"]["value"] == 42
        assert [r.id for r in engine_.list_runs(search_attributes={"region": "eu"})] == [run_id]
    finally:
        store.engine.dispose()


def test_downgrade_base_on_postgres_drops_everything(empty_pg: str) -> None:
    cfg = _config(empty_pg)
    upgrade(cfg, "head")
    downgrade(cfg, "base")

    engine = create_engine(empty_pg)
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()
    assert tables <= {"alembic_version"}
