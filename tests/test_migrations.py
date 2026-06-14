"""The Alembic baseline must stay in lockstep with ``duratiq.models``.

``upgrade head`` on a fresh database has to produce exactly the schema
``Base.metadata`` describes (the same schema ``SqlStore.create_all`` builds). We
assert that with Alembic's own ``compare_metadata`` — so a model change that ships
without a migration fails CI here, not in production.
"""

from __future__ import annotations

import pathlib

import pytest
from alembic.autogenerate import compare_metadata
from alembic.command import downgrade, upgrade
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from sqlalchemy import create_engine, inspect

from duratiq.models import Base

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _config(url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "src" / "duratiq" / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


@pytest.fixture
def url(tmp_path, monkeypatch) -> str:
    # Don't let a developer's DURATIQ_DATABASE_URL leak into the test.
    monkeypatch.delenv("DURATIQ_DATABASE_URL", raising=False)
    return f"sqlite:///{tmp_path / 'migrations.db'}"


def test_upgrade_head_matches_models(url: str) -> None:
    upgrade(_config(url), "head")

    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn, opts={"target_metadata": Base.metadata})
            diffs = compare_metadata(ctx, Base.metadata)
    finally:
        engine.dispose()

    assert diffs == [], f"alembic head has drifted from duratiq.models: {diffs}"


def test_all_model_tables_created(url: str) -> None:
    upgrade(_config(url), "head")

    engine = create_engine(url)
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()

    assert set(Base.metadata.tables) <= tables


def test_downgrade_base_drops_everything(url: str) -> None:
    cfg = _config(url)
    upgrade(cfg, "head")
    downgrade(cfg, "base")

    engine = create_engine(url)
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()

    # Only Alembic's own bookkeeping table may remain.
    assert tables <= {"alembic_version"}
