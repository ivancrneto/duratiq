"""Shared fixtures for the end-to-end suite.

Unlike the unit tests (synchronous LocalDriver, virtual time), these exercise the
production-shaped pieces for real: Dramatiq worker threads, the scanner loop on a
background thread or as its own process, real wall-clock timers, and — when
``DURATIQ_TEST_DATABASE_URL`` points at a PostgreSQL database — the advisory-lock
path that only exists on Postgres. They are slower (seconds, not milliseconds); run
``pytest -m "not e2e"`` for the fast inner loop.

To run the Postgres variants locally::

    docker run -d --rm -p 55432:5432 -e POSTGRES_USER=duratiq -e POSTGRES_PASSWORD=duratiq \\
        -e POSTGRES_DB=duratiq_test postgres:16-alpine
    export DURATIQ_TEST_DATABASE_URL=postgresql+psycopg://duratiq:duratiq@localhost:55432/duratiq_test
    pytest tests/e2e

The Postgres database is wiped (all duratiq tables dropped and recreated) by every
test that uses it — never point this at a database you care about.
"""

from __future__ import annotations

import os
import pathlib
import time
from collections.abc import Callable, Iterator

import pytest

from duratiq import SqlStore
from duratiq.models import Base

ROOT = pathlib.Path(__file__).resolve().parents[2]
PG_URL_ENV = "DURATIQ_TEST_DATABASE_URL"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    here = pathlib.Path(__file__).parent
    for item in items:
        if here in pathlib.Path(item.fspath).parents:
            item.add_marker(pytest.mark.e2e)


def wait_for(predicate: Callable[[], bool], *, timeout: float = 20.0, interval: float = 0.02, what: str = "") -> None:
    """Poll ``predicate`` until it is truthy, failing the test after ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    pytest.fail(f"timed out after {timeout}s waiting for {what or predicate}")


@pytest.fixture
def pg_url() -> str:
    url = os.environ.get(PG_URL_ENV)
    if not url:
        pytest.skip(f"set {PG_URL_ENV} to run the PostgreSQL end-to-end tests")
    pytest.importorskip("psycopg")
    return url


def _fresh_pg_store(url: str) -> SqlStore:
    store = SqlStore(url)
    Base.metadata.drop_all(store.engine)
    Base.metadata.create_all(store.engine)
    return store


@pytest.fixture(params=["sqlite", "postgres"])
def backend(request: pytest.FixtureRequest, tmp_path: pathlib.Path) -> Iterator[tuple[str, SqlStore]]:
    """A fresh store on each backend: a file-backed SQLite DB, and Postgres if configured.

    SQLite is file-backed (not ``:memory:``) so that worker threads, the scanner
    thread, and the test thread each get their own connection, as they would against
    a real database.
    """
    if request.param == "postgres":
        store = _fresh_pg_store(request.getfixturevalue("pg_url"))
    else:
        store = SqlStore(f"sqlite:///{tmp_path / 'e2e.db'}")
        store.create_all()
    try:
        yield request.param, store
    finally:
        store.engine.dispose()


@pytest.fixture
def pg_store(pg_url: str) -> Iterator[SqlStore]:
    store = _fresh_pg_store(pg_url)
    try:
        yield store
    finally:
        store.engine.dispose()


@pytest.fixture(name="wait_for")
def wait_for_fixture() -> Callable[..., None]:
    return wait_for
