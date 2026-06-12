"""API tests against an in-memory store seeded with one completed run."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from duratiq import SqlStore
from duratiq.models import WorkflowRun, WorkflowStep

from app.core.config import settings
from app.deps import get_session
from app.main import app


@pytest.fixture
def store() -> SqlStore:
    # StaticPool keeps the in-memory DB alive across sessions/threads.
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    store = SqlStore(engine=engine)
    store.create_all()
    with store.Session.begin() as s:
        s.add(
            WorkflowRun(
                id="A123",
                name="checkout",
                version=1,
                input={"order_id": "A123"},
                status="COMPLETED",
                result={"payment_id": "pay_A123"},
            )
        )
        s.add(
            WorkflowRun(id="B456", name="checkout", version=1, input={}, status="FAILED")
        )
        s.add(
            WorkflowStep(
                run_id="A123",
                seq=0,
                kind="ACTIVITY",
                name="charge_card",
                input={"amount": 1999},
                status="COMPLETED",
                result="pay_A123",
                attempt=0,
            )
        )
    return store


@pytest.fixture
def client(store: SqlStore) -> TestClient:
    def _session_override():
        with store.Session() as s:
            yield s

    app.dependency_overrides[get_session] = _session_override
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_health(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}


def test_list_runs(client: TestClient) -> None:
    body = client.get("/api/runs").json()
    assert body["total"] == 2
    assert {r["id"] for r in body["items"]} == {"A123", "B456"}


def test_filter_runs_by_status(client: TestClient) -> None:
    body = client.get("/api/runs", params={"status": "FAILED"}).json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == "B456"


def test_get_run(client: TestClient) -> None:
    body = client.get("/api/runs/A123").json()
    assert body["name"] == "checkout"
    assert body["result"] == {"payment_id": "pay_A123"}


def test_get_run_404(client: TestClient) -> None:
    assert client.get("/api/runs/NOPE").status_code == 404


def test_get_steps(client: TestClient) -> None:
    steps = client.get("/api/runs/A123/steps").json()
    assert len(steps) == 1
    assert steps[0]["name"] == "charge_card"
    assert steps[0]["result"] == "pay_A123"


def test_stats(client: TestClient) -> None:
    body = client.get("/api/stats").json()
    assert body["total"] == 2
    assert body["by_status"] == {"COMPLETED": 1, "FAILED": 1}


def test_auth_enforced(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "admin_token", "secret")
    assert client.get("/api/runs").status_code == 401
    ok = client.get("/api/runs", headers={"Authorization": "Bearer secret"})
    assert ok.status_code == 200
