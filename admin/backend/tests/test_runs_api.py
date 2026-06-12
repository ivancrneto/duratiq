"""API tests against an in-memory store seeded with one completed run."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from duratiq import SqlStore
from duratiq.models import WorkflowRun, WorkflowStep

from app.core.config import settings
from app.db import get_store
from app.deps import get_enqueue, get_session
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
            WorkflowRun(
                id="B456",
                name="checkout",
                version=1,
                input={},
                status="FAILED",
                error={"type": "ActivityFailed", "message": "boom"},
            )
        )
        # A suspended (non-terminal) run, cancellable.
        s.add(WorkflowRun(id="S789", name="checkout", version=1, input={}, status="SUSPENDED"))
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
        # B456's failed step — retry should drop it.
        s.add(
            WorkflowStep(
                run_id="B456",
                seq=0,
                kind="ACTIVITY",
                name="charge_card",
                input={},
                status="FAILED",
                error={"message": "boom"},
                attempt=0,
            )
        )
    return store


@pytest.fixture
def client(store: SqlStore):
    def _session_override():
        with store.Session() as s:
            yield s

    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_store] = lambda: store
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def enqueued(client: TestClient) -> list[str]:
    """Override the broker enqueue with a recorder; returns the captured run ids."""
    calls: list[str] = []
    app.dependency_overrides[get_enqueue] = lambda: calls.append
    return calls


def test_health(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}


def test_list_runs(client: TestClient) -> None:
    body = client.get("/api/runs").json()
    assert body["total"] == 3
    assert {r["id"] for r in body["items"]} == {"A123", "B456", "S789"}


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
    assert body["total"] == 3
    assert body["by_status"] == {"COMPLETED": 1, "FAILED": 1, "SUSPENDED": 1}


def test_auth_enforced(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "admin_token", "secret")
    assert client.get("/api/runs").status_code == 401
    ok = client.get("/api/runs", headers={"Authorization": "Bearer secret"})
    assert ok.status_code == 200


def test_cancel_suspended_run(client: TestClient) -> None:
    res = client.post("/api/runs/S789/cancel")
    assert res.status_code == 200
    assert res.json()["status"] == "CANCELLED"
    assert client.get("/api/runs/S789").json()["status"] == "CANCELLED"


def test_cancel_terminal_run_409(client: TestClient) -> None:
    res = client.post("/api/runs/A123/cancel")  # COMPLETED
    assert res.status_code == 409


def test_cancel_missing_run_404(client: TestClient) -> None:
    assert client.post("/api/runs/NOPE/cancel").status_code == 404


def test_retry_failed_run(client: TestClient, enqueued: list[str]) -> None:
    res = client.post("/api/runs/B456/retry")
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "PENDING" and body["enqueued"] is True
    # State was reset and a tick was enqueued.
    assert enqueued == ["B456"]
    assert client.get("/api/runs/B456").json()["status"] == "PENDING"
    assert client.get("/api/runs/B456/steps").json() == []  # failed step dropped


def test_retry_non_failed_run_409(client: TestClient, enqueued: list[str]) -> None:
    res = client.post("/api/runs/A123/retry")  # COMPLETED
    assert res.status_code == 409
    assert enqueued == []  # no enqueue, no mutation


def test_retry_without_broker_503(client: TestClient) -> None:
    # No get_enqueue override and broker_url is empty -> fail fast, no mutation.
    res = client.post("/api/runs/B456/retry")
    assert res.status_code == 503
    assert client.get("/api/runs/B456").json()["status"] == "FAILED"
