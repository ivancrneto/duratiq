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


# --- search attributes, signal, cascade cancel, new fields (engine parity) ---

from datetime import datetime, timezone  # noqa: E402


def test_search_attribute_filter(client: TestClient, store: SqlStore) -> None:
    store.upsert_search_attributes("A123", {"region": "eu"})
    store.upsert_search_attributes("B456", {"region": "us"})

    body = client.get("/api/runs", params={"sa": '{"region": "eu"}'}).json()
    assert {r["id"] for r in body["items"]} == {"A123"}
    assert body["total"] == 1


def test_search_attribute_filter_bad_json_400(client: TestClient) -> None:
    assert client.get("/api/runs", params={"sa": "not-json"}).status_code == 400
    assert client.get("/api/runs", params={"sa": "[1,2]"}).status_code == 400  # not an object


def test_run_detail_includes_search_attributes(client: TestClient, store: SqlStore) -> None:
    store.upsert_search_attributes("A123", {"region": "eu", "tier": 2})
    body = client.get("/api/runs/A123").json()
    assert body["search_attributes"] == {"region": "eu", "tier": 2}


def test_step_exposes_timeout_and_heartbeat(client: TestClient, store: SqlStore) -> None:
    with store.Session.begin() as s:
        s.add(
            WorkflowStep(
                run_id="S789",
                seq=0,
                kind="ACTIVITY",
                name="slow",
                status="SCHEDULED",
                attempt=0,
                timeout_at=datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc),
                heartbeat={"value": {"page": 3}},
            )
        )
    step = client.get("/api/runs/S789/steps").json()[0]
    assert step["timeout_at"] is not None
    assert step["heartbeat"] == {"value": {"page": 3}}


def test_run_detail_exposes_parent_linkage(client: TestClient, store: SqlStore) -> None:
    with store.Session.begin() as s:
        s.add(
            WorkflowRun(
                id="CHILD1", name="ship", version=1, input={}, status="SUSPENDED", parent_run_id="S789", parent_seq=0
            )
        )
    body = client.get("/api/runs/CHILD1").json()
    assert body["parent_run_id"] == "S789"
    assert body["parent_seq"] == 0


def test_cancel_cascades_to_children(client: TestClient, store: SqlStore) -> None:
    # S789 (suspended) gets a running child and grandchild.
    with store.Session.begin() as s:
        s.add(
            WorkflowRun(
                id="C1", name="ship", version=1, input={}, status="SUSPENDED", parent_run_id="S789", parent_seq=0
            )
        )
        s.add(
            WorkflowRun(
                id="GC1", name="label", version=1, input={}, status="SUSPENDED", parent_run_id="C1", parent_seq=0
            )
        )
        # A done child must be left untouched.
        s.add(
            WorkflowRun(
                id="C2", name="ship", version=1, input={}, status="COMPLETED", parent_run_id="S789", parent_seq=1
            )
        )

    assert client.post("/api/runs/S789/cancel").status_code == 200
    assert client.get("/api/runs/S789").json()["status"] == "CANCELLED"
    assert client.get("/api/runs/C1").json()["status"] == "CANCELLED"
    assert client.get("/api/runs/GC1").json()["status"] == "CANCELLED"
    assert client.get("/api/runs/C2").json()["status"] == "COMPLETED"  # already done


def test_signal_matches_waiting_run(client: TestClient, store: SqlStore, enqueued: list[str]) -> None:
    with store.Session.begin() as s:
        s.add(WorkflowStep(run_id="S789", seq=0, kind="SIGNAL_WAIT", name="go", status="SCHEDULED", attempt=0))

    res = client.post("/api/runs/S789/signal", json={"name": "go", "payload": {"ok": True}})
    assert res.status_code == 200
    assert res.json()["enqueued"] is True
    assert enqueued == ["S789"]
    # The waiting step was matched and completed with the payload.
    step = client.get("/api/runs/S789/steps").json()[0]
    assert step["status"] == "COMPLETED"
    assert step["result"] == {"value": {"ok": True}}


def test_signal_terminal_run_409(client: TestClient, enqueued: list[str]) -> None:
    res = client.post("/api/runs/A123/signal", json={"name": "go"})  # COMPLETED
    assert res.status_code == 409
    assert enqueued == []


def test_signal_missing_run_404(client: TestClient, enqueued: list[str]) -> None:
    assert client.post("/api/runs/NOPE/signal", json={"name": "go"}).status_code == 404


def test_signal_without_broker_503(client: TestClient) -> None:
    # No get_enqueue override -> broker_url empty -> fail fast, nothing stored.
    res = client.post("/api/runs/S789/signal", json={"name": "go"})
    assert res.status_code == 503
