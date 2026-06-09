"""SQLAlchemy ORM models — the durable state of every workflow run.

The two tables are the heart of the engine:

* ``workflow_runs``  — one row per workflow execution.
* ``workflow_steps`` — the event history; one row per ``ctx.*`` command, keyed by
  the deterministic ``seq`` index. Replay reads these to skip completed work.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    version: Mapped[int] = mapped_column(Integer, default=1)
    input: Mapped[Any] = mapped_column(JSON, default=dict)
    # PENDING | RUNNING | SUSPENDED | COMPLETED | FAILED | CANCELLED
    status: Mapped[str] = mapped_column(String(20), default="PENDING", index=True)
    result: Mapped[Any] = mapped_column(JSON, nullable=True)
    error: Mapped[Any] = mapped_column(JSON, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True, unique=True)
    lease_owner: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class WorkflowStep(Base):
    __tablename__ = "workflow_steps"

    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflow_runs.id"), primary_key=True)
    seq: Mapped[int] = mapped_column(Integer, primary_key=True)
    # ACTIVITY | TIMER | SIGNAL_WAIT | SIDE_EFFECT | GATHER
    kind: Mapped[str] = mapped_column(String(20))
    name: Mapped[str] = mapped_column(String(255))
    input: Mapped[Any] = mapped_column(JSON, nullable=True)
    # SCHEDULED | COMPLETED | FAILED
    status: Mapped[str] = mapped_column(String(20), default="SCHEDULED")
    result: Mapped[Any] = mapped_column(JSON, nullable=True)
    error: Mapped[Any] = mapped_column(JSON, nullable=True)
    attempt: Mapped[int] = mapped_column(Integer, default=0)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
