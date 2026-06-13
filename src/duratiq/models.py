"""SQLAlchemy ORM models — the durable state of every workflow run.

The tables are the heart of the engine:

* ``workflow_runs``   — one row per workflow execution.
* ``workflow_steps``  — the event history; one row per ``ctx.*`` command, keyed by
  the deterministic ``seq`` index. Replay reads these to skip completed work.
* ``workflow_timers``  — the due-time index for ``ctx.sleep``. A timer points back
  at its ``(run_id, seq)`` TIMER step; the timer scanner finds the ones whose
  ``fire_at`` has elapsed, marks the step COMPLETED, and re-ticks the run.
* ``workflow_signals`` — the inbox for ``ctx.wait_signal``. Signals are stored
  independently of the waits that consume them (a signal can arrive before the
  workflow waits for it); ``consumed_seq`` records which SIGNAL_WAIT step took it.
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
    # Reserved for a future leased-tick model. Unused today: a tick is atomic under
    # a transaction-scoped advisory lock, so a worker dying mid-tick rolls back
    # cleanly — there is no partial state to lease. Recovery instead re-ticks stale
    # runs (see Engine.recover_stalled), keying off updated_at.
    lease_owner: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class WorkflowStep(Base):
    __tablename__ = "workflow_steps"

    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflow_runs.id"), primary_key=True)
    seq: Mapped[int] = mapped_column(Integer, primary_key=True)
    # ACTIVITY | TIMER | SIGNAL_WAIT | SIDE_EFFECT | GATHER | PATCH
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


class WorkflowTimer(Base):
    """The due-time index for ``ctx.sleep`` durable timers.

    One row per TIMER step. ``fire_at`` is computed once at schedule time (so it
    survives replay), and ``fired_at`` is stamped when the scanner delivers it —
    the ``fired_at IS NULL`` guard makes firing exactly-once even if the scanner
    overlaps with itself.
    """

    __tablename__ = "workflow_timers"

    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflow_runs.id"), primary_key=True)
    seq: Mapped[int] = mapped_column(Integer, primary_key=True)
    fire_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    fired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


class WorkflowSignal(Base):
    """The inbox for ``ctx.wait_signal`` — one row per delivered signal.

    A signal may arrive before the workflow reaches the matching wait, so signals
    live here independently and are paired with SIGNAL_WAIT steps FIFO by name.
    ``consumed_seq`` is the seq of the wait that took it (``NULL`` while unconsumed).
    """

    __tablename__ = "workflow_signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflow_runs.id"), index=True)
    name: Mapped[str] = mapped_column(String(255))
    payload: Mapped[Any] = mapped_column(JSON, nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    consumed_seq: Mapped[int | None] = mapped_column(Integer, nullable=True)
