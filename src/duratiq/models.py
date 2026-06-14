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
* ``workflow_dedup`` — the idempotency ledger behind ``run_once``. One row per
  recorded effect, keyed by a caller-chosen idempotency key; a redelivered or
  retried activity that reuses the key gets the stored result instead of re-running.
* ``workflow_schedules`` — recurring starts. Each row holds a cron expression and a
  ``next_fire_at``; the schedule scanner starts a fresh run when it comes due and
  advances ``next_fire_at`` to the next cron time.
* ``workflow_updates`` — the inbox for ``engine.update``. Each row is a synchronous,
  mutating request; the workflow consumes it at a ``ctx.wait_update`` point, runs its
  handler, and the handler's result/error is recorded here for the caller to read.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .codec import CodecJSON


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    version: Mapped[int] = mapped_column(Integer, default=1)
    input: Mapped[Any] = mapped_column(CodecJSON, default=dict)
    # PENDING | RUNNING | SUSPENDED | COMPLETED | FAILED | CANCELLED
    status: Mapped[str] = mapped_column(String(20), default="PENDING", index=True)
    result: Mapped[Any] = mapped_column(CodecJSON, nullable=True)
    error: Mapped[Any] = mapped_column(CodecJSON, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True, unique=True)
    # Child-workflow linkage: a run started via ``ctx.child_workflow`` points back at
    # the parent run and the parent's CHILD_WORKFLOW step seq. When the child reaches
    # a terminal state the engine completes that step and re-ticks the parent. Both
    # NULL for a top-level run. ``(parent_run_id, parent_seq)`` is effectively unique —
    # it is how the engine finds an already-started child and avoids duplicating it.
    parent_run_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("workflow_runs.id"), nullable=True, index=True
    )
    parent_seq: Mapped[int | None] = mapped_column(Integer, nullable=True)
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
    # ACTIVITY | TIMER | SIGNAL_WAIT | SIDE_EFFECT | GATHER | CHILD_WORKFLOW | PATCH | UPDATE_WAIT
    kind: Mapped[str] = mapped_column(String(20))
    name: Mapped[str] = mapped_column(String(255))
    input: Mapped[Any] = mapped_column(CodecJSON, nullable=True)
    # SCHEDULED | COMPLETED | FAILED | CANCELLED (the loser of a wait_signal timeout race)
    status: Mapped[str] = mapped_column(String(20), default="SCHEDULED")
    result: Mapped[Any] = mapped_column(CodecJSON, nullable=True)
    error: Mapped[Any] = mapped_column(CodecJSON, nullable=True)
    attempt: Mapped[int] = mapped_column(Integer, default=0)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Per-attempt start-to-close deadline for an outstanding ACTIVITY step (NULL = no
    # timeout). The activity-timeout scanner retries or fails a SCHEDULED activity once
    # this elapses, so a hung or lost activity can't wedge the run forever.
    timeout_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


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
    payload: Mapped[Any] = mapped_column(CodecJSON, nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    consumed_seq: Mapped[int | None] = mapped_column(Integer, nullable=True)


class WorkflowDedup(Base):
    """The idempotency ledger behind ``run_once`` — one row per recorded effect.

    ``key`` is the caller's idempotency key (defaulting to the activity's stable
    ``run_id:seq``). The first ``run_once(key, fn)`` records ``fn``'s result here; a
    later call with the same key returns the stored result without re-running ``fn``,
    so a retried or redelivered activity doesn't repeat its external effect.
    ``run_id`` / ``seq`` are kept for traceability.
    """

    __tablename__ = "workflow_dedup"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    run_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    seq: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result: Mapped[Any] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class WorkflowSchedule(Base):
    """A recurring workflow start, driven by the schedule scanner.

    ``cron`` is a standard 5-field expression; ``next_fire_at`` is the next time a run
    is due (indexed for the scan). When the scanner fires a schedule it starts a run
    with ``input`` as the workflow kwargs, stamps ``last_run_id`` / ``last_fired_at``,
    and advances ``next_fire_at`` to the following cron time. ``active`` gates a
    schedule off without deleting it.
    """

    __tablename__ = "workflow_schedules"

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    cron: Mapped[str] = mapped_column(String(255))
    input: Mapped[Any] = mapped_column(JSON, default=dict)
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    next_fire_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    last_fired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_run_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class WorkflowUpdate(Base):
    """The inbox for ``engine.update`` — one row per synchronous, mutating request.

    An update is delivered like a signal but carries a **response**: the workflow
    consumes it at a ``ctx.wait_update`` point, runs the registered handler (which
    mutates workflow state and returns a value), and the handler's result — or the
    error it raised — is recorded here. ``consumed_seq`` is the UPDATE_WAIT step that
    took it; ``status`` moves PENDING -> COMPLETED/FAILED. The caller reads the row
    back with ``engine.get_update_result``.
    """

    __tablename__ = "workflow_updates"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflow_runs.id"), index=True)
    name: Mapped[str] = mapped_column(String(255))
    args: Mapped[Any] = mapped_column(CodecJSON, nullable=True)
    # PENDING | COMPLETED | FAILED
    status: Mapped[str] = mapped_column(String(20), default="PENDING", index=True)
    result: Mapped[Any] = mapped_column(CodecJSON, nullable=True)
    error: Mapped[Any] = mapped_column(CodecJSON, nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    consumed_seq: Mapped[int | None] = mapped_column(Integer, nullable=True)
