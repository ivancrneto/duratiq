"""Persistence layer over SQLAlchemy.

Works on SQLite (dev/tests) and PostgreSQL (production). The one
production-critical primitive here is :meth:`SqlStore.locked_run`, which serialises
all ticks for a given run so the engine never advances the same run twice
concurrently. On PostgreSQL it uses a transaction-scoped advisory lock; on SQLite
it falls back to an in-process lock (single-process dev/test only).
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from sqlalchemy import Engine as SaEngine
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker

from .models import Base, WorkflowRun, WorkflowSignal, WorkflowStep, WorkflowTimer, utcnow


def _advisory_key(run_id: str) -> int:
    digest = hashlib.blake2b(run_id.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)


class SqlStore:
    def __init__(self, url: str = "sqlite://", *, engine: SaEngine | None = None) -> None:
        self.engine: SaEngine = engine or create_engine(url, future=True)
        self.is_postgres = self.engine.dialect.name == "postgresql"
        self.Session = sessionmaker(self.engine, expire_on_commit=False, future=True)
        self._local_locks: dict[str, threading.Lock] = {}
        self._local_guard = threading.Lock()

    def create_all(self) -> None:
        Base.metadata.create_all(self.engine)

    # ------------------------------------------------------------------ runs
    def create_run(
        self,
        *,
        run_id: str,
        name: str,
        version: int,
        input: dict,
        idempotency_key: str | None = None,
        parent_run_id: str | None = None,
        parent_seq: int | None = None,
    ) -> str:
        with self.Session.begin() as session:
            session.add(
                WorkflowRun(
                    id=run_id,
                    name=name,
                    version=version,
                    input=input,
                    status="PENDING",
                    idempotency_key=idempotency_key,
                    parent_run_id=parent_run_id,
                    parent_seq=parent_seq,
                )
            )
        return run_id

    def find_child_run(self, parent_run_id: str, parent_seq: int) -> WorkflowRun | None:
        """Return the child run started by a parent's CHILD_WORKFLOW step, if any.

        Lets the engine make child-start idempotent: a re-tick that would otherwise
        start the same child twice finds the existing run instead.
        """
        with self.Session() as s:
            return s.scalar(
                select(WorkflowRun).where(
                    WorkflowRun.parent_run_id == parent_run_id,
                    WorkflowRun.parent_seq == parent_seq,
                )
            )

    def find_active_children(self, parent_run_id: str) -> list[str]:
        """Return the ids of this run's non-terminal child runs.

        Used by cancellation to cascade: a parent coming down takes its still-running
        children with it. Terminal children (already done/failed/cancelled) are left
        as they are.
        """
        with self.Session() as s:
            return list(
                s.scalars(
                    select(WorkflowRun.id).where(
                        WorkflowRun.parent_run_id == parent_run_id,
                        WorkflowRun.status.not_in(("COMPLETED", "FAILED", "CANCELLED")),
                    )
                )
            )

    def get_run(self, run_id: str, *, session: Session | None = None) -> WorkflowRun | None:
        if session is not None:
            return session.get(WorkflowRun, run_id)
        with self.Session() as s:
            return s.get(WorkflowRun, run_id)

    def find_by_idempotency_key(self, key: str) -> WorkflowRun | None:
        with self.Session() as s:
            return s.scalar(select(WorkflowRun).where(WorkflowRun.idempotency_key == key))

    def update_run(self, run_id: str, *, session: Session | None = None, **fields: Any) -> None:
        def _apply(s: Session) -> None:
            run = s.get(WorkflowRun, run_id)
            if run is None:
                return
            for key, value in fields.items():
                setattr(run, key, value)
            run.updated_at = utcnow()

        if session is not None:
            _apply(session)
        else:
            with self.Session.begin() as s:
                _apply(s)

    # ----------------------------------------------------------------- steps
    def get_steps(self, run_id: str, *, session: Session | None = None) -> list[WorkflowStep]:
        def _query(s: Session) -> list[WorkflowStep]:
            return list(
                s.scalars(
                    select(WorkflowStep).where(WorkflowStep.run_id == run_id).order_by(WorkflowStep.seq)
                )
            )

        if session is not None:
            return _query(session)
        with self.Session() as s:
            return _query(s)

    def create_step(
        self,
        run_id: str,
        seq: int,
        *,
        kind: str,
        name: str,
        input: dict | None,
        status: str = "SCHEDULED",
        result: Any = None,
        session: Session | None = None,
    ) -> None:
        def _apply(s: Session) -> None:
            if s.get(WorkflowStep, (run_id, seq)) is not None:
                return  # idempotent: this command was already scheduled on a prior tick
            s.add(
                WorkflowStep(
                    run_id=run_id, seq=seq, kind=kind, name=name, input=input,
                    status=status, result=result,
                    completed_at=utcnow() if status == "COMPLETED" else None,
                )
            )

        if session is not None:
            _apply(session)
        else:
            with self.Session.begin() as s:
                _apply(s)

    def complete_step(
        self,
        run_id: str,
        seq: int,
        *,
        status: str,
        result: Any = None,
        error: Any = None,
        attempt: int = 0,
    ) -> None:
        with self.Session.begin() as s:
            step = s.get(WorkflowStep, (run_id, seq))
            if step is None:
                return
            step.status = status
            step.result = result
            step.error = error
            step.attempt = attempt
            step.completed_at = utcnow()

    # ---------------------------------------------------------------- timers
    def create_timer(
        self,
        run_id: str,
        seq: int,
        *,
        fire_at: datetime,
        session: Session | None = None,
    ) -> None:
        def _apply(s: Session) -> None:
            if s.get(WorkflowTimer, (run_id, seq)) is not None:
                return  # idempotent: already scheduled on a prior tick
            s.add(WorkflowTimer(run_id=run_id, seq=seq, fire_at=fire_at))

        if session is not None:
            _apply(session)
        else:
            with self.Session.begin() as s:
                _apply(s)

    def fire_due_timers(self, *, now: datetime | None = None, limit: int = 100) -> list[str]:
        """Deliver every timer whose deadline has elapsed.

        In one transaction, marks each due timer fired and flips its TIMER step to
        COMPLETED. The ``fired_at IS NULL`` guard makes this exactly-once. Returns
        the run ids that advanced, so the caller can request a tick for each.
        """
        now = now or utcnow()
        fired_runs: list[str] = []
        with self.Session.begin() as s:
            timers = s.scalars(
                select(WorkflowTimer)
                .where(WorkflowTimer.fired_at.is_(None), WorkflowTimer.fire_at <= now)
                .order_by(WorkflowTimer.fire_at)
                .limit(limit)
            ).all()
            for timer in timers:
                timer.fired_at = now
                step = s.get(WorkflowStep, (timer.run_id, timer.seq))
                if step is not None and step.status == "SCHEDULED":
                    step.status = "COMPLETED"
                    step.result = {"value": None}
                    step.completed_at = now
                fired_runs.append(timer.run_id)
        return fired_runs

    # -------------------------------------------------------------- recovery
    def find_stalled_runs(self, *, older_than: datetime, limit: int = 100) -> list[str]:
        """Return ids of non-terminal runs untouched since ``older_than``.

        A run rests in PENDING or SUSPENDED between ticks; if the tick that should
        have advanced it was lost (the worker died after committing a step but
        before its re-tick was processed), nothing else will move it. The recovery
        scanner re-ticks these — safe because replay is idempotent.
        """
        with self.Session() as s:
            return list(
                s.scalars(
                    select(WorkflowRun.id)
                    .where(
                        WorkflowRun.status.in_(("PENDING", "SUSPENDED")),
                        WorkflowRun.updated_at <= older_than,
                    )
                    .order_by(WorkflowRun.updated_at)
                    .limit(limit)
                )
            )

    # --------------------------------------------------------------- signals
    def add_signal(
        self,
        run_id: str,
        name: str,
        payload: Any,
        *,
        session: Session | None = None,
    ) -> None:
        def _apply(s: Session) -> None:
            s.add(WorkflowSignal(run_id=run_id, name=name, payload=payload))

        if session is not None:
            _apply(session)
        else:
            with self.Session.begin() as s:
                _apply(s)

    def match_signals(self, run_id: str, *, session: Session) -> int:
        """Pair queued signals with waiting steps, FIFO within each name.

        For every SCHEDULED SIGNAL_WAIT step (oldest seq first) with an unconsumed
        signal of the same name (oldest first), completes the step with the signal's
        payload and stamps ``consumed_seq``. Returns how many waits were satisfied;
        the caller re-ticks the run when that is non-zero.
        """
        waits = list(
            session.scalars(
                select(WorkflowStep)
                .where(
                    WorkflowStep.run_id == run_id,
                    WorkflowStep.kind == "SIGNAL_WAIT",
                    WorkflowStep.status == "SCHEDULED",
                )
                .order_by(WorkflowStep.seq)
            )
        )
        signals = list(
            session.scalars(
                select(WorkflowSignal)
                .where(WorkflowSignal.run_id == run_id, WorkflowSignal.consumed_seq.is_(None))
                .order_by(WorkflowSignal.id)
            )
        )

        matched = 0
        for wait in waits:
            signal = next((sig for sig in signals if sig.name == wait.name and sig.consumed_seq is None), None)
            if signal is None:
                continue  # nothing for this name yet; leave it waiting
            signal.consumed_seq = wait.seq
            wait.status = "COMPLETED"
            wait.result = {"value": signal.payload}
            wait.completed_at = utcnow()
            matched += 1
        return matched

    # ------------------------------------------------------------------ lock
    @contextmanager
    def locked_run(self, run_id: str) -> Iterator[Session]:
        """Hold an exclusive lock on ``run_id`` for the duration of one tick.

        Yields a session whose transaction is committed on clean exit and rolled
        back on error.
        """
        if self.is_postgres:
            with self.Session() as session:
                session.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": _advisory_key(run_id)})
                try:
                    yield session
                    session.commit()
                except Exception:
                    session.rollback()
                    raise
        else:
            lock = self._get_local_lock(run_id)
            lock.acquire()
            try:
                with self.Session() as session:
                    try:
                        yield session
                        session.commit()
                    except Exception:
                        session.rollback()
                        raise
            finally:
                lock.release()

    def _get_local_lock(self, run_id: str) -> threading.Lock:
        with self._local_guard:
            lock = self._local_locks.get(run_id)
            if lock is None:
                lock = threading.Lock()
                self._local_locks[run_id] = lock
            return lock
