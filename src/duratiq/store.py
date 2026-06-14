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
from typing import Any, Callable

from sqlalchemy import Engine as SaEngine
from sqlalchemy import create_engine, delete, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from .models import (
    Base,
    WorkflowDedup,
    WorkflowRun,
    WorkflowSchedule,
    WorkflowSignal,
    WorkflowStep,
    WorkflowTimer,
    utcnow,
)


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

    @staticmethod
    def _runs_filter(query, status: Any, name: str | None):
        if status is not None:
            statuses = [status] if isinstance(status, str) else list(status)
            query = query.where(WorkflowRun.status.in_(statuses))
        if name is not None:
            query = query.where(WorkflowRun.name == name)
        return query

    def list_runs(
        self,
        *,
        status: "str | list[str] | None" = None,
        name: str | None = None,
        limit: int = 50,
        offset: int = 0,
        newest_first: bool = True,
    ) -> list[WorkflowRun]:
        """Return runs matching the filters, newest first by default.

        ``status`` may be a single status or a list. Ordered by ``created_at`` (then
        ``id`` for a stable tiebreak), paginated by ``limit``/``offset``.
        """
        order_col = WorkflowRun.created_at.desc() if newest_first else WorkflowRun.created_at.asc()
        id_col = WorkflowRun.id.desc() if newest_first else WorkflowRun.id.asc()
        with self.Session() as s:
            query = self._runs_filter(select(WorkflowRun), status, name)
            query = query.order_by(order_col, id_col).limit(limit).offset(offset)
            return list(s.scalars(query))

    def count_runs(self, *, status: "str | list[str] | None" = None, name: str | None = None) -> int:
        """Count runs matching the same filters as :meth:`list_runs` (ignores paging)."""
        from sqlalchemy import func

        with self.Session() as s:
            query = self._runs_filter(select(func.count()).select_from(WorkflowRun), status, name)
            return int(s.scalar(query) or 0)

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

    def continue_as_new(self, run_id: str, *, new_input: dict, session: Session) -> None:
        """Truncate a run's history and restart it with fresh input (same run id).

        Deletes every step, every timer, and every *consumed* signal, then resets the
        run to PENDING with ``new_input`` and a cleared result/error — a clean slate
        for the next iteration. Unconsumed signals are intentionally left in place so
        they carry over and are matched by the new iteration's waits. Runs inside the
        caller's locked-tick transaction, so the truncate + reset is atomic.
        """
        session.execute(delete(WorkflowStep).where(WorkflowStep.run_id == run_id))
        session.execute(delete(WorkflowTimer).where(WorkflowTimer.run_id == run_id))
        session.execute(
            delete(WorkflowSignal).where(WorkflowSignal.run_id == run_id, WorkflowSignal.consumed_seq.is_not(None))
        )
        run = session.get(WorkflowRun, run_id)
        if run is not None:
            run.input = new_input
            run.status = "PENDING"
            run.result = None
            run.error = None
            run.updated_at = utcnow()

    # ----------------------------------------------------------------- steps
    def get_steps(self, run_id: str, *, session: Session | None = None) -> list[WorkflowStep]:
        def _query(s: Session) -> list[WorkflowStep]:
            return list(s.scalars(select(WorkflowStep).where(WorkflowStep.run_id == run_id).order_by(WorkflowStep.seq)))

        if session is not None:
            return _query(session)
        with self.Session() as s:
            return _query(s)

    def get_step(self, run_id: str, seq: int, *, session: Session | None = None) -> WorkflowStep | None:
        if session is not None:
            return session.get(WorkflowStep, (run_id, seq))
        with self.Session() as s:
            return s.get(WorkflowStep, (run_id, seq))

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
        timeout_at: datetime | None = None,
        session: Session | None = None,
    ) -> None:
        def _apply(s: Session) -> None:
            if s.get(WorkflowStep, (run_id, seq)) is not None:
                return  # idempotent: this command was already scheduled on a prior tick
            s.add(
                WorkflowStep(
                    run_id=run_id,
                    seq=seq,
                    kind=kind,
                    name=name,
                    input=input,
                    status=status,
                    result=result,
                    timeout_at=timeout_at,
                    completed_at=utcnow() if status == "COMPLETED" else None,
                )
            )

        if session is not None:
            _apply(session)
        else:
            with self.Session.begin() as s:
                _apply(s)

    def get_step(self, run_id: str, seq: int, *, session: Session | None = None) -> WorkflowStep | None:
        if session is not None:
            return session.get(WorkflowStep, (run_id, seq))
        with self.Session() as s:
            return s.get(WorkflowStep, (run_id, seq))

    def complete_step(
        self,
        run_id: str,
        seq: int,
        *,
        status: str,
        result: Any = None,
        error: Any = None,
        attempt: int = 0,
        session: Session | None = None,
    ) -> None:
        def _apply(s: Session) -> None:
            step = s.get(WorkflowStep, (run_id, seq))
            if step is None:
                return
            step.status = status
            step.result = result
            step.error = error
            step.attempt = attempt
            step.completed_at = utcnow()

        if session is not None:
            _apply(session)
        else:
            with self.Session.begin() as s:
                _apply(s)

    def find_due_activity_timeouts(self, *, now: datetime, limit: int = 100) -> list[tuple[str, int]]:
        """``(run_id, seq)`` of SCHEDULED activity steps whose start-to-close deadline
        has elapsed — i.e. dispatched but not reported back in time. The engine claims
        each under the run lock (re-checking the deadline), so a result that lands
        between the scan and the claim wins the race."""
        with self.Session() as s:
            rows = s.execute(
                select(WorkflowStep.run_id, WorkflowStep.seq)
                .where(
                    WorkflowStep.kind == "ACTIVITY",
                    WorkflowStep.status == "SCHEDULED",
                    WorkflowStep.timeout_at.is_not(None),
                    WorkflowStep.timeout_at <= now,
                )
                .order_by(WorkflowStep.timeout_at)
                .limit(limit)
            ).all()
            return [(run_id, seq) for run_id, seq in rows]

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

    def cancel_timer(self, run_id: str, seq: int, *, session: Session) -> None:
        """Cancel a still-pending timer (the losing side of a won ``wait_signal`` race).

        Marks the TIMER step CANCELLED and removes its due-time row so the timer
        scanner won't fire it. A no-op if the timer already fired."""
        step = session.get(WorkflowStep, (run_id, seq))
        if step is not None and step.status == "SCHEDULED":
            step.status = "CANCELLED"
            step.completed_at = utcnow()
        timer = session.get(WorkflowTimer, (run_id, seq))
        if timer is not None:
            session.delete(timer)

    def cancel_wait(self, run_id: str, seq: int, *, session: Session) -> None:
        """Cancel a still-pending signal wait (abandoned after its timeout fired).

        Marks the SIGNAL_WAIT step CANCELLED so ``match_signals`` — which only pairs
        SCHEDULED waits — leaves a late signal queued for a later wait instead of
        silently consuming it here."""
        step = session.get(WorkflowStep, (run_id, seq))
        if step is not None and step.status == "SCHEDULED":
            step.status = "CANCELLED"
            step.completed_at = utcnow()

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

    # ----------------------------------------------------------------- dedup
    def get_dedup(self, key: str) -> WorkflowDedup | None:
        with self.Session() as s:
            return s.get(WorkflowDedup, key)

    def put_dedup(self, *, key: str, run_id: str, seq: int, result: Any) -> bool:
        """Record an effect under ``key``. Returns ``False`` if one already existed.

        Insert-if-absent: a concurrent writer that wins the race keeps its row (the
        unique key makes the loser's insert a no-op), so the stored result is stable.
        """
        try:
            with self.Session.begin() as s:
                s.add(WorkflowDedup(key=key, run_id=run_id, seq=seq, result=result))
            return True
        except IntegrityError:
            return False

    # ------------------------------------------------------------- schedules
    def create_schedule(self, *, id: str, name: str, cron: str, input: dict, next_fire_at: datetime) -> bool:
        """Insert a recurring schedule. Idempotent on ``id``: returns ``False`` (and
        leaves the existing row untouched) if one with this id already exists."""
        with self.Session.begin() as s:
            if s.get(WorkflowSchedule, id) is not None:
                return False
            s.add(WorkflowSchedule(id=id, name=name, cron=cron, input=input, active=True, next_fire_at=next_fire_at))
        return True

    def get_schedule(self, schedule_id: str) -> WorkflowSchedule | None:
        with self.Session() as s:
            return s.get(WorkflowSchedule, schedule_id)

    def set_schedule_active(self, schedule_id: str, active: bool) -> bool:
        with self.Session.begin() as s:
            sch = s.get(WorkflowSchedule, schedule_id)
            if sch is None:
                return False
            sch.active = active
            sch.updated_at = utcnow()
        return True

    def delete_schedule(self, schedule_id: str) -> bool:
        with self.Session.begin() as s:
            sch = s.get(WorkflowSchedule, schedule_id)
            if sch is None:
                return False
            s.delete(sch)
        return True

    def claim_due_schedules(
        self, *, now: datetime, limit: int, compute_next: Callable[[str, datetime], datetime]
    ) -> list[tuple[str, str, dict]]:
        """Claim every active schedule whose ``next_fire_at`` has elapsed.

        In one transaction, advances each due schedule's ``next_fire_at`` to its next
        cron time (via ``compute_next(cron, now)``) and stamps ``last_fired_at`` —
        *claiming* it so a concurrent scan won't fire it again. Returns ``(id, name,
        input)`` for each, so the caller can start the runs after the claim commits.
        On Postgres the rows are locked ``FOR UPDATE SKIP LOCKED`` to make concurrent
        scanners safe; missed ticks are skipped rather than backfilled.
        """
        claimed: list[tuple[str, str, dict]] = []
        with self.Session.begin() as s:
            query = (
                select(WorkflowSchedule)
                .where(WorkflowSchedule.active.is_(True), WorkflowSchedule.next_fire_at <= now)
                .order_by(WorkflowSchedule.next_fire_at)
                .limit(limit)
            )
            if self.is_postgres:
                query = query.with_for_update(skip_locked=True)
            for sch in s.scalars(query).all():
                claimed.append((sch.id, sch.name, dict(sch.input or {})))
                sch.last_fired_at = now
                sch.next_fire_at = compute_next(sch.cron, now)
        return claimed

    def set_schedule_last_run(self, schedule_id: str, run_id: str) -> None:
        with self.Session.begin() as s:
            sch = s.get(WorkflowSchedule, schedule_id)
            if sch is not None:
                sch.last_run_id = run_id

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
