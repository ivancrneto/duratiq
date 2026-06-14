"""The durable-execution engine: replay-from-top with DB-memoized steps.

A workflow run advances one *tick* at a time. Each tick replays the orchestrator
from the start; recorded steps return their memoized results, and the first
not-ready point raises :class:`Suspend`, which releases the worker. A tick is
re-requested whenever an activity completes, a timer fires, or a signal arrives,
driving the run forward until it returns.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Protocol
from uuid import uuid4

from . import events
from .context import WorkflowContext
from .events import Listener, WorkflowEvent
from .exceptions import ActivityFailed, ChildWorkflowFailed, Suspend
from .models import WorkflowRun, utcnow
from .registry import Registry
from .store import SqlStore

_TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}


class Driver(Protocol):
    """Transport that carries ticks and activity dispatches. See ``drivers/``."""

    def request_tick(self, run_id: str) -> None: ...

    def dispatch_activity(
        self, run_id: str, seq: int, name: str, args: list, kwargs: dict, max_retries: int
    ) -> None: ...


class Engine:
    def __init__(
        self,
        registry: Registry,
        store: SqlStore,
        driver: Driver | None = None,
        *,
        listener: Listener | None = None,
    ) -> None:
        self.registry = registry
        self.store = store
        self.driver = driver
        # Optional observability hook; see duratiq.events. Best-effort — never lets a
        # listener exception affect execution.
        self.listener = listener

    def _emit(self, type: str, run_id: str, **fields: Any) -> None:
        if self.listener is None:
            return
        try:
            self.listener(WorkflowEvent(type=type, run_id=run_id, **fields))
        except Exception:  # noqa: BLE001 - observability must never break a run
            pass

    # --------------------------------------------------------------- client
    def start(self, name: str, *, idempotency_key: str | None = None, **kwargs: Any) -> str:
        wf = self.registry.get_workflow(name)  # validate name early
        if idempotency_key:
            existing = self.store.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                return existing.id
        run_id = uuid4().hex
        self.store.create_run(
            run_id=run_id,
            name=name,
            version=wf.version,
            input=kwargs,
            idempotency_key=idempotency_key,
        )
        self._emit(events.RUN_STARTED, run_id, name=name)
        self.driver.request_tick(run_id)
        return run_id

    def get(self, run_id: str) -> WorkflowRun | None:
        return self.store.get_run(run_id)

    # ----------------------------------------------------------------- core
    def tick(self, run_id: str) -> None:
        scheduled: list = []
        children: list = []
        matched = 0
        run_name: str | None = None
        outcome: tuple | None = None  # (event_type, result, error) to emit post-commit
        parent_notify: tuple | None = None  # (parent_run_id, parent_seq, status, result, error)
        with self.store.locked_run(run_id) as session:
            run = self.store.get_run(run_id, session=session)
            if run is None or run.status in _TERMINAL:
                return

            run_name = run.name
            wf = self.registry.get_workflow(run.name)
            steps = self.store.get_steps(run_id, session=session)
            ctx = WorkflowContext(run_id, steps)

            terminal_status: str | None = None
            terminal_result: Any = None
            terminal_error: dict | None = None
            try:
                result = wf.fn(ctx, **(run.input or {}))
            except Suspend:
                self.store.update_run(run_id, session=session, status="SUSPENDED")
                outcome = (events.RUN_SUSPENDED, None, None)
            except (ActivityFailed, ChildWorkflowFailed) as exc:
                terminal_status, terminal_error = "FAILED", {"type": type(exc).__name__, "message": str(exc)}
                self.store.update_run(run_id, session=session, status="FAILED", error=terminal_error)
                outcome = (events.RUN_FAILED, None, terminal_error)
            except Exception as exc:  # noqa: BLE001 - workflow code may raise anything
                terminal_status, terminal_error = "FAILED", {"type": type(exc).__name__, "message": str(exc)}
                self.store.update_run(run_id, session=session, status="FAILED", error=terminal_error)
                outcome = (events.RUN_FAILED, None, terminal_error)
            else:
                terminal_status, terminal_result = "COMPLETED", {"value": result}
                self.store.update_run(run_id, session=session, status="COMPLETED", result=terminal_result)
                outcome = (events.RUN_COMPLETED, result, None)

            # Record any newly-scheduled activities inside the same transaction.
            for sa in ctx.scheduled:
                self.store.create_step(
                    run_id,
                    sa.seq,
                    kind="ACTIVITY",
                    name=sa.name,
                    input={"args": sa.args, "kwargs": sa.kwargs},
                    status="SCHEDULED",
                    session=session,
                )
            scheduled = list(ctx.scheduled)

            # Record any newly-scheduled timers: a TIMER step plus a due-time index
            # row. fire_at is computed here (not in workflow code) and persisted, so
            # the deadline is fixed across replays and survives a crash.
            for st in ctx.scheduled_timers:
                self.store.create_step(
                    run_id,
                    st.seq,
                    kind="TIMER",
                    name="sleep",
                    input={"delay_seconds": st.delay_seconds},
                    status="SCHEDULED",
                    session=session,
                )
                self.store.create_timer(
                    run_id,
                    st.seq,
                    fire_at=utcnow() + timedelta(seconds=st.delay_seconds),
                    session=session,
                )

            # Record newly-registered signal waits, then pair any already-queued
            # signal so a signal that arrived before its wait is consumed at once.
            for sw in ctx.scheduled_waits:
                self.store.create_step(
                    run_id,
                    sw.seq,
                    kind="SIGNAL_WAIT",
                    name=sw.name,
                    input={"name": sw.name},
                    status="SCHEDULED",
                    session=session,
                )
            if ctx.scheduled_waits:
                matched = self.store.match_signals(run_id, session=session)

            # Record side-effect values computed during the replay. They are born
            # COMPLETED — the value was produced in this tick, not awaited — and
            # commit atomically with everything else so replay reuses them verbatim.
            for se in ctx.scheduled_side_effects:
                self.store.create_step(
                    run_id,
                    se.seq,
                    kind="SIDE_EFFECT",
                    name="side_effect",
                    input=None,
                    status="COMPLETED",
                    result={"value": se.value},
                    session=session,
                )

            # Record newly-scheduled child workflows. The sub-run itself is started
            # post-commit (like an activity dispatch), so we never spawn a child for
            # a step that got rolled back.
            for sc in ctx.scheduled_children:
                self.store.create_step(
                    run_id,
                    sc.seq,
                    kind="CHILD_WORKFLOW",
                    name=sc.name,
                    input={"input": sc.input},
                    status="SCHEDULED",
                    session=session,
                )
            children = list(ctx.scheduled_children)

            # If this run is itself a child and just reached a terminal state, queue a
            # notification so its parent's CHILD_WORKFLOW step resolves and the parent
            # advances (done post-commit, outside this run's lock).
            if terminal_status is not None and run.parent_run_id is not None:
                parent_notify = (run.parent_run_id, run.parent_seq, terminal_status, terminal_result, terminal_error)

        # Dispatch only after the tick transaction has committed, so we never put a
        # message on the broker for a step that got rolled back.
        for sa in scheduled:
            self.driver.dispatch_activity(run_id, sa.seq, sa.name, sa.args, sa.kwargs, sa.max_retries)
        # Start child workflows after commit. Idempotent: a re-tick that re-runs this
        # for an already-started child finds the existing run and just re-ticks it.
        for sc in children:
            self._start_child(run_id, sc.seq, sc.name, sc.input)
        # A queued signal was consumed during this tick — replay again to advance.
        if matched:
            self.driver.request_tick(run_id)
        # This run finished and has a parent waiting on it — resolve the parent's step.
        if parent_notify is not None:
            self._notify_parent(*parent_notify)

        # Emit observability events after the transaction commits, so listeners only
        # ever see committed state.
        for sa in scheduled:
            self._emit(events.ACTIVITY_SCHEDULED, run_id, name=sa.name, seq=sa.seq)
        if outcome is not None:
            event_type, result_value, error = outcome
            self._emit(event_type, run_id, name=run_name, result=result_value, error=error)

    def report_activity_result(
        self, run_id: str, seq: int, result: Any, error: BaseException | None, *, attempt: int = 0
    ) -> None:
        if error is None:
            self.store.complete_step(run_id, seq, status="COMPLETED", result={"value": result}, attempt=attempt)
            self._emit(events.ACTIVITY_COMPLETED, run_id, seq=seq, attempt=attempt)
        else:
            err = {"type": type(error).__name__, "message": str(error)}
            self.store.complete_step(run_id, seq, status="FAILED", error=err, attempt=attempt)
            self._emit(events.ACTIVITY_FAILED, run_id, seq=seq, attempt=attempt, error=err)
        self.driver.request_tick(run_id)

    def _start_child(self, parent_run_id: str, parent_seq: int, name: str, input: dict) -> None:
        """Start (or re-tick) the child run for a parent's CHILD_WORKFLOW step.

        Idempotent on ``(parent_run_id, parent_seq)``: if the child already exists —
        because a crash re-ran the post-commit dispatch — it is re-ticked rather than
        duplicated.
        """
        existing = self.store.find_child_run(parent_run_id, parent_seq)
        if existing is not None:
            self.driver.request_tick(existing.id)
            return
        wf = self.registry.get_workflow(name)  # validate before creating the sub-run
        child_id = uuid4().hex
        self.store.create_run(
            run_id=child_id,
            name=name,
            version=wf.version,
            input=input,
            parent_run_id=parent_run_id,
            parent_seq=parent_seq,
        )
        self.driver.request_tick(child_id)

    def _notify_parent(self, parent_run_id: str, parent_seq: int, status: str, result: Any, error: dict | None) -> None:
        """Resolve a parent's CHILD_WORKFLOW step from a finished child and re-tick it.

        Mirrors :meth:`report_activity_result`: an atomic step update followed by a
        re-tick, no parent lock needed (the re-tick replays under the parent's own
        lock). A COMPLETED child carries its result; a FAILED/CANCELLED child records
        FAILED so the parent raises :class:`ChildWorkflowFailed` on replay.
        """
        if status == "COMPLETED":
            self.store.complete_step(parent_run_id, parent_seq, status="COMPLETED", result=result)
        else:
            self.store.complete_step(parent_run_id, parent_seq, status="FAILED", error=error)
        self.driver.request_tick(parent_run_id)

    def fire_due_timers(self, *, now: datetime | None = None, limit: int = 100) -> int:
        """Deliver elapsed ``ctx.sleep`` timers and re-tick the runs they unblock.

        This is the timer-scanner body: call it periodically (cron/``periodiq``).
        Passing ``now`` lets tests fast-forward without sleeping. Returns the number
        of runs advanced.
        """
        run_ids = self.store.fire_due_timers(now=now, limit=limit)
        for run_id in run_ids:
            self.driver.request_tick(run_id)
        return len(run_ids)

    def recover_stalled(self, *, older_than_seconds: float = 60, now: datetime | None = None, limit: int = 100) -> int:
        """Re-tick non-terminal runs that have been idle longer than the threshold.

        This is the recovery-scanner body: call it periodically (cron/``periodiq``).
        It backstops *lost ticks* — a timer fired or signal matched, but the worker
        died before its re-tick ran — by re-ticking stale runs; replay is idempotent
        so a genuinely-waiting run just re-suspends. (Lost *activity* messages are
        recovered by the broker's own redelivery, not here.) The threshold keeps the
        scan from racing runs that are actively progressing. Returns runs re-ticked.
        """
        cutoff = (now or utcnow()) - timedelta(seconds=older_than_seconds)
        run_ids = self.store.find_stalled_runs(older_than=cutoff, limit=limit)
        for run_id in run_ids:
            self.driver.request_tick(run_id)
        return len(run_ids)

    def signal(self, run_id: str, name: str, payload: Any = None) -> bool:
        """Deliver a signal to a run, waking any matching ``ctx.wait_signal``.

        The signal is stored even if no wait is outstanding yet — a later
        ``wait_signal(name)`` will consume it FIFO. Returns ``False`` if the run is
        missing or already terminal.
        """
        with self.store.locked_run(run_id) as session:
            run = self.store.get_run(run_id, session=session)
            if run is None or run.status in _TERMINAL:
                return False
            self.store.add_signal(run_id, name, payload, session=session)
            self.store.match_signals(run_id, session=session)
        # Re-tick unconditionally: a matched wait must replay to advance, and an
        # unmatched signal is cheap (the tick is a no-op past the frontier).
        self.driver.request_tick(run_id)
        return True

    # -------------------------------------------------------------- control
    def cancel(self, run_id: str) -> bool:
        """Mark a non-terminal run ``CANCELLED``.

        Returns ``False`` if the run is missing or already terminal. No driver or
        registry needed — ``tick`` already short-circuits on a cancelled run.
        """
        run_name: str | None = None
        parent_notify: tuple | None = None
        with self.store.locked_run(run_id) as session:
            run = self.store.get_run(run_id, session=session)
            if run is None or run.status in _TERMINAL:
                return False
            run_name = run.name
            self.store.update_run(run_id, session=session, status="CANCELLED")
            if run.parent_run_id is not None:
                error = {"type": "ChildWorkflowCancelled", "message": f"child workflow {run.name!r} was cancelled"}
                parent_notify = (run.parent_run_id, run.parent_seq, "CANCELLED", None, error)
        self._emit(events.RUN_CANCELLED, run_id, name=run_name)
        # A cancelled child resolves its parent's step as FAILED so the parent does
        # not wait forever. (Cancelling a parent does not yet cascade to children.)
        if parent_notify is not None:
            self._notify_parent(*parent_notify)
        return True

    def retry(self, run_id: str) -> bool:
        """Re-arm a ``FAILED`` run and request a fresh tick.

        Drops the failed step(s) so they reschedule on the next replay, resets the
        run to ``PENDING``, and clears the recorded error. Returns ``False`` if the
        run is missing or not ``FAILED``. Requires a driver to actually resume.
        """
        with self.store.locked_run(run_id) as session:
            run = self.store.get_run(run_id, session=session)
            if run is None or run.status != "FAILED":
                return False
            for step in self.store.get_steps(run_id, session=session):
                if step.status == "FAILED":
                    session.delete(step)
            self.store.update_run(run_id, session=session, status="PENDING", error=None)
        # Request the tick only after the reset has committed.
        if self.driver is not None:
            self.driver.request_tick(run_id)
        return True
