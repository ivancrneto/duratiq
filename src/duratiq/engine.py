"""The durable-execution engine: replay-from-top with DB-memoized steps.

A workflow run advances one *tick* at a time. Each tick replays the orchestrator
from the start; recorded steps return their memoized results, and the first
not-ready point raises :class:`Suspend`, which releases the worker. A tick is
re-requested whenever an activity completes, a timer fires, or a signal arrives,
driving the run forward until it returns.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Protocol
from uuid import uuid4

from . import events
from .context import WorkflowContext
from .cron import parse_cron
from .events import Listener, WorkflowEvent
from .exceptions import (
    ActivityFailed,
    ChildWorkflowFailed,
    ContinueAsNew,
    QueryNotFound,
    Suspend,
    UpdateFailed,
    WorkflowNotFound,
)
from .models import WorkflowRun, utcnow
from .registry import Registry
from .store import SqlStore

_TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}


class _UpdatePending:
    """Sentinel from ``engine.get_update_result`` while an update is still being applied
    (the run hasn't ticked past its ``wait_update`` yet). Test with ``is UPDATE_PENDING``."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "duratiq.UPDATE_PENDING"


UPDATE_PENDING = _UpdatePending()


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
    def start(
        self,
        name: str,
        *,
        idempotency_key: str | None = None,
        search_attributes: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> str:
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
        if search_attributes:
            self.store.upsert_search_attributes(run_id, search_attributes)
        self._emit(events.RUN_STARTED, run_id, name=name)
        self.driver.request_tick(run_id)
        return run_id

    def signal_with_start(
        self, name: str, *, signal: str, payload: Any = None, idempotency_key: str | None = None, **kwargs: Any
    ) -> str:
        """Deliver a signal to a run, starting it first if it does not exist yet.

        The classic Temporal "signal-with-start": dedupe on ``idempotency_key`` —
        if a run already exists, just signal it; otherwise start a fresh run and
        deliver the signal so it is waiting in the inbox before the first tick. The
        run's ``ctx.wait_signal(signal)`` then consumes it immediately, with no race
        against the start. Returns the run id (existing or new).

        Use it for "ensure a per-entity workflow is running, then nudge it" — e.g. a
        per-customer cart workflow that you signal on every add-to-cart, starting it
        on the first one.
        """
        wf = self.registry.get_workflow(name)  # validate name early
        if idempotency_key:
            existing = self.store.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                self.signal(existing.id, signal, payload)
                return existing.id
        run_id = uuid4().hex
        self.store.create_run(
            run_id=run_id,
            name=name,
            version=wf.version,
            input=kwargs,
            idempotency_key=idempotency_key,
        )
        # Queue the signal before the first tick so the inbox already holds it when
        # the run reaches its wait — matched FIFO, exactly like a signal that races
        # ahead of its wait normally.
        self.store.add_signal(run_id, signal, payload)
        self.driver.request_tick(run_id)
        return run_id

    def get(self, run_id: str) -> WorkflowRun | None:
        return self.store.get_run(run_id)

    def list_runs(
        self,
        *,
        status: "str | list[str] | None" = None,
        name: str | None = None,
        search_attributes: dict[str, Any] | None = None,
        limit: int = 50,
        offset: int = 0,
        newest_first: bool = True,
    ) -> list[WorkflowRun]:
        """List runs, newest first, optionally filtered by status, name, and attributes.

        ``status`` accepts a single status (``"RUNNING"``) or a list
        (``["FAILED", "CANCELLED"]``). ``search_attributes`` is an AND of equality
        matches (``{"region": "eu", "priority": 1}``). ``limit`` is clamped to
        ``[1, 1000]``; page with ``offset``. Pair with :meth:`count_runs` for a total.
        This is the read side an admin/ops view is built on.
        """
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)
        return self.store.list_runs(
            status=status,
            name=name,
            search_attributes=search_attributes,
            limit=limit,
            offset=offset,
            newest_first=newest_first,
        )

    def count_runs(
        self,
        *,
        status: "str | list[str] | None" = None,
        name: str | None = None,
        search_attributes: dict[str, Any] | None = None,
    ) -> int:
        """Total runs matching the filters (the unpaginated count behind ``list_runs``)."""
        return self.store.count_runs(status=status, name=name, search_attributes=search_attributes)

    def get_search_attributes(self, run_id: str) -> dict:
        """Return a run's search attributes as a ``{key: value}`` dict (empty if none)."""
        return self.store.get_search_attributes(run_id)

    def query(self, run_id: str, name: str, *args: Any, **kwargs: Any) -> Any:
        """Read a running (or finished) workflow's computed state, without advancing it.

        Replays the workflow **read-only** — completed steps return their memoized
        results and the replay stops at the frontier (or where the run ended), so
        nothing is scheduled, committed, or dispatched — then calls the handler the
        workflow registered with :meth:`WorkflowContext.set_query_handler`. The handler
        is typically a closure over the workflow's locals, so it sees every step
        processed so far. ``*args``/``**kwargs`` are passed through to it.

        Raises :class:`WorkflowNotFound` if the run is unknown and
        :class:`QueryNotFound` if no handler by that name was registered.
        """
        run = self.store.get_run(run_id)
        if run is None:
            raise WorkflowNotFound(f"run {run_id!r} not found")
        ctx = self._replay_readonly(run_id, run)
        handler = ctx.query_handlers.get(name)
        if handler is None:
            raise QueryNotFound(name, list(ctx.query_handlers))
        return handler(*args, **kwargs)

    def _replay_readonly(self, run_id: str, run: WorkflowRun) -> WorkflowContext:
        """Replay the workflow side-effect-free and return its context.

        Used by query/update to register handlers and rebuild state without advancing
        the run: memoized steps return their results and the replay stops at the
        frontier or where the run ended (Suspend / a terminal failure / continue-as-new
        are all just stopping points — handlers set before that point are available).
        Nothing is scheduled, committed, or dispatched.
        """
        wf = self.registry.get_workflow(run.name)
        ctx = WorkflowContext(run_id, self.store.get_steps(run_id))
        try:
            wf.fn(ctx, **(run.input or {}))
        except (Suspend, ActivityFailed, ChildWorkflowFailed, ContinueAsNew):
            pass
        return ctx

    def update(self, run_id: str, name: str, *args: Any, **kwargs: Any) -> str:
        """Deliver a synchronous, mutating update to a running workflow.

        Unlike a signal (fire-and-forget) an update carries a **response**. If the
        workflow registered a validator for ``name`` it runs first, read-only — if it
        raises, the update is **rejected** and nothing is recorded (validate before
        mutate). Otherwise the update is queued and the run re-ticked; the workflow
        consumes it at a :meth:`WorkflowContext.wait_update` point, runs the registered
        handler, and the result is recorded for :meth:`get_update_result`.

        Returns the update id. Raises :class:`WorkflowNotFound` for an unknown run and
        ``ValueError`` if the run is already terminal. Like the rest of duratiq the tick
        is asynchronous: with a broker the result lands once a worker processes it; read
        it back with :meth:`get_update_result`.
        """
        run = self.store.get_run(run_id)
        if run is None:
            raise WorkflowNotFound(f"run {run_id!r} not found")
        if run.status in _TERMINAL:
            raise ValueError(f"run {run_id!r} is {run.status}; cannot accept updates")
        validator = self._replay_readonly(run_id, run).update_validators.get(name)
        if validator is not None:
            validator(*args, **kwargs)  # raises to reject — propagated to the caller
        update_id = uuid4().hex
        # Queue the update and pair it with any already-waiting step in one transaction
        # (mirrors engine.signal); the tick then replays past the now-completed wait and
        # runs the handler. An update that arrives before the first wait stays PENDING
        # and is matched by the tick that first reaches wait_update.
        with self.store.locked_run(run_id) as session:
            self.store.add_update(run_id, update_id, name, {"args": list(args), "kwargs": kwargs}, session=session)
            self.store.match_updates(run_id, session=session)
        self.driver.request_tick(run_id)
        return update_id

    def get_update_result(self, run_id: str, update_id: str) -> Any:
        """Return a finished update's result, or :data:`UPDATE_PENDING` if not yet applied.

        Raises :class:`UpdateFailed` if the handler raised, and :class:`WorkflowNotFound`
        if the update id is unknown.
        """
        update = self.store.get_update(update_id)
        if update is None or update.run_id != run_id:
            raise WorkflowNotFound(f"update {update_id!r} not found for run {run_id!r}")
        if update.status == "PENDING":
            return UPDATE_PENDING
        if update.status == "FAILED":
            raise UpdateFailed(update.name, update.error)
        return (update.result or {}).get("value")

    # ----------------------------------------------------------------- core
    def tick(self, run_id: str) -> None:
        scheduled: list = []
        children: list = []
        matched = 0
        run_name: str | None = None
        outcome: tuple | None = None  # (event_type, result, error) to emit post-commit
        restart = False
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
            except ContinueAsNew as can:
                # Truncate history and reset the run to PENDING with the new input;
                # the run restarts from seq 0 on the re-tick requested post-commit.
                self.store.continue_as_new(run_id, new_input=can.input, session=session)
                restart = True
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

            # Record any newly-scheduled activities inside the same transaction. An
            # activity with a start-to-close or heartbeat timeout carries a deadline, so
            # the timeout scanner can retry/fail it if it never reports back or beats.
            for sa in ctx.scheduled:
                ms = sa.heartbeat_timeout_ms or sa.start_to_close_ms
                timeout_at = utcnow() + timedelta(milliseconds=ms) if ms else None
                self.store.create_step(
                    run_id,
                    sa.seq,
                    kind="ACTIVITY",
                    name=sa.name,
                    input={"args": sa.args, "kwargs": sa.kwargs},
                    status="SCHEDULED",
                    timeout_at=timeout_at,
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

            # Record newly-registered update waits, then pair any already-queued update
            # so a pending update is consumed at once (mirrors the signal path).
            for uw in ctx.scheduled_update_waits:
                self.store.create_step(
                    run_id,
                    uw.seq,
                    kind="UPDATE_WAIT",
                    name="update",
                    input=None,
                    status="SCHEDULED",
                    session=session,
                )
            if ctx.scheduled_update_waits and self.store.match_updates(run_id, session=session):
                matched += 1
            # Write back each handler outcome applied during this replay (idempotent —
            # the handler re-runs every replay and produces the same result).
            for applied in ctx.applied_updates:
                self.store.record_update_result(
                    applied.update_id, result=applied.result, error=applied.error, session=session
                )

            # Persist any search attributes the workflow upserted this replay (idempotent).
            if ctx.upserted_search_attributes:
                self.store.upsert_search_attributes(run_id, ctx.upserted_search_attributes, session=session)

            # Cancel the losing side of any resolved wait_signal(timeout=...) race so
            # it can't fire/match later: the timer if the signal won, the wait if it
            # timed out. Done in this tick's transaction with the workflow's progress.
            for seq in ctx.cancelled_timers:
                self.store.cancel_timer(run_id, seq, session=session)
            for seq in ctx.cancelled_waits:
                self.store.cancel_wait(run_id, seq, session=session)

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

            # Record patch markers from ctx.patched(). Like side effects they are born
            # COMPLETED — the decision was made in this tick — so replay returns True
            # at this seq forever after, while runs that predate the patch (which have
            # no marker here) keep returning False.
            for sp in ctx.scheduled_patches:
                self.store.create_step(
                    run_id,
                    sp.seq,
                    kind="PATCH",
                    name=sp.patch_id,
                    input=None,
                    status="COMPLETED",
                    result={"value": True},
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
        # continue-as-new reset the run to PENDING — kick off the next iteration.
        if restart:
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

    def fire_due_activity_timeouts(self, *, now: datetime | None = None, limit: int = 100) -> int:
        """Time out activities that were dispatched but never reported back in time.

        This is the activity-timeout-scanner body: call it periodically alongside
        ``fire_due_timers``. For each SCHEDULED activity past its start-to-close
        deadline it re-dispatches a fresh attempt while the retry budget lasts, else
        records the step FAILED so the workflow sees ``ActivityFailed`` on replay —
        which is what keeps a hung or lost activity from wedging the run forever.
        Passing ``now`` lets tests fast-forward. Returns the number of activities
        timed out (retried or failed)."""
        now = now or utcnow()
        handled = 0
        for run_id, seq in self.store.find_due_activity_timeouts(now=now, limit=limit):
            if self._timeout_activity(run_id, seq, now):
                handled += 1
        return handled

    def _timeout_activity(self, run_id: str, seq: int, now: datetime) -> bool:
        """Claim and resolve one timed-out activity under the run lock.

        Re-checks the deadline inside the lock, so a result that landed between the
        scan and here wins. Retries (a fresh dispatch + new deadline) while attempts
        remain, otherwise fails the step. The driver call and re-tick happen after the
        transaction commits, mirroring ``tick``."""
        redispatch: tuple | None = None
        failed: tuple | None = None
        with self.store.locked_run(run_id) as session:
            step = self.store.get_step(run_id, seq, session=session)
            if step is None or step.kind != "ACTIVITY" or step.status != "SCHEDULED":
                return False
            deadline = step.timeout_at
            if deadline is None:
                return False
            if deadline.tzinfo is None:  # SQLite returns naive datetimes; treat as UTC
                deadline = deadline.replace(tzinfo=timezone.utc)
            if deadline > now:
                return False  # already resolved or its deadline was pushed out
            activity = self.registry.get_activity(step.name)
            ms = activity.attempt_timeout_ms
            if step.attempt < activity.max_retries and ms:
                step.attempt += 1
                step.timeout_at = now + timedelta(milliseconds=ms)
                # Keep step.heartbeat: the retried attempt reads it via heartbeat_details()
                # to resume from the last reported progress rather than restarting.
                args = (step.input or {}).get("args", [])
                kwargs = (step.input or {}).get("kwargs", {})
                redispatch = (step.attempt, step.name, args, kwargs, activity.max_retries)
            else:
                kind = "missed a heartbeat" if activity.heartbeat_timeout_ms else "timed out"
                error = {
                    "type": "ActivityTimeout",
                    "message": f"activity {step.name!r} {kind} after {ms} ms on attempt {step.attempt}",
                }
                self.store.complete_step(
                    run_id, seq, status="FAILED", error=error, attempt=step.attempt, session=session
                )
                failed = (step.name, step.attempt, error)

        if redispatch is not None:
            attempt, name, args, kwargs, max_retries = redispatch
            self._emit(events.ACTIVITY_TIMED_OUT, run_id, name=name, seq=seq, attempt=attempt)
            self.driver.dispatch_activity(run_id, seq, name, args, kwargs, max_retries)
            return True
        if failed is not None:
            name, attempt, error = failed
            self._emit(events.ACTIVITY_FAILED, run_id, name=name, seq=seq, attempt=attempt, error=error)
            self.driver.request_tick(run_id)
            return True
        return False

    # ------------------------------------------------------------- schedules
    def create_schedule(
        self, name: str, cron: str, *, schedule_id: str | None = None, now: datetime | None = None, **kwargs: Any
    ) -> str:
        """Register a recurring start of workflow ``name`` on a cron schedule.

        ``cron`` is a standard 5-field expression (``"0 9 * * 1-5"`` = 9am on
        weekdays). ``kwargs`` are passed as the workflow input on every run. Returns a
        schedule id (provide ``schedule_id`` to make registration idempotent — a
        repeat call with the same id is a no-op). The schedule does nothing until
        ``fire_due_schedules`` is called periodically.
        """
        self.registry.get_workflow(name)  # validate the workflow exists
        spec = parse_cron(cron)  # validate the expression
        sid = schedule_id or uuid4().hex
        next_fire_at = spec.next_after(now or utcnow())
        self.store.create_schedule(id=sid, name=name, cron=cron, input=kwargs, next_fire_at=next_fire_at)
        return sid

    def fire_due_schedules(self, *, now: datetime | None = None, limit: int = 100) -> int:
        """Start a run for every schedule that has come due, and advance each.

        This is the schedule-scanner body: call it periodically (cron/``periodiq``),
        alongside ``fire_due_timers``. Passing ``now`` lets tests fast-forward. Each
        due schedule is claimed (its ``next_fire_at`` advanced to the next cron time)
        before its run is started, so a missed tick is skipped rather than backfilled
        and concurrent scanners don't double-fire. Returns the number of runs started.
        """
        now = now or utcnow()
        claimed = self.store.claim_due_schedules(
            now=now, limit=limit, compute_next=lambda cron, n: parse_cron(cron).next_after(n)
        )
        for schedule_id, name, input in claimed:
            run_id = self.start(name, **input)
            self.store.set_schedule_last_run(schedule_id, run_id)
        return len(claimed)

    def pause_schedule(self, schedule_id: str) -> bool:
        """Stop a schedule from firing without deleting it. Returns ``False`` if absent."""
        return self.store.set_schedule_active(schedule_id, False)

    def resume_schedule(self, schedule_id: str) -> bool:
        """Re-enable a paused schedule. Returns ``False`` if absent."""
        return self.store.set_schedule_active(schedule_id, True)

    def delete_schedule(self, schedule_id: str) -> bool:
        """Remove a schedule entirely. Returns ``False`` if absent."""
        return self.store.delete_schedule(schedule_id)

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
        """Mark a non-terminal run ``CANCELLED``, cascading to its children.

        Any still-running child workflows are cancelled too (recursively, so an
        entire sub-tree comes down), and if this run is itself a child, its parent's
        ``ctx.child_workflow`` is resolved as failed so the parent doesn't wait
        forever. Returns ``False`` if the run is missing or already terminal. No
        driver or registry needed — ``tick`` already short-circuits a cancelled run.
        """
        return self._cancel(run_id, notify_parent=True)

    def _cancel(self, run_id: str, *, notify_parent: bool) -> bool:
        """Cancel one run and cascade to its children.

        ``notify_parent`` is ``True`` for the run the caller cancelled directly (so
        its parent learns the child is gone) and ``False`` for runs reached by the
        downward cascade (their parent is already being cancelled, so there is
        nothing to tell it).
        """
        run_name: str | None = None
        parent_notify: tuple | None = None
        with self.store.locked_run(run_id) as session:
            run = self.store.get_run(run_id, session=session)
            if run is None or run.status in _TERMINAL:
                return False
            run_name = run.name
            self.store.update_run(run_id, session=session, status="CANCELLED")
            if notify_parent and run.parent_run_id is not None:
                error = {"type": "ChildWorkflowCancelled", "message": f"child workflow {run.name!r} was cancelled"}
                parent_notify = (run.parent_run_id, run.parent_seq, "CANCELLED", None, error)
        self._emit(events.RUN_CANCELLED, run_id, name=run_name)
        # Cascade down to any still-running children (which cascade to *their*
        # children); they don't notify this run, since it's already cancelled.
        for child_id in self.store.find_active_children(run_id):
            self._cancel(child_id, notify_parent=False)
        # A directly-cancelled child resolves its parent's step as FAILED so the
        # parent does not wait forever.
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
