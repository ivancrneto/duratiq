"""Test helpers: a time-skipping, in-memory environment for workflow tests.

:class:`WorkflowEnvironment` wires a registry to an in-memory SQLite store, a
synchronous :class:`~duratiq.drivers.local.LocalDriver`, and a **virtual clock**. The
clock only moves when you tell it to, and every deadline the engine computes (durable
timers, activity and workflow timeouts, cron schedules, ``ctx.now()``) reads from it —
so a workflow that sleeps for a month runs in milliseconds, deterministically::

    from duratiq.testing import WorkflowEnvironment

    @workflow(name="trial", registry=reg)
    def trial(ctx, user_id: int) -> str:
        ctx.sleep("30d")        # or ISO-8601: "P30D"
        return ctx.activity(send_reminder, user_id)

    def test_trial_sends_reminder_after_30_days():
        with WorkflowEnvironment(reg) as env:
            assert env.execute("trial", user_id=7) == "reminded"

``execute`` starts the run and drives it to completion, jumping the clock straight to
the next pending deadline whenever the run is only waiting on time. For step-by-step
control, ``start`` a run, assert on it, then ``skip`` a precise amount of time::

    run_id = env.start("trial", user_id=7)
    env.skip("29d")                               # or "P29D", seconds, a timedelta
    assert env.status(run_id) == "SUSPENDED"     # not yet
    env.skip("1d")
    assert env.result(run_id) == "reminded"

Activities — regular or local — can be replaced for a test without touching the
shared registry::

    @env.mock_activity(send_reminder)
    def fake_send(user_id):
        return "reminded"

The environment is a thin layer over the real engine, so anything not wrapped here is
available on ``env.engine``, ``env.driver``, and ``env.store``. A runnable tour lives
in ``examples/testing_workflows.py``.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar

from sqlalchemy import func, select

from .context import ScheduledLocalActivity, duration_seconds
from .decorators import default_registry
from .drivers.local import LocalDriver
from .engine import Engine
from .events import Listener
from .models import WorkflowRun, WorkflowSchedule, WorkflowStep, WorkflowTimer, utcnow
from .registry import Activity, Registry
from .store import SqlStore

__all__ = ["RunBlocked", "RunFailed", "WorkflowEnvironment"]

_TERMINAL = ("COMPLETED", "FAILED", "CANCELLED")

F = TypeVar("F", bound=Callable[..., Any])


class RunFailed(Exception):
    """Raised by :meth:`WorkflowEnvironment.result` when the run ended ``FAILED`` or ``CANCELLED``.

    ``error`` is the run's recorded error dict (``{"type": ..., "message": ...}``), or
    ``None`` for a cancelled run.
    """

    def __init__(self, run_id: str, status: str, error: dict | None) -> None:
        self.run_id = run_id
        self.status = status
        self.error = error
        if error:
            detail = f"{error.get('type', 'Error')}: {error.get('message', '')}"
        else:
            detail = status.lower()
        super().__init__(f"run {run_id} ended {status}: {detail}")


class RunBlocked(Exception):  # noqa: N818 - a state report, like QueryNotFound
    """Raised by :meth:`WorkflowEnvironment.result` when a run cannot finish on its own.

    The run is waiting on something that time alone will not deliver — a signal, an
    update, or an activity/child that isn't progressing — and no timer or timeout is
    pending that skipping the clock could fire. ``waiting_on`` describes the run's
    outstanding steps, which is usually exactly what the test forgot to send.
    """

    def __init__(self, run_id: str, status: str, waiting_on: list[str]) -> None:
        self.run_id = run_id
        self.status = status
        self.waiting_on = waiting_on
        what = "; ".join(waiting_on) if waiting_on else "nothing recorded"
        super().__init__(f"run {run_id} is {status} and cannot progress without outside input (waiting on: {what})")


class _EnvironmentEngine(Engine):
    """An :class:`Engine` whose local activities honour ``WorkflowEnvironment.mock_activity``.

    Regular activities are mocked through the environment's registry copy; local
    activities never consult the registry, so their replacements live here — keyed by
    activity name, or by function identity for plain callables.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.local_mocks_by_name: dict[str, Callable[..., Any]] = {}
        self.local_mocks_by_fn: dict[Callable[..., Any], Callable[..., Any]] = {}

    def _local_activity_impl(self, sla: ScheduledLocalActivity) -> Callable[..., Any]:
        if isinstance(sla.fn, Activity):
            return self.local_mocks_by_name.get(sla.fn.name, sla.fn)
        try:
            return self.local_mocks_by_fn.get(sla.fn, sla.fn)
        except TypeError:  # an unhashable callable can't have been registered as a mock
            return sla.fn


def _aware(value: datetime) -> datetime:
    """Treat naive datetimes as UTC (SQLite hands them back naive)."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def _as_seconds(duration: timedelta | float | str) -> float:
    if isinstance(duration, timedelta):
        return duration.total_seconds()
    return duration_seconds(duration)


class WorkflowEnvironment:
    """An in-memory engine with a virtual clock, for fast, deterministic workflow tests.

    ``registry`` defaults to :data:`~duratiq.default_registry`. The environment works on
    a **copy** of it, so :meth:`mock_activity` never leaks into other tests. ``store``
    defaults to a fresh in-memory SQLite database (schema created for you); pass your
    own :class:`~duratiq.SqlStore` to run against another database, in which case you
    own its schema and lifecycle. ``start_time`` sets the virtual clock's initial
    value (default: the real current time, frozen). ``max_skips`` bounds how many
    clock jumps :meth:`result` and :meth:`skip` will make, so a workflow that sleeps in
    an endless loop fails the test instead of hanging it.

    Use it as a context manager (or call :meth:`close`) to release the database.
    """

    def __init__(
        self,
        registry: Registry | None = None,
        *,
        store: SqlStore | None = None,
        start_time: datetime | None = None,
        listener: Listener | None = None,
        max_skips: int = 10_000,
    ) -> None:
        self._now = _aware(start_time) if start_time is not None else utcnow()
        self.max_skips = max_skips
        self.registry = (registry if registry is not None else default_registry).copy()
        self._owns_store = store is None
        if store is None:
            store = SqlStore()
            store.create_all()
        self.store = store
        self.engine = _EnvironmentEngine(self.registry, self.store, listener=listener, clock=self.now)
        self.driver = LocalDriver(self.engine)

    # ------------------------------------------------------------- lifecycle
    def __enter__(self) -> WorkflowEnvironment:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Dispose of the environment's in-memory database (no-op for a store you passed in)."""
        if self._owns_store:
            self.store.engine.dispose()

    # ----------------------------------------------------------------- clock
    def now(self) -> datetime:
        """The virtual current time (timezone-aware UTC). Moves only via :meth:`skip` / :meth:`result`."""
        return self._now

    def skip(self, duration: timedelta | float | str) -> None:
        """Advance the virtual clock by ``duration``, firing everything that comes due.

        ``duration`` is a :class:`~datetime.timedelta` or anything ``ctx.sleep``
        accepts: seconds, shorthand (``"10m"``, ``"30d"``, ``"1h30m"``), or ISO-8601
        (``"PT10M"``, ``"P30D"``). The clock stops at **each** intermediate deadline (timer, activity or
        workflow timeout, cron schedule) in order and fires it before moving on, exactly
        as a continuously running scanner would. So ``skip("P31D")`` over a workflow
        that sleeps 30 days and then 1 day fires both timers, and ``ctx.now()`` after
        each one reads the right time.
        """
        seconds = _as_seconds(duration)
        if seconds < 0:
            raise ValueError(f"cannot skip backwards in time (got {duration!r})")
        target = self._now + timedelta(seconds=seconds)
        self._fire_due()
        for _ in range(self.max_skips):
            deadline = self._next_deadline(include_schedules=True)
            if deadline is None or deadline > target:
                break
            self._now = deadline
            self._fire_due()
        else:
            raise RuntimeError(
                f"skip({duration!r}) hit more than max_skips={self.max_skips} deadlines; "
                "skip a shorter span or raise WorkflowEnvironment(max_skips=...)"
            )
        self._now = target
        self._fire_due()

    # ------------------------------------------------------------------ runs
    def start(self, name: str, **kwargs: Any) -> str:
        """Start a run (``kwargs`` as for :meth:`Engine.start`) and drive it until it blocks.

        Returns the run id. The clock does not move: a run that sleeps stays
        ``SUSPENDED`` until you :meth:`skip` or ask for its :meth:`result`.
        """
        run_id = self.engine.start(name, **kwargs)
        self.run_until_idle()
        return run_id

    def execute(self, name: str, **kwargs: Any) -> Any:
        """Start a run and return its result, skipping time as needed — the one-liner.

        Equivalent to ``env.result(env.start(name, **kwargs))``. Raises
        :class:`RunFailed` if the run fails and :class:`RunBlocked` if it waits on a
        signal or update that never arrives.
        """
        return self.result(self.start(name, **kwargs))

    def result(self, run_id: str, *, skip_time: bool = True) -> Any:
        """Drive ``run_id`` to a terminal state and return its result.

        While the run is waiting only on time, the virtual clock jumps straight to the
        next pending deadline (across *all* runs — a parent waiting on a sleeping child
        works). Raises :class:`RunFailed` if the run ends ``FAILED``/``CANCELLED``, and
        :class:`RunBlocked` if it still needs outside input (a signal, an update) once
        nothing time-based is left to fire. With ``skip_time=False`` the clock is left
        alone, and a run that has not finished yet raises :class:`RunBlocked` at once.
        """
        for _ in range(self.max_skips + 1):
            self.run_until_idle()
            run = self._get_run(run_id)
            if run.status in _TERMINAL:
                return self._outcome(run)
            if self._fire_due():
                continue  # something was due at the current time — let the run react
            if not skip_time:
                break
            deadline = self._next_deadline(include_schedules=False)
            if deadline is None:
                break
            self._now = deadline
        else:
            raise RuntimeError(
                f"run {run_id} did not finish within max_skips={self.max_skips} clock jumps "
                "(is it sleeping in an endless loop?); raise WorkflowEnvironment(max_skips=...) if intended"
            )
        raise RunBlocked(run_id, run.status, self.pending(run_id))

    def signal(self, run_id: str, name: str, payload: Any = None) -> bool:
        """Deliver a signal and drive the run until it blocks again. See :meth:`Engine.signal`."""
        delivered = self.engine.signal(run_id, name, payload)
        self.run_until_idle()
        return delivered

    def update(self, run_id: str, name: str, *args: Any, **kwargs: Any) -> Any:
        """Send an update, drive the run, and return the handler's result.

        Raises the validator's exception if the update is rejected, and
        :class:`~duratiq.UpdateFailed` if the handler raised. Returns
        :data:`~duratiq.UPDATE_PENDING` if the run never reached a ``ctx.wait_update``.
        """
        update_id = self.engine.update(run_id, name, *args, **kwargs)
        self.run_until_idle()
        return self.engine.get_update_result(run_id, update_id)

    def query(self, run_id: str, name: str, *args: Any, **kwargs: Any) -> Any:
        """Call a workflow's query handler. See :meth:`Engine.query`."""
        return self.engine.query(run_id, name, *args, **kwargs)

    def cancel(self, run_id: str) -> bool:
        """Cancel a run (cascading to children) and drive whatever that unblocks."""
        cancelled = self.engine.cancel(run_id)
        self.run_until_idle()
        return cancelled

    def terminate(self, run_id: str, reason: str | None = None) -> bool:
        """Terminate a run (cascading to children) and drive whatever that unblocks."""
        terminated = self.engine.terminate(run_id, reason)
        self.run_until_idle()
        return terminated

    def run_until_idle(self) -> None:
        """Process every queued tick and activity (no clock movement)."""
        self.driver.run_until_idle()

    # ------------------------------------------------------------ inspection
    def get(self, run_id: str) -> WorkflowRun:
        """The run's current database row."""
        return self._get_run(run_id)

    def status(self, run_id: str) -> str:
        """The run's current status (``SUSPENDED``, ``COMPLETED``, ...)."""
        return self._get_run(run_id).status

    def history(self, run_id: str) -> list[WorkflowStep]:
        """The run's recorded steps, in ``seq`` order."""
        return self.store.get_steps(run_id)

    def pending(self, run_id: str) -> list[str]:
        """Human-readable descriptions of the run's outstanding (``SCHEDULED``) steps."""
        described: list[str] = []
        with self.store.Session() as s:
            steps = s.scalars(
                select(WorkflowStep)
                .where(WorkflowStep.run_id == run_id, WorkflowStep.status == "SCHEDULED")
                .order_by(WorkflowStep.seq)
            ).all()
            for step in steps:
                where = f"seq {step.seq}"
                if step.kind == "TIMER":
                    timer = s.get(WorkflowTimer, (run_id, step.seq))
                    due = f" due {_aware(timer.fire_at).isoformat()}" if timer is not None else ""
                    described.append(f"timer{due} ({where})")
                elif step.kind == "SIGNAL_WAIT":
                    described.append(f"signal {step.name!r} ({where})")
                elif step.kind == "SIGNAL_HANDLER":
                    described.append(f"signal handler {step.name!r} ({where})")
                elif step.kind == "UPDATE_WAIT":
                    described.append(f"an update ({where})")
                elif step.kind == "CHILD_WORKFLOW":
                    child = s.scalar(
                        select(WorkflowRun).where(
                            WorkflowRun.parent_run_id == run_id, WorkflowRun.parent_seq == step.seq
                        )
                    )
                    state = f", child run {child.id} is {child.status}" if child is not None else ""
                    described.append(f"child workflow {step.name!r} ({where}{state})")
                else:
                    described.append(f"{step.kind.lower().replace('_', ' ')} {step.name!r} ({where})")
        return described

    # --------------------------------------------------------------- mocking
    def mock_activity(self, activity: Activity | str | Callable[..., Any], fn: F | None = None) -> Any:
        """Replace an activity's implementation for this environment only.

        ``activity`` identifies what to replace:

        * an :class:`~duratiq.Activity` (what ``@activity`` returns) or its registered
          name — replaced wherever the workflow runs it, via ``ctx.activity`` *or*
          ``ctx.local_activity``;
        * a plain function the workflow passes to ``ctx.local_activity`` — matched by
          identity, so pass the very function object the workflow uses.

        The replacement keeps the original's retry and timeout options (for a local
        activity, the ``max_retries`` the workflow asked for), so retry behaviour is
        still exercised, and the registry you gave the environment is never modified.
        Works directly or as a decorator::

            env.mock_activity(charge_card, lambda order_id: "ch_test")

            @env.mock_activity("charge_card")
            def fake_charge(order_id):
                raise RuntimeError("card declined")

            env.mock_activity(normalize_address, Mock(return_value={"zip": "00000"}))

        Any callable works as the replacement, including a :class:`unittest.mock.Mock`
        for call assertions.
        """
        if isinstance(activity, (str, Activity)):
            name = activity if isinstance(activity, str) else activity.name
            try:
                original: Activity | None = self.registry.get_activity(name)
            except KeyError:
                if isinstance(activity, str):
                    raise  # an unknown name is a typo, not something to mock
                original = None  # an Activity this registry doesn't know: only ever run locally

            def install(replacement: F) -> F:
                if original is not None:
                    self.registry.add_activity(dataclasses.replace(original, fn=replacement, name=name))
                self.engine.local_mocks_by_name[name] = replacement
                return replacement

        elif callable(activity):

            def install(replacement: F) -> F:
                self.engine.local_mocks_by_fn[activity] = replacement
                return replacement

        else:
            raise TypeError(f"mock_activity expects an Activity, an activity name, or a function, got {activity!r}")

        return install if fn is None else install(fn)

    # ------------------------------------------------------------- internals
    def _get_run(self, run_id: str) -> WorkflowRun:
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(f"run {run_id!r} not found")
        return run

    @staticmethod
    def _outcome(run: WorkflowRun) -> Any:
        if run.status == "COMPLETED":
            return (run.result or {}).get("value")
        raise RunFailed(run.id, run.status, run.error)

    def _fire_due(self) -> int:
        """Run every scanner at the current virtual time, then drain the driver queue."""
        now = self._now
        fired = (
            self.engine.fire_due_timers(now=now)
            + self.engine.fire_due_activity_timeouts(now=now)
            + self.engine.fire_due_schedule_to_start_timeouts(now=now)
            + self.engine.fire_due_execution_timeouts(now=now)
            + self.engine.fire_due_run_timeouts(now=now)
            + self.engine.fire_due_schedules(now=now)
        )
        self.run_until_idle()
        return fired

    def _next_deadline(self, *, include_schedules: bool) -> datetime | None:
        """The earliest pending deadline strictly after the current virtual time."""
        now = self._now
        queries = [
            select(func.min(WorkflowTimer.fire_at)).where(WorkflowTimer.fired_at.is_(None), WorkflowTimer.fire_at > now)
        ]
        for column in (WorkflowStep.timeout_at, WorkflowStep.schedule_to_start_at, WorkflowStep.schedule_to_close_at):
            queries.append(
                select(func.min(column)).where(
                    WorkflowStep.kind == "ACTIVITY", WorkflowStep.status == "SCHEDULED", column > now
                )
            )
        for column in (WorkflowRun.execution_timeout_at, WorkflowRun.run_timeout_at):
            queries.append(select(func.min(column)).where(WorkflowRun.status.not_in(_TERMINAL), column > now))
        if include_schedules:
            queries.append(
                select(func.min(WorkflowSchedule.next_fire_at)).where(
                    WorkflowSchedule.active.is_(True), WorkflowSchedule.next_fire_at > now
                )
            )
        with self.store.Session() as s:
            found = [_aware(value) for value in (s.scalar(q) for q in queries) if value is not None]
        return min(found, default=None)
