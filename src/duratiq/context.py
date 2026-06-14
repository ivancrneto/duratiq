"""The workflow context — the only legal door from workflow code to the outside.

Every ``ctx`` call gets a deterministic ``seq`` based on call order. On replay the
same code produces the same sequence of seqs, which line up with the recorded
history so completed work is skipped and only the frontier advances.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, NoReturn

from .exceptions import ActivityFailed, ChildWorkflowFailed, ContinueAsNew, DeterminismError, Suspend
from .registry import Activity, Workflow

# ISO-8601 duration subset: P[nD]T[nH][nM][nS], e.g. "PT10M", "P1DT6H".
_ISO_DURATION = re.compile(
    r"^P(?:(\d+(?:\.\d+)?)D)?(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?)?$"
)


def duration_seconds(duration: float | str) -> float:
    """Normalise a delay to seconds. Accepts a number (seconds) or an ISO-8601
    duration string such as ``"PT10M"`` or ``"P1DT6H"``."""
    if isinstance(duration, (int, float)):
        return float(duration)
    match = _ISO_DURATION.match(duration)
    if match is None or duration in ("P", "PT"):
        raise ValueError(f"invalid duration {duration!r}: expected seconds or ISO-8601 (e.g. 'PT10M')")
    days, hours, minutes, seconds = (float(part) if part else 0.0 for part in match.groups())
    return days * 86_400 + hours * 3_600 + minutes * 60 + seconds


def _workflow_name(workflow: "str | Workflow | Any") -> str:
    """Resolve a child-workflow reference to its registered name.

    Accepts the name string, a :class:`Workflow`, or a function decorated with
    ``@workflow`` (which carries its registration on ``__duratiq_workflow__``)."""
    if isinstance(workflow, str):
        return workflow
    if isinstance(workflow, Workflow):
        return workflow.name
    wf = getattr(workflow, "__duratiq_workflow__", None)
    if isinstance(wf, Workflow):
        return wf.name
    raise TypeError(f"child_workflow expected a workflow name, a @workflow function, or a Workflow, got {workflow!r}")


class _Timeout:
    """Sentinel returned by ``ctx.wait_signal(name, timeout=...)`` when the timeout
    fires before a signal arrives. A distinct object (not ``None``) so it can't be
    confused with a signal whose payload is ``None``; test with ``is TIMEOUT``."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "duratiq.TIMEOUT"


TIMEOUT = _Timeout()


@dataclass
class ScheduledActivity:
    seq: int
    name: str
    args: list
    kwargs: dict
    max_retries: int
    start_to_close_ms: int | None = None


@dataclass
class ScheduledTimer:
    seq: int
    delay_seconds: float


@dataclass
class ScheduledWait:
    seq: int
    name: str


@dataclass
class ScheduledSideEffect:
    seq: int
    value: Any


@dataclass
class ScheduledPatch:
    seq: int
    patch_id: str


@dataclass
class ScheduledChild:
    seq: int
    name: str
    input: dict


@dataclass
class ScheduledUpdateWait:
    seq: int


@dataclass
class AppliedUpdate:
    update_id: str
    result: Any  # {"value": ...} on success, None on failure
    error: dict | None  # the handler's exception, recorded on the update row


@dataclass
class DeferredCall:
    """An activity call built by :meth:`WorkflowContext.defer` but not yet started.

    Deferring is what lets ``ctx.gather`` collect several calls and launch them
    together — a plain ``ctx.activity`` would suspend on the first one.
    """

    activity: Activity
    args: tuple
    kwargs: dict


class WorkflowContext:
    def __init__(self, run_id: str, steps: list) -> None:
        self.run_id = run_id
        self._history = {step.seq: step for step in steps}
        self.scheduled: list[ScheduledActivity] = []
        self.scheduled_timers: list[ScheduledTimer] = []
        self.scheduled_waits: list[ScheduledWait] = []
        self.scheduled_side_effects: list[ScheduledSideEffect] = []
        self.scheduled_patches: list[ScheduledPatch] = []
        self.scheduled_children: list[ScheduledChild] = []
        # Seqs of the losing side of a wait_signal(timeout=...) race to cancel in this
        # tick's transaction: the timer if the signal won, the wait if it timed out.
        self.cancelled_timers: list[int] = []
        self.cancelled_waits: list[int] = []
        # Read-only handlers registered via set_query_handler; invoked by engine.query
        # after a side-effect-free replay. Populated on every tick but only read by a
        # query, so registering one is free during normal execution.
        self.query_handlers: dict[str, Callable[..., Any]] = {}
        # Update handlers (mutate state, return a result) and optional validators
        # (run read-only before an update is accepted). Keyed by update name.
        self.update_handlers: dict[str, Callable[..., Any]] = {}
        self.update_validators: dict[str, Callable[..., Any]] = {}
        self.scheduled_update_waits: list[ScheduledUpdateWait] = []
        # Results to write back to update rows after the tick's replay (idempotent).
        self.applied_updates: list[AppliedUpdate] = []
        self._seq = 0

    def _next_seq(self) -> int:
        seq = self._seq
        self._seq += 1
        return seq

    def set_query_handler(self, name: str, handler: Callable[..., Any]) -> None:
        """Register a read-only handler that reports the workflow's current state.

        ``engine.query(run_id, name)`` replays the workflow (side-effect-free, advancing
        nothing) up to its frontier, then calls the named handler — typically a closure
        over the workflow's local state, so it sees everything processed so far:

            @workflow(name="cart", registry=reg)
            def cart(ctx):
                items = []
                ctx.set_query_handler("item_count", lambda: len(items))
                while True:
                    items.append(ctx.wait_signal("add"))

        Registering a handler consumes no ``seq`` and never suspends, so it is safe to
        call unconditionally at the top of a workflow and has no effect on replay.
        """
        self.query_handlers[name] = handler

    def set_update_handler(self, name: str, handler: Callable[..., Any]) -> None:
        """Register a handler for ``engine.update(run_id, name, ...)`` updates.

        Unlike a query, an update **mutates** the workflow: the handler runs when the
        workflow consumes the update at a :meth:`wait_update` point, typically mutating
        the workflow's locals and returning a value that the caller reads back. The
        handler must be deterministic — mutate state and return, no I/O or ``ctx`` calls
        — because it is re-run on every replay to reconstruct state (like a query
        handler, but writing). Registering it consumes no ``seq``.
        """
        self.update_handlers[name] = handler

    def set_update_validator(self, name: str, validator: Callable[..., Any]) -> None:
        """Register an optional validator run *before* an update is accepted.

        ``engine.update`` replays the workflow read-only and calls the validator with
        the update's arguments; if it raises, the update is **rejected** and never
        recorded — nothing mutates. Use it to reject bad input (validate-before-mutate)
        without consuming the update. Consumes no ``seq``.
        """
        self.update_validators[name] = validator

    def activity(self, activity: Activity, *args: Any, **kwargs: Any) -> Any:
        """Run an activity durably.

        On first encounter the activity is scheduled and the workflow suspends. On
        every later replay the recorded result is returned without re-running the
        activity.
        """
        seq = self._next_seq()
        step = self._history.get(seq)

        if step is not None:
            if step.name != activity.name:
                raise DeterminismError(
                    f"replay divergence at seq {seq}: history recorded {step.name!r}, "
                    f"but the workflow called {activity.name!r}. Did the workflow code change?"
                )
            if step.status == "COMPLETED":
                return (step.result or {}).get("value")
            if step.status == "FAILED":
                raise ActivityFailed(activity.name, step.error)
            # SCHEDULED but not yet finished — still waiting.
            raise Suspend()

        # Not in history: schedule it (the engine writes the row + dispatches post-commit).
        self.scheduled.append(
            ScheduledActivity(
                seq=seq,
                name=activity.name,
                args=list(args),
                kwargs=dict(kwargs),
                max_retries=activity.max_retries,
                start_to_close_ms=activity.start_to_close_ms,
            )
        )
        raise Suspend()

    def sleep(self, duration: float | str) -> None:
        """Sleep durably for ``duration`` (seconds, or an ISO-8601 string like
        ``"PT10M"``).

        On first encounter a timer is scheduled and the workflow suspends; the
        timer scanner re-ticks the run once the deadline elapses. On every later
        replay the elapsed timer is a no-op and execution continues past it. The
        run survives a crash mid-sleep: the deadline lives in Postgres, not memory.
        """
        seq = self._next_seq()
        step = self._history.get(seq)

        if step is not None:
            if step.kind != "TIMER":
                raise DeterminismError(
                    f"replay divergence at seq {seq}: history recorded a {step.kind!r} step, "
                    f"but the workflow called ctx.sleep(). Did the workflow code change?"
                )
            if step.status == "COMPLETED":
                return None
            # SCHEDULED — the deadline has not elapsed yet.
            raise Suspend()

        # Not in history: schedule the timer (the engine computes fire_at + records it).
        self.scheduled_timers.append(ScheduledTimer(seq=seq, delay_seconds=duration_seconds(duration)))
        raise Suspend()

    def wait_signal(self, name: str, *, timeout: float | str | None = None) -> Any:
        """Wait for an external signal named ``name`` and return its payload.

        The run suspends until ``engine.signal(run_id, name, payload)`` delivers a
        matching signal — typically a human action (approval, cancellation) or an
        outside event. Signals that arrive *before* the wait is reached are queued
        and matched FIFO, so there is no race. On replay the consumed payload is
        returned without re-waiting.

        With ``timeout`` (seconds, or an ISO-8601 string like ``"PT24H"``) the wait
        races a durable timer: if the signal arrives first its payload is returned; if
        the timer fires first :data:`TIMEOUT` is returned (a distinct sentinel, so a
        ``None`` payload is unambiguous — test with ``is TIMEOUT``). The losing side is
        cancelled, so a signal that lands after a timeout is left for a later wait
        rather than silently consumed.
        """
        if timeout is None:
            return self._wait_signal_forever(name)
        return self._wait_signal_timeout(name, timeout)

    def _wait_signal_forever(self, name: str) -> Any:
        seq = self._next_seq()
        step = self._history.get(seq)

        if step is not None:
            if step.kind != "SIGNAL_WAIT":
                raise DeterminismError(
                    f"replay divergence at seq {seq}: history recorded a {step.kind!r} step, "
                    f"but the workflow called ctx.wait_signal(). Did the workflow code change?"
                )
            if step.name != name:
                raise DeterminismError(
                    f"replay divergence at seq {seq}: history waited on signal {step.name!r}, "
                    f"but the workflow now waits on {name!r}. Did the workflow code change?"
                )
            if step.status == "COMPLETED":
                return (step.result or {}).get("value")
            # SCHEDULED — no matching signal has arrived yet.
            raise Suspend()

        # Not in history: register the wait (the engine records it and pairs any
        # already-queued signal in the same transaction).
        self.scheduled_waits.append(ScheduledWait(seq=seq, name=name))
        raise Suspend()

    def _wait_signal_timeout(self, name: str, timeout: float | str) -> Any:
        # A wait_signal(timeout=) occupies two consecutive seqs — a SIGNAL_WAIT and a
        # TIMER — and resolves to whichever completes first. The signal takes priority
        # if both completed in the same window, which keeps the decision deterministic.
        sig_seq = self._next_seq()
        timer_seq = self._next_seq()
        sig_step = self._history.get(sig_seq)
        timer_step = self._history.get(timer_seq)

        if sig_step is not None and sig_step.kind != "SIGNAL_WAIT":
            raise DeterminismError(
                f"replay divergence at seq {sig_seq}: history recorded a {sig_step.kind!r} step, "
                f"but the workflow called ctx.wait_signal(timeout=...). Did the workflow code change?"
            )
        if sig_step is not None and sig_step.name != name:
            raise DeterminismError(
                f"replay divergence at seq {sig_seq}: history waited on signal {sig_step.name!r}, "
                f"but the workflow now waits on {name!r}. Did the workflow code change?"
            )
        if timer_step is not None and timer_step.kind != "TIMER":
            raise DeterminismError(
                f"replay divergence at seq {timer_seq}: history recorded a {timer_step.kind!r} step, "
                f"but the workflow called ctx.wait_signal(timeout=...). Did the workflow code change?"
            )

        if sig_step is not None and sig_step.status == "COMPLETED":
            if timer_step is not None and timer_step.status == "SCHEDULED":
                self.cancelled_timers.append(timer_seq)  # signal won — drop the timer
            return (sig_step.result or {}).get("value")
        if timer_step is not None and timer_step.status == "COMPLETED":
            if sig_step is not None and sig_step.status == "SCHEDULED":
                self.cancelled_waits.append(sig_seq)  # timed out — drop the abandoned wait
            return TIMEOUT

        if sig_step is None and timer_step is None:
            # First encounter: arm both sides.
            self.scheduled_waits.append(ScheduledWait(seq=sig_seq, name=name))
            self.scheduled_timers.append(ScheduledTimer(seq=timer_seq, delay_seconds=duration_seconds(timeout)))
        raise Suspend()

    def wait_update(self) -> str:
        """Suspend until an ``engine.update`` arrives, apply its handler, and continue.

        Mirrors :meth:`wait_signal`: the run parks until an update is delivered, then —
        on the tick that consumes it — looks up the handler registered for the update's
        name, calls it with the update's arguments, and records the handler's result
        (or the error it raised) for the caller. Returns the update's name, so a loop
        can branch on it:

            while True:
                ctx.wait_update()   # one update per call, applied in arrival order

        The handler runs here, inside the replay, so its state mutation is reconstructed
        on every replay; the result recording is idempotent. An update whose name has no
        registered handler is recorded FAILED.
        """
        seq = self._next_seq()
        step = self._history.get(seq)

        if step is None:
            self.scheduled_update_waits.append(ScheduledUpdateWait(seq=seq))
            raise Suspend()
        if step.kind != "UPDATE_WAIT":
            raise DeterminismError(
                f"replay divergence at seq {seq}: history recorded a {step.kind!r} step, "
                f"but the workflow called ctx.wait_update(). Did the workflow code change?"
            )
        if step.status != "COMPLETED":
            raise Suspend()  # no update matched yet

        info = (step.result or {}).get("value") or {}
        name = info.get("name", "")
        handler = self.update_handlers.get(name)
        if handler is None:
            self.applied_updates.append(
                AppliedUpdate(
                    update_id=info["id"],
                    result=None,
                    error={"type": "UpdateHandlerNotFound", "message": f"no update handler named {name!r}"},
                )
            )
            return name
        try:
            value = handler(*info.get("args", []), **info.get("kwargs", {}))
            self.applied_updates.append(AppliedUpdate(update_id=info["id"], result={"value": value}, error=None))
        except Exception as exc:  # noqa: BLE001 - the handler's error is reported to the caller
            self.applied_updates.append(
                AppliedUpdate(
                    update_id=info["id"],
                    result=None,
                    error={"type": type(exc).__name__, "message": str(exc)},
                )
            )
        return name

    def side_effect(self, fn: Callable[[], Any]) -> Any:
        """Record a non-deterministic value once and reuse it on every replay.

        Use this for anything a workflow must *not* recompute on replay —
        ``now()``, a random id, a generated uuid. ``fn`` runs exactly once, on first
        encounter; its (JSON-serialisable) result is stored and returned verbatim
        thereafter. Unlike the other ``ctx`` calls this does not suspend — the value
        is available immediately and the workflow keeps running.
        """
        seq = self._next_seq()
        step = self._history.get(seq)

        if step is not None:
            if step.kind != "SIDE_EFFECT":
                raise DeterminismError(
                    f"replay divergence at seq {seq}: history recorded a {step.kind!r} step, "
                    f"but the workflow called ctx.side_effect(). Did the workflow code change?"
                )
            return (step.result or {}).get("value")

        # Not in history: run it once now and record the value (the engine writes the
        # COMPLETED step in this tick's transaction).
        value = fn()
        self.scheduled_side_effects.append(ScheduledSideEffect(seq=seq, value=value))
        return value

    def patched(self, patch_id: str) -> bool:
        """Gate a change to workflow code so in-flight runs stay deterministic.

        Wrap new behaviour in ``if ctx.patched("my-change"):`` and leave the old
        behaviour in the ``else``. The return value is decided once per call site and
        replayed stably:

        * **New runs** (reaching this point for the first time) record a patch marker
          and take the new path — ``patched`` returns ``True``.
        * **Runs already past this point** under the old code — i.e. history holds a
          real command where the marker would sit — never recorded a marker, so
          ``patched`` returns ``False`` and they keep taking the old path. Crucially
          it does *not* consume a ``seq`` in that case, so the old branch's commands
          line up with the recorded history exactly as before.

        This is the safe way to evolve a deployed workflow without a `DeterminismError`
        on its in-flight runs. Once every pre-patch run has drained you can delete the
        old branch (and eventually the ``patched`` call); removing it earlier risks
        diverging a run that still needs the old path.
        """
        seq = self._seq
        step = self._history.get(seq)

        if step is not None and step.kind == "PATCH":
            if step.name != patch_id:
                raise DeterminismError(
                    f"replay divergence at seq {seq}: history recorded patch {step.name!r}, "
                    f"but the workflow checked ctx.patched({patch_id!r}). Did the patch order change?"
                )
            self._seq += 1  # the marker occupies this seq — advance past it
            return True

        if step is not None:
            # A real command sits here: this run executed past this point under the
            # pre-patch code. Take the old path and leave seq untouched so that
            # branch's commands realign with the recorded history.
            return False

        # Frontier — first execution reaching this point. Record a marker, take the
        # new path. (Committed COMPLETED in this tick, like a side effect.)
        self._seq += 1
        self.scheduled_patches.append(ScheduledPatch(seq=seq, patch_id=patch_id))
        return True

    def continue_as_new(self, **kwargs: Any) -> NoReturn:
        """Restart this workflow with fresh input, discarding accumulated history.

        For long-running or looping workflows (an event loop draining a queue, a
        polling cron) whose step history would otherwise grow without bound. The
        current iteration ends and the run restarts *as if newly started* with
        ``kwargs`` as its input — same run id, empty history. Signals that have not
        yet been consumed carry over to the new iteration; everything else (completed
        steps, fired timers, consumed signals) is dropped.

        This never returns — it raises to unwind the workflow, exactly like the other
        control-flow points. Reaching the call means every prior ``ctx`` step in this
        iteration already completed, so there is no pending work to lose.
        """
        raise ContinueAsNew(dict(kwargs))

    def child_workflow(self, workflow: "str | Workflow | Any", **kwargs: Any) -> Any:
        """Run another workflow as a child and return its result.

        On first encounter the child run is started and the parent suspends; when the
        child reaches a terminal state the parent is re-ticked and ``child_workflow``
        returns the child's result (or raises :class:`ChildWorkflowFailed` if it
        failed or was cancelled). On every later replay the memoized result is
        returned without re-running the child.

        ``workflow`` may be the registered name, a function decorated with
        ``@workflow``, or a :class:`Workflow`. Arguments are keyword-only, mirroring
        ``engine.start``.
        """
        name = _workflow_name(workflow)
        seq = self._next_seq()
        step = self._history.get(seq)

        if step is not None:
            if step.kind != "CHILD_WORKFLOW":
                raise DeterminismError(
                    f"replay divergence at seq {seq}: history recorded a {step.kind!r} step, "
                    f"but the workflow called ctx.child_workflow(). Did the workflow code change?"
                )
            if step.name != name:
                raise DeterminismError(
                    f"replay divergence at seq {seq}: history started child {step.name!r}, "
                    f"but the workflow now starts {name!r}. Did the workflow code change?"
                )
            if step.status == "COMPLETED":
                return (step.result or {}).get("value")
            if step.status == "FAILED":
                raise ChildWorkflowFailed(name, step.error)
            # SCHEDULED — the child run has not reached a terminal state yet.
            raise Suspend()

        # Not in history: schedule the child (the engine starts the sub-run post-commit).
        self.scheduled_children.append(ScheduledChild(seq=seq, name=name, input=dict(kwargs)))
        raise Suspend()

    def defer(self, activity: Activity, *args: Any, **kwargs: Any) -> DeferredCall:
        """Build an activity call without starting it, for use with :meth:`gather`.

        Calling ``defer`` has no effect on its own — it just captures the activity
        and arguments. Pass the result to ``ctx.gather`` to launch it in parallel
        with others.
        """
        return DeferredCall(activity=activity, args=args, kwargs=kwargs)

    def gather(self, *calls: DeferredCall) -> list:
        """Run several deferred activities in parallel and return their results.

        All branches are launched together on first encounter; the workflow then
        suspends until **every** branch has completed, at which point their results
        are returned in call order. If any branch fails, ``gather`` fails fast with
        that error (the others still finish but their results are discarded).
        """
        if not calls:
            return []

        results: list = [None] * len(calls)
        all_done = True
        for i, call in enumerate(calls):
            seq = self._next_seq()
            step = self._history.get(seq)

            if step is None:
                # First encounter: schedule this branch. The engine dispatches every
                # entry in ``scheduled`` after commit, so all branches start at once.
                all_done = False
                self.scheduled.append(
                    ScheduledActivity(
                        seq=seq,
                        name=call.activity.name,
                        args=list(call.args),
                        kwargs=dict(call.kwargs),
                        max_retries=call.activity.max_retries,
                        start_to_close_ms=call.activity.start_to_close_ms,
                    )
                )
                continue

            if step.kind != "ACTIVITY" or step.name != call.activity.name:
                raise DeterminismError(
                    f"replay divergence at seq {seq}: history recorded {step.kind} "
                    f"{step.name!r}, but gather branch {i} called activity "
                    f"{call.activity.name!r}. Did the workflow code change?"
                )
            if step.status == "COMPLETED":
                results[i] = (step.result or {}).get("value")
            elif step.status == "FAILED":
                raise ActivityFailed(call.activity.name, step.error)
            else:  # SCHEDULED — this branch is still in flight.
                all_done = False

        if all_done:
            return results
        raise Suspend()
