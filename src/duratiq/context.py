"""The workflow context — the only legal door from workflow code to the outside.

Every ``ctx`` call gets a deterministic ``seq`` based on call order. On replay the
same code produces the same sequence of seqs, which line up with the recorded
history so completed work is skipped and only the frontier advances.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable

from .exceptions import ActivityFailed, DeterminismError, Suspend
from .registry import Activity

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


@dataclass
class ScheduledActivity:
    seq: int
    name: str
    args: list
    kwargs: dict
    max_retries: int


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
        self._seq = 0

    def _next_seq(self) -> int:
        seq = self._seq
        self._seq += 1
        return seq

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

    def wait_signal(self, name: str) -> Any:
        """Wait for an external signal named ``name`` and return its payload.

        The run suspends until ``engine.signal(run_id, name, payload)`` delivers a
        matching signal — typically a human action (approval, cancellation) or an
        outside event. Signals that arrive *before* the wait is reached are queued
        and matched FIFO, so there is no race. On replay the consumed payload is
        returned without re-waiting.
        """
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
