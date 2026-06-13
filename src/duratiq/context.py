"""The workflow context — the only legal door from workflow code to the outside.

Every ``ctx`` call gets a deterministic ``seq`` based on call order. On replay the
same code produces the same sequence of seqs, which line up with the recorded
history so completed work is skipped and only the frontier advances.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

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


class WorkflowContext:
    def __init__(self, run_id: str, steps: list) -> None:
        self.run_id = run_id
        self._history = {step.seq: step for step in steps}
        self.scheduled: list[ScheduledActivity] = []
        self.scheduled_timers: list[ScheduledTimer] = []
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
