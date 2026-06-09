"""The workflow context — the only legal door from workflow code to the outside.

Every ``ctx`` call gets a deterministic ``seq`` based on call order. On replay the
same code produces the same sequence of seqs, which line up with the recorded
history so completed work is skipped and only the frontier advances.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .exceptions import ActivityFailed, DeterminismError, Suspend
from .registry import Activity


@dataclass
class ScheduledActivity:
    seq: int
    name: str
    args: list
    kwargs: dict
    max_retries: int


class WorkflowContext:
    def __init__(self, run_id: str, steps: list) -> None:
        self.run_id = run_id
        self._history = {step.seq: step for step in steps}
        self.scheduled: list[ScheduledActivity] = []
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
