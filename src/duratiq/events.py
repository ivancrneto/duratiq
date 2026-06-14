"""Lifecycle events for observability.

An :class:`Engine` can be given a ``listener`` — any callable taking a
:class:`WorkflowEvent` — which is invoked as runs and activities change state. It's
the seam for metrics, structured logs, and tracing (wire it to OpenTelemetry,
Prometheus, or your logger) without the engine depending on any of them.

Listeners are best-effort and must never affect execution: an exception raised by a
listener is swallowed, and events are emitted only *after* the state they describe
has been committed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

# Run lifecycle.
RUN_STARTED = "run.started"
RUN_SUSPENDED = "run.suspended"
RUN_COMPLETED = "run.completed"
RUN_FAILED = "run.failed"
RUN_CANCELLED = "run.cancelled"
# Activity lifecycle.
ACTIVITY_SCHEDULED = "activity.scheduled"
ACTIVITY_COMPLETED = "activity.completed"
ACTIVITY_FAILED = "activity.failed"


@dataclass(frozen=True)
class WorkflowEvent:
    """A single lifecycle event. Fields beyond ``type``/``run_id`` are populated
    only when relevant to the event (e.g. ``result`` on ``run.completed``,
    ``seq``/``name`` on activity events, ``error`` on failures)."""

    type: str
    run_id: str
    name: str | None = None  # workflow name (run.*) or activity name (activity.*)
    seq: int | None = None  # step seq for activity.* events
    result: Any = None  # the run's return value on run.completed
    error: Any = None  # error dict on run.failed / activity.failed
    attempt: int | None = None  # activity attempt index on activity.* events


Listener = Callable[[WorkflowEvent], None]
