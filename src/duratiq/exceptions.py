"""Duratiq control-flow and error types."""

from __future__ import annotations


class Suspend(Exception):  # noqa: N818 - control-flow signal, not an "Error"
    """Raised inside a workflow when it reaches an async point that is not ready.

    This is *not* a failure. It unwinds the orchestrator stack so the worker can
    be released. The run is re-ticked (and replayed from the top) once the awaited
    thing — an activity result, a timer, a signal — becomes available.
    """


class ContinueAsNew(Exception):  # noqa: N818 - control-flow signal, not an "Error"
    """Raised by ``ctx.continue_as_new`` to restart the workflow with fresh history.

    Like :class:`Suspend` this is *not* a failure — it unwinds the orchestrator so the
    engine can truncate the run's accumulated step history and restart it from the top
    with new input. The carried input becomes the next iteration's arguments.
    """

    def __init__(self, input: dict) -> None:
        self.input = input
        super().__init__("continue-as-new")


class ActivityFailed(Exception):
    """Raised during replay when a memoized activity step is in FAILED state."""

    def __init__(self, activity: str, error: dict | None) -> None:
        self.activity = activity
        self.error = error or {}
        message = self.error.get("message", "activity failed")
        super().__init__(f"activity {activity!r} failed: {message}")


class ChildWorkflowFailed(Exception):
    """Raised during replay when a memoized child-workflow step is in FAILED state.

    A child run that ends FAILED or CANCELLED records its parent's CHILD_WORKFLOW
    step as FAILED; the parent then raises this on replay, where it can be caught or
    left to fail the parent run — exactly like :class:`ActivityFailed`.
    """

    def __init__(self, workflow: str, error: dict | None) -> None:
        self.workflow = workflow
        self.error = error or {}
        message = self.error.get("message", "child workflow failed")
        super().__init__(f"child workflow {workflow!r} failed: {message}")


class WorkflowNotFound(Exception):
    """Raised when a run references a workflow name absent from the registry."""


class QueryNotFound(Exception):  # noqa: N818 - a lookup miss, not a runtime failure
    """Raised by ``engine.query`` when the workflow registered no handler by that name.

    ``available`` lists the handlers the run *did* register, to make a typo obvious.
    """

    def __init__(self, name: str, available: list[str]) -> None:
        self.name = name
        self.available = available
        super().__init__(f"no query handler named {name!r}; registered: {sorted(available)}")


class UpdateFailed(Exception):
    """Raised by ``engine.get_update_result`` when the update's handler raised.

    Carries the recorded ``error`` dict (the handler exception's type and message).
    """

    def __init__(self, name: str, error: dict | None) -> None:
        self.name = name
        self.error = error or {}
        message = self.error.get("message", "update failed")
        super().__init__(f"update {name!r} failed: {message}")


class DeterminismError(Exception):
    """Raised when replay diverges from recorded history (e.g. step kind/name mismatch)."""
