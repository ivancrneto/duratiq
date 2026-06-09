"""Registry of workflows and activities, and their wrapper types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .exceptions import WorkflowNotFound


@dataclass
class Activity:
    """A unit of side-effecting work. In Duratiq an activity is dispatched as a
    Dramatiq message; its result is recorded so workflow replay can skip it."""

    fn: Callable[..., Any]
    name: str
    max_retries: int = 3

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # Allow calling the activity directly (handy in unit tests / outside a workflow).
        return self.fn(*args, **kwargs)


@dataclass
class Workflow:
    """A deterministic orchestrator function. It must only reach the outside world
    through the :class:`WorkflowContext` so that replay is reproducible."""

    fn: Callable[..., Any]
    name: str
    version: int = 1


class Registry:
    """Holds the workflows and activities a worker knows how to run."""

    def __init__(self) -> None:
        self._activities: dict[str, Activity] = {}
        self._workflows: dict[str, Workflow] = {}

    def add_activity(self, activity: Activity) -> None:
        self._activities[activity.name] = activity

    def add_workflow(self, workflow: Workflow) -> None:
        self._workflows[workflow.name] = workflow

    def get_activity(self, name: str) -> Activity:
        try:
            return self._activities[name]
        except KeyError:
            raise KeyError(f"activity {name!r} is not registered") from None

    def get_workflow(self, name: str) -> Workflow:
        try:
            return self._workflows[name]
        except KeyError:
            raise WorkflowNotFound(f"workflow {name!r} is not registered") from None
