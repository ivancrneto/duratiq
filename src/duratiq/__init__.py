"""Duratiq — durable workflows for Dramatiq.

Temporal-style durable execution on your existing Dramatiq + Postgres stack:
workflows are deterministic orchestrator functions, activities are Dramatiq
messages, and all state lives in Postgres so a run resumes exactly where it left
off after a crash.

This package is the W1–W2 skeleton from DURATIQ_MVP_PLAN.md: activities, replay,
memoization, and crash recovery. Timers, signals, gather, and the admin UI are the
next milestones.
"""

from __future__ import annotations

from .context import WorkflowContext
from .decorators import activity, default_registry, workflow
from .engine import Engine
from .exceptions import ActivityFailed, DeterminismError, Suspend, WorkflowNotFound
from .registry import Activity, Registry, Workflow
from .store import SqlStore

__all__ = [
    "Activity",
    "ActivityFailed",
    "DeterminismError",
    "Engine",
    "Registry",
    "SqlStore",
    "Suspend",
    "Workflow",
    "WorkflowContext",
    "WorkflowNotFound",
    "activity",
    "default_registry",
    "workflow",
]
