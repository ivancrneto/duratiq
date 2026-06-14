"""Duratiq — durable workflows for Dramatiq.

Temporal-style durable execution on your existing Dramatiq + Postgres stack:
workflows are deterministic orchestrator functions, activities are Dramatiq
messages, and all state lives in Postgres so a run resumes exactly where it left
off after a crash.

This package covers W1–W4 of DURATIQ_MVP_PLAN.md — the core MVP engine: activities
with per-activity retries, replay, memoization, crash recovery, durable timers
(``ctx.sleep``), signals (``ctx.wait_signal``), side effects (``ctx.side_effect``),
a parallel barrier (``ctx.gather``), child workflows (``ctx.child_workflow``), and a
recovery scanner for stalled runs. Fast-follow items (continue-as-new, ctx.patched
versioning) remain.
"""

from __future__ import annotations

from .context import WorkflowContext
from .decorators import activity, default_registry, workflow
from .engine import Engine
from .exceptions import ActivityFailed, ChildWorkflowFailed, DeterminismError, Suspend, WorkflowNotFound
from .registry import Activity, Registry, Workflow
from .store import SqlStore

__all__ = [
    "Activity",
    "ActivityFailed",
    "ChildWorkflowFailed",
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
