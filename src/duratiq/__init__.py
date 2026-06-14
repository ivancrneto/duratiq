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

from .codec import IdentityCodec, PayloadCodec, get_payload_codec, set_payload_codec
from .context import WorkflowContext
from .decorators import activity, default_registry, workflow
from .engine import Engine
from .events import WorkflowEvent
from .exceptions import ActivityFailed, ChildWorkflowFailed, ContinueAsNew, DeterminismError, Suspend, WorkflowNotFound
from .registry import Activity, Registry, Workflow
from .store import SqlStore

__all__ = [
    "Activity",
    "ActivityFailed",
    "ChildWorkflowFailed",
    "ContinueAsNew",
    "DeterminismError",
    "Engine",
    "IdentityCodec",
    "PayloadCodec",
    "Registry",
    "SqlStore",
    "Suspend",
    "Workflow",
    "WorkflowContext",
    "WorkflowEvent",
    "WorkflowNotFound",
    "activity",
    "default_registry",
    "get_payload_codec",
    "set_payload_codec",
    "workflow",
]
