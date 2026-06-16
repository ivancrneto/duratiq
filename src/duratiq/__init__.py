"""Duratiq — durable workflows for Dramatiq.

Temporal-style durable execution on your existing Dramatiq + Postgres stack:
workflows are deterministic orchestrator functions, activities are Dramatiq
messages, and all state lives in Postgres so a run resumes exactly where it left
off after a crash.

The engine is feature-complete: activities with per-activity retries, replay and
memoization, crash recovery; durable timers (``ctx.sleep``), signals
(``ctx.wait_signal``), side effects (``ctx.side_effect``), a parallel barrier
(``ctx.gather``), racing branches (``ctx.select``); child workflows
(``ctx.child_workflow``), continue-as-new, ``ctx.patched`` versioning, recurring cron
schedules, and idempotent activities (``activity_info`` / ``run_once``); activity
timeouts and heartbeats; queries and updates; search attributes; a pluggable payload
codec; OpenTelemetry tracing and a lifecycle listener; and a recovery scanner. See
the project CHANGELOG for the full surface.
"""

from __future__ import annotations

from .activity_runtime import ActivityInfo, activity_info, heartbeat, heartbeat_details, run_once
from .codec import IdentityCodec, PayloadCodec, get_payload_codec, set_payload_codec
from .context import TIMEOUT, CancellationScope, WorkflowContext, WorkflowInfo
from .decorators import activity, default_registry, workflow
from .engine import UPDATE_PENDING, Engine
from .events import WorkflowEvent
from .exceptions import (
    ActivityFailed,
    ChildWorkflowFailed,
    ContinueAsNew,
    DeterminismError,
    QueryNotFound,
    Suspend,
    UpdateFailed,
    WorkflowNotFound,
    WorkflowTerminated,
)
from .registry import Activity, Registry, Workflow
from .scanner import Scanner
from .store import SqlStore

__all__ = [
    "TIMEOUT",
    "UPDATE_PENDING",
    "Activity",
    "CancellationScope",
    "ActivityFailed",
    "ActivityInfo",
    "ChildWorkflowFailed",
    "ContinueAsNew",
    "DeterminismError",
    "Engine",
    "IdentityCodec",
    "PayloadCodec",
    "QueryNotFound",
    "Registry",
    "Scanner",
    "SqlStore",
    "Suspend",
    "UpdateFailed",
    "Workflow",
    "WorkflowContext",
    "WorkflowEvent",
    "WorkflowInfo",
    "WorkflowNotFound",
    "WorkflowTerminated",
    "activity",
    "activity_info",
    "default_registry",
    "get_payload_codec",
    "heartbeat",
    "heartbeat_details",
    "run_once",
    "set_payload_codec",
    "workflow",
]
