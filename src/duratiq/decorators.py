"""``@workflow`` and ``@activity`` decorators.

They register into a :class:`Registry` — a shared default one for convenience, or
an explicit one (used by tests for isolation).
"""

from __future__ import annotations

from typing import Any, Callable

from .registry import Activity, Registry, Workflow

default_registry = Registry()


def activity(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    max_retries: int = 3,
    min_backoff_ms: int | None = None,
    max_backoff_ms: int | None = None,
    start_to_close_ms: int | None = None,
    heartbeat_timeout_ms: int | None = None,
    schedule_to_start_timeout_ms: int | None = None,
    schedule_to_close_timeout_ms: int | None = None,
    registry: Registry | None = None,
) -> Any:
    reg = registry or default_registry

    def wrap(func: Callable[..., Any]) -> Activity:
        act = Activity(
            fn=func,
            name=name or func.__name__,
            max_retries=max_retries,
            min_backoff_ms=min_backoff_ms,
            max_backoff_ms=max_backoff_ms,
            start_to_close_ms=start_to_close_ms,
            heartbeat_timeout_ms=heartbeat_timeout_ms,
            schedule_to_start_timeout_ms=schedule_to_start_timeout_ms,
            schedule_to_close_timeout_ms=schedule_to_close_timeout_ms,
        )
        reg.add_activity(act)
        return act

    return wrap(fn) if fn is not None else wrap


def workflow(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    version: int = 1,
    execution_timeout: float | None = None,
    run_timeout: float | None = None,
    registry: Registry | None = None,
) -> Any:
    reg = registry or default_registry

    def wrap(func: Callable[..., Any]) -> Callable[..., Any]:
        wf = Workflow(
            fn=func,
            name=name or func.__name__,
            version=version,
            execution_timeout=execution_timeout,
            run_timeout=run_timeout,
        )
        reg.add_workflow(wf)
        # Stash the registration so ``ctx.child_workflow(func)`` can recover the
        # registered name even when it was customised via ``name=``.
        func.__duratiq_workflow__ = wf  # type: ignore[attr-defined]
        return func

    return wrap(fn) if fn is not None else wrap


def _dynamic_activity(fn: Callable[..., Any] | None = None, *, registry: Registry | None = None) -> Any:
    """Register a catch-all activity for any name not explicitly registered."""
    reg = registry or default_registry

    def wrap(func: Callable[..., Any]) -> Activity:
        act = Activity(fn=func, name="__dynamic__")
        reg.set_dynamic_activity(act)
        return act

    return wrap(fn) if fn is not None else wrap


def _dynamic_workflow(fn: Callable[..., Any] | None = None, *, registry: Registry | None = None) -> Any:
    """Register a catch-all workflow for any name not explicitly registered."""
    reg = registry or default_registry

    def wrap(func: Callable[..., Any]) -> Callable[..., Any]:
        wf = Workflow(fn=func, name="__dynamic__")
        reg.set_dynamic_workflow(wf)
        return func

    return wrap(fn) if fn is not None else wrap


# Expose .dynamic on the decorator functions as attributes so callers can write
# @activity.dynamic and @workflow.dynamic without importing extra names.
activity.dynamic = _dynamic_activity  # type: ignore[attr-defined]
workflow.dynamic = _dynamic_workflow  # type: ignore[attr-defined]
