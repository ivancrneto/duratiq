"""Runtime helpers available *inside* an activity body.

Activities are at-least-once: a per-activity retry, a broker redelivery, or a crash
can run the same activity more than once. These helpers make that survivable:

* :func:`activity_info` exposes a **stable idempotency key** for the current
  activity (``run_id:seq``), unchanged across retries, redelivery, and replay — pass
  it to an idempotent external API (a Stripe ``Idempotency-Key`` header, say) for
  true end-to-end exactly-once.
* :func:`run_once` records an effect in a dedup table the first time and returns the
  stored result on every later call with the same key — so the expensive/external
  part of an activity runs once even if the activity is retried.
* :func:`heartbeat` reports liveness + progress from a long-running activity, pushing
  its timeout deadline out; :func:`heartbeat_details` reads the last progress back so a
  retry resumes instead of restarting.

These read a context that the driver installs around each activity execution; calling
them outside an activity raises ``RuntimeError``.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Callable, Iterator

# Set by the driver around an activity body; None outside any activity.
_current: ContextVar["_ActivityScope | None"] = ContextVar("duratiq_activity_scope", default=None)


@dataclass(frozen=True)
class ActivityInfo:
    """Identity of the activity execution in progress."""

    run_id: str
    seq: int
    idempotency_key: str  # stable across retries/redelivery/replay: f"{run_id}:{seq}"


@dataclass
class _ActivityScope:
    run_id: str
    seq: int
    store: Any  # SqlStore — kept untyped to avoid an import cycle
    heartbeat_timeout_ms: int | None = None  # how far each heartbeat pushes the deadline


@contextmanager
def activity_scope(run_id: str, seq: int, store: Any, *, heartbeat_timeout_ms: int | None = None) -> Iterator[None]:
    """Install the activity runtime context for the duration of one activity body.

    Drivers wrap each activity call in this so :func:`activity_info` / :func:`run_once`
    / :func:`heartbeat` work inside the body. ``heartbeat_timeout_ms`` is the activity's
    heartbeat timeout, used to push the deadline out on each beat.
    """
    token = _current.set(_ActivityScope(run_id=run_id, seq=seq, store=store, heartbeat_timeout_ms=heartbeat_timeout_ms))
    try:
        yield
    finally:
        _current.reset(token)


def _require_scope(fn_name: str) -> _ActivityScope:
    scope = _current.get()
    if scope is None:
        raise RuntimeError(f"{fn_name} must be called inside an activity body")
    return scope


def activity_info() -> ActivityInfo:
    """Return the current activity's :class:`ActivityInfo` (raises if outside one)."""
    scope = _require_scope("activity_info()")
    return ActivityInfo(run_id=scope.run_id, seq=scope.seq, idempotency_key=f"{scope.run_id}:{scope.seq}")


def run_once(key: str, fn: Callable[[], Any]) -> Any:
    """Run ``fn`` once per ``key``, returning the stored result on repeat calls.

    On first call with ``key`` the result of ``fn()`` is recorded in the dedup table
    and returned; later calls with the same key skip ``fn`` and return that result.
    Use it to wrap the non-idempotent part of an activity so a retry/redelivery
    doesn't repeat the effect:

        info = activity_info()
        return run_once(info.idempotency_key, lambda: charge_card(order_id))

    The result must be JSON-serialisable. The guarantee covers re-execution within
    Duratiq's control (a retried or sequentially-redelivered activity). As with
    Temporal, a crash *between* ``fn``'s external effect landing and the dedup row
    committing can still re-run it — pair the idempotency key with your downstream
    system for hard exactly-once.
    """
    scope = _require_scope("run_once()")
    existing = scope.store.get_dedup(key)
    if existing is not None:
        return (existing.result or {}).get("value")
    value = fn()
    scope.store.put_dedup(key=key, run_id=scope.run_id, seq=scope.seq, result={"value": value})
    return value


def heartbeat(details: Any = None) -> None:
    """Report liveness (and optional progress ``details``) from a long-running activity.

    For an activity declared with ``heartbeat_timeout_ms``, each heartbeat pushes the
    timeout deadline out — so an activity that keeps beating is never timed out, while
    one that goes silent past the interval is retried/failed by the activity-timeout
    scanner. ``details`` (JSON-serialisable) is the latest progress, readable by a retry
    via :func:`heartbeat_details` to resume instead of restarting. Raises if called
    outside an activity body."""
    scope = _require_scope("heartbeat()")
    deadline: datetime | None = None
    if scope.heartbeat_timeout_ms:
        deadline = datetime.now(timezone.utc) + timedelta(milliseconds=scope.heartbeat_timeout_ms)
    scope.store.record_heartbeat(scope.run_id, scope.seq, details=details, timeout_at=deadline)


def heartbeat_details() -> Any:
    """Return the progress recorded by the previous attempt's last :func:`heartbeat`.

    ``None`` if this activity has never heartbeated. Use it at the top of an activity to
    resume from where a retried attempt left off."""
    scope = _require_scope("heartbeat_details()")
    step = scope.store.get_step(scope.run_id, scope.seq)
    return (step.heartbeat or {}).get("value") if step is not None and step.heartbeat else None
