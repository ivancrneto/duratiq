"""OpenTelemetry tracing for duratiq, built on the :class:`Engine` listener hook.

``instrument(engine)`` attaches a listener that turns lifecycle events (run started,
activity completed, run failed, ...) into OpenTelemetry spans — so a run's progress
shows up in Jaeger / Tempo / any OTLP backend with no change to workflow code.

**Cross-process trace propagation.** Every span for a run is placed in one trace
whose id is *derived from the durable ``run_id``* (a uuid4 hex — already a 128-bit
W3C trace-id). Nothing is stored and no headers are threaded through messages: the
engine tick, an activity running in another worker, and a re-tick after a crash all
compute the *same* trace-id from the run_id, so their spans land in one trace. Use
:func:`run_trace_context` to parent your own spans (an HTTP handler, an activity
body) to that same trace.

This is an optional integration; install the extra::

    pip install "duratiq[otel]"

Spans mark lifecycle transitions (they're emitted from the post-commit listener, so
they're point-in-time). They carry ``duratiq.run_id``, the workflow/activity name,
step ``seq``/``attempt``, and — on failures — the error and an ERROR status.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from . import events
from .events import WorkflowEvent

if TYPE_CHECKING:
    from opentelemetry.context import Context
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.trace import Tracer

    from .engine import Engine
    from .events import Listener

try:
    from opentelemetry import trace as _trace
    from opentelemetry.trace import (
        NonRecordingSpan,
        SpanContext,
        Status,
        StatusCode,
        TraceFlags,
    )
except ModuleNotFoundError as exc:  # pragma: no cover - exercised only without the extra
    raise ModuleNotFoundError("duratiq.otel needs OpenTelemetry. Install it with: pip install 'duratiq[otel]'") from exc

_INSTRUMENTATION = "duratiq"


def _trace_id_for_run(run_id: str) -> int:
    """Stable 128-bit W3C trace-id for a run, derived from its durable id.

    ``engine.start`` ids are ``uuid4().hex`` (32 hex chars = 128 bits), so the id is
    already a valid trace-id; any other id is hashed down to 128 bits. The result is
    forced non-zero (a zero trace-id is invalid)."""
    try:
        tid = int(run_id[:32], 16)
    except ValueError:
        tid = int.from_bytes(hashlib.blake2b(run_id.encode(), digest_size=16).digest(), "big")
    return tid or 1


def _span_id_for_run(run_id: str) -> int:
    """Stable 64-bit id for a run's synthetic root span (non-zero)."""
    sid = int.from_bytes(hashlib.blake2b(run_id.encode(), digest_size=8).digest(), "big")
    return sid or 1


def run_trace_context(run_id: str) -> Context:
    """An OTel context whose trace is the run's, for parenting your own spans.

        with tracer.start_as_current_span("handle", context=run_trace_context(run_id)):
            ...

    The returned context holds a non-recording remote span carrying the run's derived
    ``(trace_id, span_id)`` — so a span started under it joins the run's trace and
    hangs off the same logical root every other duratiq span uses."""
    span_context = SpanContext(
        trace_id=_trace_id_for_run(run_id),
        span_id=_span_id_for_run(run_id),
        is_remote=True,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    return _trace.set_span_in_context(NonRecordingSpan(span_context))


def _attributes(event: WorkflowEvent) -> dict[str, object]:
    attrs: dict[str, object] = {"duratiq.run_id": event.run_id, "duratiq.event": event.type}
    if event.name is not None:
        key = "duratiq.activity.name" if event.type.startswith("activity.") else "duratiq.workflow.name"
        attrs[key] = event.name
    if event.seq is not None:
        attrs["duratiq.activity.seq"] = event.seq
    if event.attempt is not None:
        attrs["duratiq.activity.attempt"] = event.attempt
    if isinstance(event.error, dict):
        if "type" in event.error:
            attrs["duratiq.error.type"] = str(event.error["type"])
        if "message" in event.error:
            attrs["duratiq.error.message"] = str(event.error["message"])
    return attrs


_ERROR_EVENTS = frozenset({events.RUN_FAILED, events.ACTIVITY_FAILED})


class OTelListener:
    """Engine listener that emits an OpenTelemetry span per lifecycle event.

    Each span sits in the run's derived trace (see :func:`run_trace_context`). If the
    engine already had a listener, it is chained — yours still runs — so instrumenting
    never silently drops an existing metrics/logging hook."""

    def __init__(self, tracer: Tracer, *, chain: Listener | None = None) -> None:
        self._tracer = tracer
        self._chain = chain

    def __call__(self, event: WorkflowEvent) -> None:
        span = self._tracer.start_span(
            event.type,
            context=run_trace_context(event.run_id),
            attributes=_attributes(event),
        )
        if event.type in _ERROR_EVENTS:
            message = event.error.get("message") if isinstance(event.error, dict) else None
            span.set_status(Status(StatusCode.ERROR, str(message) if message else None))
        span.end()
        if self._chain is not None:
            self._chain(event)


def instrument(engine: Engine, *, tracer_provider: TracerProvider | None = None) -> OTelListener:
    """Attach OpenTelemetry tracing to ``engine`` and return the installed listener.

    Uses ``tracer_provider`` if given, else the global one. Any listener already on
    the engine is chained, not replaced."""
    provider = tracer_provider or _trace.get_tracer_provider()
    listener = OTelListener(provider.get_tracer(_INSTRUMENTATION), chain=engine.listener)
    engine.listener = listener
    return listener
