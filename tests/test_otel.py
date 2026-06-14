"""OpenTelemetry tracing: lifecycle events become spans, all in the run's trace.

Spans are collected with the in-memory exporter, so these assert real exported
spans — names, the run-derived trace grouping, attributes, error status, and that
an existing listener is chained rather than dropped.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.otel import _trace_id_for_run, instrument, run_trace_context


@pytest.fixture
def tracing() -> SimpleNamespace:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    reg = Registry()

    @activity(name="charge", registry=reg)
    def charge(order_id: str) -> str:
        return f"paid::{order_id}"

    @activity(name="boom", registry=reg)
    def boom() -> None:
        raise ValueError("kaboom")

    @workflow(name="checkout", registry=reg)
    def checkout(ctx, order_id: str) -> dict:
        return {"payment": ctx.activity(charge, order_id)}

    @workflow(name="doomed", registry=reg)
    def doomed(ctx) -> None:
        ctx.activity(boom)

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)
    return SimpleNamespace(engine=engine, driver=driver, provider=provider, exporter=exporter)


def _spans_by_name(exporter: InMemorySpanExporter) -> dict[str, list]:
    out: dict[str, list] = {}
    for span in exporter.get_finished_spans():
        out.setdefault(span.name, []).append(span)
    return out


def test_completed_run_emits_lifecycle_spans(tracing: SimpleNamespace) -> None:
    instrument(tracing.engine, tracer_provider=tracing.provider)
    run_id = tracing.engine.start("checkout", order_id="A1")
    tracing.driver.run_until_idle()

    names = _spans_by_name(tracing.exporter)
    assert "run.started" in names
    assert "activity.scheduled" in names
    assert "activity.completed" in names
    assert "run.completed" in names

    # Every span for the run shares the trace derived from its run_id.
    expected_trace = _trace_id_for_run(run_id)
    assert {s.context.trace_id for s in tracing.exporter.get_finished_spans()} == {expected_trace}


def test_activity_span_attributes(tracing: SimpleNamespace) -> None:
    instrument(tracing.engine, tracer_provider=tracing.provider)
    run_id = tracing.engine.start("checkout", order_id="A1")
    tracing.driver.run_until_idle()

    by_name = _spans_by_name(tracing.exporter)
    # The activity name rides on activity.scheduled; seq/attempt on activity.completed.
    scheduled = by_name["activity.scheduled"][0]
    assert scheduled.attributes["duratiq.run_id"] == run_id
    assert scheduled.attributes["duratiq.activity.name"] == "charge"
    assert scheduled.attributes["duratiq.activity.seq"] == 0

    completed = by_name["activity.completed"][0]
    assert completed.attributes["duratiq.activity.seq"] == 0
    assert completed.attributes["duratiq.activity.attempt"] == 0


def test_failed_run_marks_error_status(tracing: SimpleNamespace) -> None:
    instrument(tracing.engine, tracer_provider=tracing.provider)
    tracing.engine.start("doomed")
    tracing.driver.run_until_idle()

    names = _spans_by_name(tracing.exporter)
    failed = names["run.failed"][0]
    assert failed.status.status_code is StatusCode.ERROR
    assert failed.attributes["duratiq.error.type"] == "ActivityFailed"

    activity_failed = names["activity.failed"][0]
    assert activity_failed.status.status_code is StatusCode.ERROR
    assert "kaboom" in activity_failed.attributes["duratiq.error.message"]


def test_existing_listener_is_chained(tracing: SimpleNamespace) -> None:
    seen = []
    tracing.engine.listener = lambda event: seen.append(event.type)

    instrument(tracing.engine, tracer_provider=tracing.provider)
    tracing.engine.start("checkout", order_id="A1")
    tracing.driver.run_until_idle()

    # The pre-existing listener still fired for the run lifecycle.
    assert "run.started" in seen
    assert "run.completed" in seen
    # ...and spans were still exported.
    assert tracing.exporter.get_finished_spans()


def test_run_trace_context_joins_the_run_trace(tracing: SimpleNamespace) -> None:
    run_id = "ab" * 16  # 32 hex chars, like a real run id
    tracer = tracing.provider.get_tracer("test")
    with tracer.start_as_current_span("app-handler", context=run_trace_context(run_id)) as span:
        assert span.get_span_context().trace_id == _trace_id_for_run(run_id)


def test_trace_id_is_nonzero_and_stable() -> None:
    assert _trace_id_for_run("0" * 32) != 0  # forced non-zero even from an all-zero id
    assert _trace_id_for_run("deadbeef") == _trace_id_for_run("deadbeef")  # stable
