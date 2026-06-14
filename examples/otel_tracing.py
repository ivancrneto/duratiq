"""OpenTelemetry tracing: a run's lifecycle as spans in one trace.

``duratiq.otel.instrument`` attaches a listener that emits a span per lifecycle
event. Every span for a run lands in one trace whose id is derived from the durable
``run_id`` — so spans from the engine, from activity workers, and from re-ticks after
a crash all correlate, with nothing stored. Here we export to memory and print them;
in production you'd point an OTLP exporter at Jaeger/Tempo instead.

Needs the extra:  pip install "duratiq[otel]"

    cd duratiq && python -m examples.otel_tracing
"""

from __future__ import annotations

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from duratiq import Engine, Registry, SqlStore, activity, workflow
from duratiq.drivers.local import LocalDriver
from duratiq.otel import instrument

reg = Registry()


@activity(name="charge_card", registry=reg)
def charge_card(order_id: str) -> str:
    return f"pay_{order_id}"


@activity(name="send_receipt", registry=reg)
def send_receipt(order_id: str) -> str:
    return f"receipt_{order_id}"


@workflow(name="checkout", registry=reg)
def checkout(ctx, order_id: str) -> dict:
    payment = ctx.activity(charge_card, order_id)
    receipt = ctx.activity(send_receipt, order_id)
    return {"payment": payment, "receipt": receipt}


def main() -> None:
    # Wire an OpenTelemetry SDK that exports finished spans into memory.
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    store = SqlStore()
    store.create_all()
    engine = Engine(reg, store)
    driver = LocalDriver(engine)

    instrument(engine, tracer_provider=provider)  # one line: now every run is traced

    run_id = engine.start("checkout", order_id="A123")
    driver.run_until_idle()

    spans = exporter.get_finished_spans()
    trace_ids = {span.context.trace_id for span in spans}
    print(f"run {run_id[:8]} produced {len(spans)} spans in {len(trace_ids)} trace:\n")
    for span in spans:
        seq = span.attributes.get("duratiq.activity.seq")
        suffix = f" seq={seq}" if seq is not None else ""
        print(f"  {span.name:<20} trace={span.context.trace_id:032x}{suffix}")

    print(f"\nall spans share one trace derived from the run id: {len(trace_ids) == 1}")


if __name__ == "__main__":
    main()
