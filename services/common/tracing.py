import os

from fastapi import FastAPI

from opentelemetry import (
    propagate,
    trace,
)

from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)

from opentelemetry.instrumentation.fastapi import (
    FastAPIInstrumentor,
)

from opentelemetry.sdk.resources import (
    Resource,
    SERVICE_NAME,
)

from opentelemetry.sdk.trace import (
    TracerProvider,
)

from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)


def install_tracing(
    app: FastAPI,
    service_name: str,
):
    resource = Resource.create(
        {
            SERVICE_NAME: service_name,
            "deployment.environment": "local",
            "service.namespace":
                "event-driven-platform",
        }
    )

    provider = TracerProvider(
        resource=resource
    )

    endpoint = os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://tempo:4318/v1/traces",
    )

    exporter = OTLPSpanExporter(
        endpoint=endpoint
    )

    provider.add_span_processor(
        BatchSpanProcessor(
            exporter
        )
    )

    trace.set_tracer_provider(
        provider
    )

    FastAPIInstrumentor.instrument_app(
        app,
        tracer_provider=provider,
    )


def current_trace_context() -> dict[str, str]:
    """
    Capture the current OpenTelemetry context so it can be
    persisted in the transactional outbox.
    """

    carrier: dict[str, str] = {}

    propagate.inject(
        carrier
    )

    return carrier
