import time

from fastapi import FastAPI, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)


HTTP_REQUESTS = Counter(
    "http_requests_total",
    "Total HTTP requests",
    [
        "service",
        "method",
        "path",
        "status",
    ],
)


HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    [
        "service",
        "method",
        "path",
    ],
)


EVENTS_PROCESSED = Counter(
    "events_processed_total",
    "Domain events processed by services",
    [
        "service",
        "event_type",
        "result",
    ],
)


RETRY_EVENTS = Counter(
    "event_retry_actions_total",
    "Kafka retry and DLQ actions",
    [
        "topic",
        "destination",
    ],
)


def record_event(
    service: str,
    event_type: str,
    result: str = "success",
):
    EVENTS_PROCESSED.labels(
        service=service,
        event_type=event_type,
        result=result,
    ).inc()


def install_metrics(
    app: FastAPI,
    service_name: str,
):

    @app.middleware("http")
    async def prometheus_middleware(
        request: Request,
        call_next,
    ):
        start = time.perf_counter()

        try:
            response = await call_next(
                request
            )

            status = str(
                response.status_code
            )

            return response

        except Exception:
            status = "500"
            raise

        finally:
            duration = (
                time.perf_counter()
                - start
            )

            path = (
                request.url.path
            )

            HTTP_REQUESTS.labels(
                service=service_name,
                method=request.method,
                path=path,
                status=status,
            ).inc()

            HTTP_REQUEST_DURATION.labels(
                service=service_name,
                method=request.method,
                path=path,
            ).observe(
                duration
            )

    @app.get(
        "/metrics",
        include_in_schema=False,
    )
    def metrics():
        return Response(
            generate_latest(),
            media_type=
                CONTENT_TYPE_LATEST,
        )
