import json
import os

from aiokafka import (
    AIOKafkaConsumer,
    AIOKafkaProducer,
)

from opentelemetry import (
    context as otel_context,
    propagate,
    trace,
)

from opentelemetry.trace import (
    SpanKind,
)


BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "redpanda:9092",
)


def _headers_to_carrier(
    headers,
):
    carrier = {}

    for key, value in (
        headers or []
    ):
        if isinstance(
            value,
            bytes,
        ):
            value = value.decode(
                errors="ignore"
            )

        carrier[key] = str(
            value
        )

    return carrier


def _inject_headers(
    existing_headers=None,
):
    carrier = {}

    propagate.inject(
        carrier
    )

    headers = list(
        existing_headers or []
    )

    existing_keys = {
        key
        for key, _ in headers
    }

    for key, value in (
        carrier.items()
    ):
        if key not in existing_keys:
            headers.append(
                (
                    key,
                    value.encode(),
                )
            )

    return headers


class TracingProducer:

    def __init__(
        self,
        raw_producer,
    ):
        self._raw = raw_producer

        self._tracer = (
            trace.get_tracer(
                "kafka-producer"
            )
        )

    async def stop(
        self,
    ):
        await self._raw.stop()

    async def send_and_wait(
        self,
        topic,
        value,
        **kwargs,
    ):
        parent_context = None

        # Transactional-outbox messages may contain the
        # HTTP request trace context persisted in PostgreSQL.
        if isinstance(
            value,
            dict,
        ):
            trace_context = value.get(
                "trace_context"
            )

            if trace_context:
                parent_context = (
                    propagate.extract(
                        trace_context
                    )
                )

        with self._tracer.start_as_current_span(
            f"{topic} publish",
            context=parent_context,
            kind=SpanKind.PRODUCER,
        ) as span:

            span.set_attribute(
                "messaging.system",
                "kafka",
            )

            span.set_attribute(
                "messaging.destination.name",
                topic,
            )

            if isinstance(
                value,
                dict,
            ):
                event_type = value.get(
                    "event_type"
                )

                aggregate_id = value.get(
                    "aggregate_id"
                )

                if event_type:
                    span.set_attribute(
                        "messaging.event.type",
                        event_type,
                    )

                if aggregate_id:
                    span.set_attribute(
                        "order.id",
                        aggregate_id,
                    )

            original_headers = kwargs.pop(
                "headers",
                None,
            )

            trace_headers = (
                _inject_headers(
                    original_headers
                )
            )

            return await (
                self._raw.send_and_wait(
                    topic,
                    value,
                    headers=trace_headers,
                    **kwargs,
                )
            )


class TracingConsumer:

    def __init__(
        self,
        raw_consumer,
        group_id,
    ):
        self._raw = raw_consumer

        self._group_id = (
            group_id
        )

        self._tracer = (
            trace.get_tracer(
                "kafka-consumer"
            )
        )

        self._span = None
        self._token = None

    async def start(
        self,
    ):
        await self._raw.start()

    async def stop(
        self,
    ):
        self._finish_span()

        await self._raw.stop()

    def __aiter__(
        self,
    ):
        return self

    async def __anext__(
        self,
    ):
        self._finish_span()

        msg = await (
            self._raw.__anext__()
        )

        carrier = (
            _headers_to_carrier(
                msg.headers
            )
        )

        parent_context = (
            propagate.extract(
                carrier
            )
        )

        self._span = (
            self._tracer.start_span(
                f"{msg.topic} process",
                context=parent_context,
                kind=SpanKind.CONSUMER,
            )
        )

        self._span.set_attribute(
            "messaging.system",
            "kafka",
        )

        self._span.set_attribute(
            "messaging.destination.name",
            msg.topic,
        )

        self._span.set_attribute(
            "messaging.kafka.consumer.group",
            self._group_id,
        )

        if isinstance(
            msg.value,
            dict,
        ):
            event_type = (
                msg.value.get(
                    "event_type"
                )
            )

            aggregate_id = (
                msg.value.get(
                    "aggregate_id"
                )
            )

            if event_type:
                self._span.set_attribute(
                    "messaging.event.type",
                    event_type,
                )

            if aggregate_id:
                self._span.set_attribute(
                    "order.id",
                    aggregate_id,
                )

        span_context = (
            trace.set_span_in_context(
                self._span
            )
        )

        self._token = (
            otel_context.attach(
                span_context
            )
        )

        return msg

    async def commit(
        self,
        *args,
        **kwargs,
    ):
        try:
            return await (
                self._raw.commit(
                    *args,
                    **kwargs,
                )
            )

        finally:
            self._finish_span()

    def _finish_span(
        self,
    ):
        if self._token is not None:
            try:
                otel_context.detach(
                    self._token
                )
            except Exception:
                pass

            self._token = None

        if self._span is not None:
            try:
                self._span.end()
            except Exception:
                pass

            self._span = None

    def __getattr__(
        self,
        name,
    ):
        return getattr(
            self._raw,
            name,
        )


async def producer():
    raw = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,

        value_serializer=lambda value:
            json.dumps(
                value
            ).encode(),
    )

    await raw.start()

    return TracingProducer(
        raw
    )


def consumer(
    topics: str | list[str],
    group: str,
):
    if isinstance(
        topics,
        str,
    ):
        topics = [
            topics
        ]

    raw = AIOKafkaConsumer(
        *topics,

        bootstrap_servers=
            BOOTSTRAP,

        group_id=
            group,

        enable_auto_commit=
            False,

        value_deserializer=
            lambda value:
                json.loads(
                    value.decode()
                ),
    )

    return TracingConsumer(
        raw,
        group,
    )
