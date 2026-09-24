import os

from services.common.events import EventEnvelope


MAX_RETRIES = int(
    os.getenv(
        "MAX_EVENT_RETRIES",
        "3",
    )
)


def get_retry_count(message) -> int:
    headers = dict(
        message.headers or []
    )

    raw = headers.get(
        "retry-count"
    )

    if not raw:
        return 0

    try:
        return int(
            raw.decode()
        )
    except Exception:
        return 0


async def retry_or_dlq(
    producer,
    base_topic: str,
    event: EventEnvelope,
    message,
    error: Exception,
):
    current_retry = get_retry_count(
        message
    )

    next_retry = current_retry + 1

    if next_retry <= MAX_RETRIES:
        target_topic = (
            f"{base_topic}.retry"
        )

        print(
            f"Retrying event "
            f"{event.event_id} "
            f"attempt={next_retry}"
        )

    else:
        target_topic = (
            f"{base_topic}.dlq"
        )

        print(
            f"Moving event "
            f"{event.event_id} "
            f"to DLQ"
        )

    headers = [
        (
            "retry-count",
            str(next_retry).encode(),
        ),
        (
            "error",
            str(error)[:500].encode(),
        ),
    ]

    await producer.send_and_wait(
        target_topic,
        event.model_dump(),
        headers=headers,
    )

    return target_topic
