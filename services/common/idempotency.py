import os

from redis.asyncio import Redis


REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://redis:6379/0",
)

redis_client = Redis.from_url(
    REDIS_URL,
    decode_responses=True,
)


def _key(scope: str, event_id: str) -> str:
    return f"idempotency:{scope}:{event_id}"


async def acquire_event(
    scope: str,
    event_id: str,
    processing_ttl: int = 300,
) -> bool:
    """
    Atomically claim an event.

    Returns True if this consumer is allowed to process it.
    Returns False if the event is already being processed or completed.
    """
    result = await redis_client.set(
        _key(scope, event_id),
        "processing",
        nx=True,
        ex=processing_ttl,
    )

    return bool(result)


async def complete_event(
    scope: str,
    event_id: str,
    retention_seconds: int = 604800,
) -> None:
    """
    Mark event as successfully processed.

    Default retention = 7 days.
    """
    await redis_client.set(
        _key(scope, event_id),
        "done",
        ex=retention_seconds,
    )


async def release_event(
    scope: str,
    event_id: str,
) -> None:
    """
    Release the claim if processing failed,
    allowing Kafka retry to process the event again.
    """
    await redis_client.delete(
        _key(scope, event_id)
    )
