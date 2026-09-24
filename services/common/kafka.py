import json
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "redpanda:9092",
)


async def producer():
    p = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda value: json.dumps(
            value
        ).encode(),
    )

    await p.start()

    return p


def consumer(
    topics: str | list[str],
    group: str,
):
    if isinstance(topics, str):
        topics = [topics]

    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(
            value.decode()
        ),
    )
