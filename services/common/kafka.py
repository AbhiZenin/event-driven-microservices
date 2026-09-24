import json, os
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")

async def producer():
    p = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode()
    )
    await p.start()
    return p

def consumer(topic: str, group: str):
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode()),
    )
