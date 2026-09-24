import asyncio

from fastapi import FastAPI

from services.common.events import EventEnvelope
from services.common.idempotency import (
    acquire_event,
    complete_event,
    release_event,
)
from services.common.kafka import consumer, producer


app = FastAPI(title="Payments Service")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "payments",
    }


async def consume_orders():
    c = consumer(
        "orders.events",
        "payments",
    )

    p = await producer()

    await c.start()

    try:
        async for msg in c:
            event = EventEnvelope(**msg.value)

            acquired = await acquire_event(
                "payments",
                event.event_id,
            )

            if not acquired:
                print(
                    f"Skipping duplicate payment event "
                    f"{event.event_id}"
                )

                await c.commit()
                continue

            try:
                payment_event = EventEnvelope(
                    event_type="PaymentAuthorized",
                    aggregate_id=event.aggregate_id,
                    payload={
                        "amount": event.payload["amount"],
                        "status": "AUTHORIZED",
                    },
                )

                await p.send_and_wait(
                    "payments.events",
                    payment_event.model_dump(),
                )

                await complete_event(
                    "payments",
                    event.event_id,
                )

                await c.commit()

                print(
                    f"Processed payment for order "
                    f"{event.aggregate_id}"
                )

            except Exception:
                await release_event(
                    "payments",
                    event.event_id,
                )

                raise

    finally:
        await c.stop()
        await p.stop()


@app.on_event("startup")
async def startup():
    asyncio.create_task(
        consume_orders()
    )
