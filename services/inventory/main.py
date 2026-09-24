import asyncio

from fastapi import FastAPI

from services.common.events import EventEnvelope
from services.common.idempotency import (
    acquire_event,
    complete_event,
    release_event,
)
from services.common.kafka import consumer, producer


app = FastAPI(title="Inventory Service")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "inventory",
    }


async def consume_orders():
    c = consumer(
        "orders.events",
        "inventory",
    )

    p = await producer()

    await c.start()

    try:
        async for msg in c:
            event = EventEnvelope(**msg.value)

            acquired = await acquire_event(
                "inventory",
                event.event_id,
            )

            if not acquired:
                print(
                    f"Skipping duplicate inventory event "
                    f"{event.event_id}"
                )

                await c.commit()
                continue

            try:
                inventory_event = EventEnvelope(
                    event_type="InventoryReserved",
                    aggregate_id=event.aggregate_id,
                    payload={
                        "sku": event.payload["sku"],
                        "quantity": event.payload["quantity"],
                        "status": "RESERVED",
                    },
                )

                await p.send_and_wait(
                    "inventory.events",
                    inventory_event.model_dump(),
                )

                await complete_event(
                    "inventory",
                    event.event_id,
                )

                await c.commit()

                print(
                    f"Reserved inventory for order "
                    f"{event.aggregate_id}"
                )

            except Exception:
                await release_event(
                    "inventory",
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
