import asyncio

from fastapi import FastAPI

from services.common.events import EventEnvelope
from services.common.idempotency import (
    acquire_event,
    complete_event,
    release_event,
)
from services.common.kafka import (
    consumer,
    producer,
)


app = FastAPI(
    title="Inventory Service"
)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "inventory",
    }


async def reserve_inventory():
    c = consumer(
        "orders.events",
        "inventory-reservation",
    )

    p = await producer()

    await c.start()

    try:
        async for msg in c:
            event = EventEnvelope(
                **msg.value
            )

            if (
                event.event_type
                != "OrderCreated"
            ):
                await c.commit()
                continue

            acquired = await acquire_event(
                "inventory-reserve",
                event.event_id,
            )

            if not acquired:
                await c.commit()
                continue

            try:
                reserved = EventEnvelope(
                    event_type=
                        "InventoryReserved",

                    aggregate_id=
                        event.aggregate_id,

                    payload={
                        "sku":
                            event.payload["sku"],

                        "quantity":
                            event.payload[
                                "quantity"
                            ],

                        "amount":
                            event.payload[
                                "amount"
                            ],

                        "simulate_payment_failure":
                            event.payload.get(
                                "simulate_payment_failure",
                                False,
                            ),

                        "status":
                            "RESERVED",
                    },
                )

                await p.send_and_wait(
                    "inventory.events",
                    reserved.model_dump(),
                )

                await complete_event(
                    "inventory-reserve",
                    event.event_id,
                )

                await c.commit()

                print(
                    "Inventory reserved for "
                    f"{event.aggregate_id}"
                )

            except Exception:
                await release_event(
                    "inventory-reserve",
                    event.event_id,
                )

                raise

    finally:
        await c.stop()
        await p.stop()


async def compensate_payment_failure():
    c = consumer(
        "payments.events",
        "inventory-compensation",
    )

    p = await producer()

    await c.start()

    try:
        async for msg in c:
            event = EventEnvelope(
                **msg.value
            )

            if (
                event.event_type
                != "PaymentFailed"
            ):
                await c.commit()
                continue

            acquired = await acquire_event(
                "inventory-release",
                event.event_id,
            )

            if not acquired:
                await c.commit()
                continue

            try:
                released = EventEnvelope(
                    event_type=
                        "InventoryReleased",

                    aggregate_id=
                        event.aggregate_id,

                    payload={
                        "sku":
                            event.payload["sku"],

                        "quantity":
                            event.payload[
                                "quantity"
                            ],

                        "status":
                            "RELEASED",

                        "reason":
                            "PAYMENT_FAILED",
                    },
                )

                await p.send_and_wait(
                    "inventory.events",
                    released.model_dump(),
                )

                await complete_event(
                    "inventory-release",
                    event.event_id,
                )

                await c.commit()

                print(
                    "Inventory released for "
                    f"{event.aggregate_id}"
                )

            except Exception:
                await release_event(
                    "inventory-release",
                    event.event_id,
                )

                raise

    finally:
        await c.stop()
        await p.stop()


@app.on_event("startup")
async def startup():
    asyncio.create_task(
        reserve_inventory()
    )

    asyncio.create_task(
        compensate_payment_failure()
    )
