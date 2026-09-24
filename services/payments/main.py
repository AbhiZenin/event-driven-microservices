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
    title="Payments Service"
)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "payments",
    }


async def consume_inventory_events():
    c = consumer(
        "inventory.events",
        "payments",
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
                != "InventoryReserved"
            ):
                await c.commit()
                continue

            acquired = await acquire_event(
                "payments",
                event.event_id,
            )

            if not acquired:
                await c.commit()
                continue

            try:
                should_fail = (
                    event.payload.get(
                        "simulate_payment_failure",
                        False,
                    )
                )

                if should_fail:
                    payment_event = (
                        EventEnvelope(
                            event_type=
                                "PaymentFailed",

                            aggregate_id=
                                event.aggregate_id,

                            payload={
                                "amount":
                                    event.payload[
                                        "amount"
                                    ],

                                "sku":
                                    event.payload[
                                        "sku"
                                    ],

                                "quantity":
                                    event.payload[
                                        "quantity"
                                    ],

                                "status":
                                    "FAILED",

                                "reason":
                                    "SIMULATED_FAILURE",
                            },
                        )
                    )

                else:
                    payment_event = (
                        EventEnvelope(
                            event_type=
                                "PaymentAuthorized",

                            aggregate_id=
                                event.aggregate_id,

                            payload={
                                "amount":
                                    event.payload[
                                        "amount"
                                    ],

                                "sku":
                                    event.payload[
                                        "sku"
                                    ],

                                "quantity":
                                    event.payload[
                                        "quantity"
                                    ],

                                "status":
                                    "AUTHORIZED",
                            },
                        )
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
                    f"{payment_event.event_type} "
                    f"for {event.aggregate_id}"
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
        consume_inventory_events()
    )
