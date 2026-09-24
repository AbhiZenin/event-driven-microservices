from services.common.tracing import install_tracing
from services.common.metrics import install_metrics
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
from services.common.retry import retry_or_dlq


app = FastAPI(
    title="Inventory Service",
    version="1.0.0",
)

install_metrics(app, "inventory")
install_tracing(app, "inventory")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "inventory",
    }


# ============================================================
# INVENTORY RESERVATION
# ============================================================

async def reserve_inventory():
    c = consumer(
        [
            "orders.events",
            "orders.events.retry",
        ],
        "inventory-reservation",
    )

    p = await producer()

    await c.start()

    print(
        "Inventory reservation consumer started"
    )

    try:
        async for msg in c:
            event = EventEnvelope(
                **msg.value
            )

            if event.event_type != "OrderCreated":
                await c.commit()
                continue

            acquired = await acquire_event(
                "inventory-reserve",
                event.event_id,
            )

            if not acquired:
                print(
                    "Skipping duplicate inventory event "
                    f"{event.event_id}"
                )

                await c.commit()
                continue

            try:
                # ------------------------------------------------
                # CONTROLLED TECHNICAL FAILURE
                #
                # Used only to test:
                # retry 1 -> retry 2 -> retry 3 -> DLQ
                # ------------------------------------------------

                if event.payload.get(
                    "simulate_inventory_error",
                    False,
                ):
                    raise RuntimeError(
                        "Simulated inventory infrastructure failure"
                    )

                # ------------------------------------------------
                # SUCCESSFUL INVENTORY RESERVATION
                # ------------------------------------------------

                reserved_event = EventEnvelope(
                    event_type="InventoryReserved",
                    aggregate_id=event.aggregate_id,
                    payload={
                        "sku":
                            event.payload["sku"],

                        "quantity":
                            event.payload["quantity"],

                        "amount":
                            event.payload["amount"],

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
                    reserved_event.model_dump(),
                )

                await complete_event(
                    "inventory-reserve",
                    event.event_id,
                )

                await c.commit()

                print(
                    "Inventory reserved for order "
                    f"{event.aggregate_id}"
                )

            except Exception as exc:
                await release_event(
                    "inventory-reserve",
                    event.event_id,
                )

                target_topic = await retry_or_dlq(
                    p,
                    "orders.events",
                    event,
                    msg,
                    exc,
                )

                await c.commit()

                print(
                    "Inventory reservation failed for "
                    f"order {event.aggregate_id}. "
                    f"Forwarded to {target_topic}. "
                    f"Error: {exc}"
                )

    finally:
        await c.stop()
        await p.stop()


# ============================================================
# SAGA COMPENSATION
# ============================================================

async def compensate_payment_failure():
    c = consumer(
        [
            "payments.events",
            "payments.events.retry",
        ],
        "inventory-compensation",
    )

    p = await producer()

    await c.start()

    print(
        "Inventory compensation consumer started"
    )

    try:
        async for msg in c:
            event = EventEnvelope(
                **msg.value
            )

            if event.event_type != "PaymentFailed":
                await c.commit()
                continue

            acquired = await acquire_event(
                "inventory-release",
                event.event_id,
            )

            if not acquired:
                print(
                    "Skipping duplicate compensation event "
                    f"{event.event_id}"
                )

                await c.commit()
                continue

            try:
                released_event = EventEnvelope(
                    event_type="InventoryReleased",
                    aggregate_id=event.aggregate_id,
                    payload={
                        "sku":
                            event.payload["sku"],

                        "quantity":
                            event.payload["quantity"],

                        "status":
                            "RELEASED",

                        "reason":
                            "PAYMENT_FAILED",
                    },
                )

                await p.send_and_wait(
                    "inventory.events",
                    released_event.model_dump(),
                )

                await complete_event(
                    "inventory-release",
                    event.event_id,
                )

                await c.commit()

                print(
                    "Inventory released for order "
                    f"{event.aggregate_id}"
                )

            except Exception as exc:
                await release_event(
                    "inventory-release",
                    event.event_id,
                )

                target_topic = await retry_or_dlq(
                    p,
                    "payments.events",
                    event,
                    msg,
                    exc,
                )

                await c.commit()

                print(
                    "Inventory compensation failed for "
                    f"order {event.aggregate_id}. "
                    f"Forwarded to {target_topic}. "
                    f"Error: {exc}"
                )

    finally:
        await c.stop()
        await p.stop()


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
async def startup():
    asyncio.create_task(
        reserve_inventory()
    )

    asyncio.create_task(
        compensate_payment_failure()
    )
