from services.common.tracing import install_tracing, current_trace_context
from services.common.metrics import install_metrics
import asyncio
import json
import time
from decimal import Decimal
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import (
    Boolean,
    Integer,
    Numeric,
    String,
    select,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import (
    Mapped,
    mapped_column,
)

from services.common.db import (
    Base,
    SessionLocal,
    engine,
)
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
    title="Orders Service",
    version="1.0.0",
)

install_metrics(app, "orders")
install_tracing(app, "orders")


# ============================================================
# DATABASE MODELS
# ============================================================

class Order(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
    )

    sku: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    quantity: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    amount: Mapped[Decimal] = mapped_column(
        Numeric(12, 2),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String,
        default="PENDING",
        nullable=False,
    )

    inventory_status: Mapped[str] = mapped_column(
        String,
        default="PENDING",
        nullable=False,
    )

    payment_status: Mapped[str] = mapped_column(
        String,
        default="PENDING",
        nullable=False,
    )

    simulate_payment_failure: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        nullable=False,
    )


class Outbox(Base):
    __tablename__ = "outbox"

    id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
    )

    topic: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    payload: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    published: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        nullable=False,
    )


# ============================================================
# REQUEST MODEL
# ============================================================

class OrderIn(BaseModel):
    sku: str = Field(
        min_length=1,
    )

    quantity: int = Field(
        gt=0,
    )

    amount: float = Field(
        gt=0,
    )

    simulate_payment_failure: bool = False

    simulate_inventory_error: bool = False


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "orders",
    }


# ============================================================
# CREATE ORDER
# ============================================================

@app.post(
    "/orders",
    status_code=201,
)
def create_order(
    data: OrderIn,
):
    order_id = str(
        uuid4()
    )

    event = EventEnvelope(
        event_type="OrderCreated",
        trace_context=current_trace_context(),
        aggregate_id=order_id,
        payload={
            "sku":
                data.sku,

            "quantity":
                data.quantity,

            "amount":
                data.amount,

            "simulate_payment_failure":
                data.simulate_payment_failure,

            "simulate_inventory_error":
                data.simulate_inventory_error,
        },
    )

    # --------------------------------------------------------
    # TRANSACTIONAL OUTBOX
    #
    # Order + event are committed in the SAME transaction.
    # --------------------------------------------------------

    with SessionLocal.begin() as db:

        db.add(
            Order(
                id=order_id,

                sku=data.sku,

                quantity=data.quantity,

                amount=Decimal(
                    str(data.amount)
                ),

                status="PENDING",

                inventory_status="PENDING",

                payment_status="PENDING",

                simulate_payment_failure=
                    data.simulate_payment_failure,
            )
        )

        db.add(
            Outbox(
                id=event.event_id,

                topic="orders.events",

                payload=
                    event.model_dump_json(),

                published=False,
            )
        )

    return {
        "order_id":
            order_id,

        "status":
            "PENDING",
    }


# ============================================================
# GET ORDER
# ============================================================

@app.get(
    "/orders/{order_id}"
)
def get_order(
    order_id: str,
):
    with SessionLocal() as db:

        order = db.get(
            Order,
            order_id,
        )

        if not order:
            raise HTTPException(
                status_code=404,
                detail="Order not found",
            )

        return {
            "order_id":
                order.id,

            "sku":
                order.sku,

            "quantity":
                order.quantity,

            "amount":
                float(order.amount),

            "status":
                order.status,

            "inventory_status":
                order.inventory_status,

            "payment_status":
                order.payment_status,
        }


# ============================================================
# OUTBOX PUBLISHER
# ============================================================

async def outbox_publisher():
    """
    Dedicated long-running transactional-outbox publisher.

    PostgreSQL remains the source of truth.

    If Kafka publication fails, the message remains
    unpublished and is retried on the next polling cycle.

    If Kafka publication succeeds but the database update
    fails, the event can be published again.

    Downstream consumer idempotency protects against that
    at-least-once delivery scenario.
    """

    print(
        "Outbox publisher started"
    )

    while True:

        kafka_producer = None

        try:
            kafka_producer = (
                await producer()
            )

            with SessionLocal() as db:

                rows = list(
                    db.scalars(
                        select(
                            Outbox
                        ).where(
                            Outbox.published
                            == False
                        )
                    ).all()
                )

                for row in rows:

                    payload = json.loads(
                        row.payload
                    )

                    await (
                        kafka_producer
                        .send_and_wait(
                            row.topic,
                            payload,
                        )
                    )

                    row.published = True

                    print(
                        "Published outbox event "
                        f"{row.id} "
                        f"to {row.topic}"
                    )

                db.commit()

        except Exception as exc:

            print(
                "Outbox publisher error: "
                f"{exc}"
            )

        finally:

            if kafka_producer:

                try:
                    await (
                        kafka_producer.stop()
                    )

                except Exception:
                    pass

        await asyncio.sleep(
            1
        )


# ============================================================
# INVENTORY EVENT CONSUMER
# ============================================================

async def consume_inventory_events():

    c = consumer(
        [
            "inventory.events",
            "inventory.events.retry",
        ],
        "orders-inventory",
    )

    await c.start()

    print(
        "Orders inventory consumer started"
    )

    try:

        async for msg in c:

            event = EventEnvelope(
                **msg.value
            )

            if event.event_type not in {
                "InventoryReserved",
                "InventoryReleased",
            }:

                await c.commit()

                continue

            acquired = await acquire_event(
                "orders-inventory",
                event.event_id,
            )

            if not acquired:

                print(
                    "Skipping duplicate "
                    "inventory event "
                    f"{event.event_id}"
                )

                await c.commit()

                continue

            try:

                with SessionLocal.begin() as db:

                    order = db.get(
                        Order,
                        event.aggregate_id,
                    )

                    if not order:

                        print(
                            "Order not found for "
                            "inventory event "
                            f"{event.aggregate_id}"
                        )

                    elif (
                        event.event_type
                        == "InventoryReserved"
                    ):

                        order.inventory_status = (
                            "RESERVED"
                        )

                        if (
                            order.status
                            == "PENDING"
                        ):

                            order.status = (
                                "PROCESSING"
                            )

                        print(
                            "Inventory reserved "
                            f"for order "
                            f"{order.id}"
                        )

                    elif (
                        event.event_type
                        == "InventoryReleased"
                    ):

                        order.inventory_status = (
                            "RELEASED"
                        )

                        order.status = (
                            "CANCELLED"
                        )

                        print(
                            "Order cancelled "
                            "after compensation "
                            f"{order.id}"
                        )

                await complete_event(
                    "orders-inventory",
                    event.event_id,
                )

                await c.commit()

            except Exception:

                await release_event(
                    "orders-inventory",
                    event.event_id,
                )

                raise

    finally:

        await c.stop()


# ============================================================
# PAYMENT EVENT CONSUMER
# ============================================================

async def consume_payment_events():

    c = consumer(
        [
            "payments.events",
            "payments.events.retry",
        ],
        "orders-payments",
    )

    await c.start()

    print(
        "Orders payment consumer started"
    )

    try:

        async for msg in c:

            event = EventEnvelope(
                **msg.value
            )

            if event.event_type not in {
                "PaymentAuthorized",
                "PaymentFailed",
            }:

                await c.commit()

                continue

            acquired = await acquire_event(
                "orders-payments",
                event.event_id,
            )

            if not acquired:

                print(
                    "Skipping duplicate "
                    "payment event "
                    f"{event.event_id}"
                )

                await c.commit()

                continue

            try:

                with SessionLocal.begin() as db:

                    order = db.get(
                        Order,
                        event.aggregate_id,
                    )

                    if not order:

                        print(
                            "Order not found for "
                            "payment event "
                            f"{event.aggregate_id}"
                        )

                    elif (
                        event.event_type
                        == "PaymentAuthorized"
                    ):

                        order.payment_status = (
                            "AUTHORIZED"
                        )

                        order.status = (
                            "CONFIRMED"
                        )

                        print(
                            "Order confirmed "
                            f"{order.id}"
                        )

                    elif (
                        event.event_type
                        == "PaymentFailed"
                    ):

                        order.payment_status = (
                            "FAILED"
                        )

                        order.status = (
                            "COMPENSATING"
                        )

                        print(
                            "Payment failed. "
                            "Compensation started "
                            f"for {order.id}"
                        )

                await complete_event(
                    "orders-payments",
                    event.event_id,
                )

                await c.commit()

            except Exception:

                await release_event(
                    "orders-payments",
                    event.event_id,
                )

                raise

    finally:

        await c.stop()


# ============================================================
# DATABASE STARTUP
# ============================================================

def initialize_database():

    retries = 10

    for attempt in range(
        1,
        retries + 1,
    ):

        try:

            Base.metadata.create_all(
                engine
            )

            print(
                "Database connection established"
            )

            return

        except OperationalError:

            if attempt == retries:

                raise

            print(
                "Database not ready. "
                "Retrying in 2 seconds "
                f"({attempt}/{retries})"
            )

            time.sleep(
                2
            )


# ============================================================
# APPLICATION STARTUP
# ============================================================

@app.on_event("startup")
async def startup():

    initialize_database()

    asyncio.create_task(
        outbox_publisher()
    )

    asyncio.create_task(
        consume_inventory_events()
    )

    asyncio.create_task(
        consume_payment_events()
    )
