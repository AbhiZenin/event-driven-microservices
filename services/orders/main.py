import asyncio
import time
from decimal import Decimal
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import Boolean, Integer, Numeric, String, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Mapped, mapped_column

from services.common.db import Base, SessionLocal, engine
from services.common.events import EventEnvelope
from services.common.idempotency import (
    acquire_event,
    complete_event,
    release_event,
)
from services.common.kafka import consumer, producer


app = FastAPI(title="Orders Service")


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String, primary_key=True)

    sku: Mapped[str] = mapped_column(String)
    quantity: Mapped[int] = mapped_column(Integer)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2))

    status: Mapped[str] = mapped_column(
        String,
        default="PENDING",
    )

    inventory_status: Mapped[str] = mapped_column(
        String,
        default="PENDING",
    )

    payment_status: Mapped[str] = mapped_column(
        String,
        default="PENDING",
    )

    simulate_payment_failure: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
    )


class Outbox(Base):
    __tablename__ = "outbox"

    id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
    )

    topic: Mapped[str] = mapped_column(String)

    payload: Mapped[str] = mapped_column(String)

    published: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
    )


class OrderIn(BaseModel):
    sku: str

    quantity: int = Field(gt=0)

    amount: float = Field(gt=0)

    simulate_payment_failure: bool = False


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "orders",
    }


@app.post("/orders", status_code=201)
def create_order(
    data: OrderIn,
    bg: BackgroundTasks,
):
    order_id = str(uuid4())

    event = EventEnvelope(
        event_type="OrderCreated",
        aggregate_id=order_id,
        payload={
            "sku": data.sku,
            "quantity": data.quantity,
            "amount": data.amount,
            "simulate_payment_failure":
                data.simulate_payment_failure,
        },
    )

    with SessionLocal.begin() as db:
        db.add(
            Order(
                id=order_id,
                sku=data.sku,
                quantity=data.quantity,
                amount=Decimal(str(data.amount)),
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
                payload=event.model_dump_json(),
                published=False,
            )
        )

    bg.add_task(flush_outbox)

    return {
        "order_id": order_id,
        "status": "PENDING",
    }


@app.get("/orders/{order_id}")
def get_order(order_id: str):
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
            "order_id": order.id,
            "sku": order.sku,
            "quantity": order.quantity,
            "amount": float(order.amount),
            "status": order.status,
            "inventory_status":
                order.inventory_status,
            "payment_status":
                order.payment_status,
        }


async def flush_outbox():
    p = await producer()

    try:
        with SessionLocal() as db:
            rows = list(
                db.scalars(
                    select(Outbox).where(
                        Outbox.published == False
                    )
                ).all()
            )

            for row in rows:
                import json

                await p.send_and_wait(
                    row.topic,
                    json.loads(row.payload),
                )

                row.published = True

            db.commit()

    finally:
        await p.stop()


async def consume_inventory_events():
    c = consumer(
        "inventory.events",
        "orders-inventory",
    )

    await c.start()

    try:
        async for msg in c:
            event = EventEnvelope(**msg.value)

            acquired = await acquire_event(
                "orders-inventory",
                event.event_id,
            )

            if not acquired:
                await c.commit()
                continue

            try:
                with SessionLocal.begin() as db:
                    order = db.get(
                        Order,
                        event.aggregate_id,
                    )

                    if not order:
                        await complete_event(
                            "orders-inventory",
                            event.event_id,
                        )

                        await c.commit()
                        continue

                    if (
                        event.event_type
                        == "InventoryReserved"
                    ):
                        order.inventory_status = (
                            "RESERVED"
                        )

                        if order.status == "PENDING":
                            order.status = "PROCESSING"

                    elif (
                        event.event_type
                        == "InventoryReleased"
                    ):
                        order.inventory_status = (
                            "RELEASED"
                        )

                        order.status = "CANCELLED"

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


async def consume_payment_events():
    c = consumer(
        "payments.events",
        "orders-payments",
    )

    await c.start()

    try:
        async for msg in c:
            event = EventEnvelope(**msg.value)

            acquired = await acquire_event(
                "orders-payments",
                event.event_id,
            )

            if not acquired:
                await c.commit()
                continue

            try:
                with SessionLocal.begin() as db:
                    order = db.get(
                        Order,
                        event.aggregate_id,
                    )

                    if not order:
                        await complete_event(
                            "orders-payments",
                            event.event_id,
                        )

                        await c.commit()
                        continue

                    if (
                        event.event_type
                        == "PaymentAuthorized"
                    ):
                        order.payment_status = (
                            "AUTHORIZED"
                        )

                        order.status = "CONFIRMED"

                    elif (
                        event.event_type
                        == "PaymentFailed"
                    ):
                        order.payment_status = "FAILED"

                        order.status = (
                            "COMPENSATING"
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


def initialize_database():
    retries = 10

    for attempt in range(
        1,
        retries + 1,
    ):
        try:
            Base.metadata.create_all(engine)

            print(
                "Database connection established"
            )

            return

        except OperationalError:
            if attempt == retries:
                raise

            print(
                "Database not ready. "
                f"Retrying ({attempt}/{retries})"
            )

            time.sleep(2)


@app.on_event("startup")
async def startup():
    initialize_database()

    asyncio.create_task(
        consume_inventory_events()
    )

    asyncio.create_task(
        consume_payment_events()
    )
