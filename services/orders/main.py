import asyncio, os
from uuid import uuid4
from decimal import Decimal
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy import String, Numeric, Integer, Boolean, select
from sqlalchemy.orm import Mapped, mapped_column
from services.common.db import Base, engine, SessionLocal
from services.common.events import EventEnvelope
from services.common.kafka import producer

app = FastAPI(title="Orders Service")

class Order(Base):
    __tablename__ = "orders"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    sku: Mapped[str] = mapped_column(String)
    quantity: Mapped[int] = mapped_column(Integer)
    amount: Mapped[Decimal] = mapped_column(Numeric(12,2))
    status: Mapped[str] = mapped_column(String, default="PENDING")

class Outbox(Base):
    __tablename__ = "outbox"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    topic: Mapped[str] = mapped_column(String)
    payload: Mapped[str] = mapped_column(String)
    published: Mapped[bool] = mapped_column(Boolean, default=False)

class OrderIn(BaseModel):
    sku: str
    quantity: int = Field(gt=0)
    amount: float = Field(gt=0)

@app.on_event("startup")
def startup():
    retries = 10

    for attempt in range(1, retries + 1):
        try:
            Base.metadata.create_all(engine)
            print("Database connection established")
            return
        except OperationalError as exc:
            if attempt == retries:
                raise

            print(
                f"Database not ready. "
                f"Retrying in 2 seconds "
                f"({attempt}/{retries})"
            )

            time.sleep(2)
@app.get("/health")
def health():
    return {"status":"ok","service":"orders"}

@app.post("/orders", status_code=201)
def create_order(data: OrderIn, bg: BackgroundTasks):
    import json
    oid = str(uuid4())
    evt = EventEnvelope(event_type="OrderCreated", aggregate_id=oid, payload=data.model_dump())
    with SessionLocal.begin() as db:
        db.add(Order(id=oid, sku=data.sku, quantity=data.quantity, amount=data.amount, status="PENDING"))
        db.add(Outbox(id=evt.event_id, topic="orders.events", payload=evt.model_dump_json(), published=False))
    bg.add_task(flush_outbox)
    return {"order_id":oid,"status":"PENDING"}

async def flush_outbox():
    p = await producer()
    try:
        with SessionLocal() as db:
            rows = list(db.scalars(select(Outbox).where(Outbox.published == False)).all())
            for row in rows:
                import json
                await p.send_and_wait(row.topic, json.loads(row.payload))
                row.published = True
            db.commit()
    finally:
        await p.stop()
