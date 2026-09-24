import asyncio
from fastapi import FastAPI
from services.common.kafka import consumer, producer
from services.common.events import EventEnvelope

app = FastAPI(title="Payments Service")
processed: set[str] = set()

@app.get("/health")
def health():
    return {"status":"ok","service":"payments"}

async def consume_orders():
    c = consumer("orders.events", "payments")
    p = await producer()
    await c.start()
    try:
        async for msg in c:
            e = EventEnvelope(**msg.value)
            if e.event_id in processed:
                await c.commit()
                continue
            processed.add(e.event_id)
            payment = EventEnvelope(
                event_type="PaymentAuthorized",
                aggregate_id=e.aggregate_id,
                payload={"amount": e.payload["amount"], "status":"AUTHORIZED"}
            )
            await p.send_and_wait("payments.events", payment.model_dump())
            await c.commit()
    finally:
        await c.stop(); await p.stop()

@app.on_event("startup")
async def startup():
    asyncio.create_task(consume_orders())
