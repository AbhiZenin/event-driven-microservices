import asyncio
from fastapi import FastAPI
from services.common.kafka import consumer

app = FastAPI(title="Notifications Service")
received = []

@app.get("/health")
def health():
    return {"status":"ok","service":"notifications"}

@app.get("/notifications")
def notifications():
    return received[-100:]

async def consume():
    consumers = [
        consumer("payments.events", "notifications-payments"),
        consumer("inventory.events", "notifications-inventory"),
    ]
    for c in consumers: await c.start()
    async def drain(c):
        async for msg in c:
            received.append(msg.value)
            await c.commit()
    try:
        await asyncio.gather(*(drain(c) for c in consumers))
    finally:
        for c in consumers: await c.stop()

@app.on_event("startup")
async def startup():
    asyncio.create_task(consume())
