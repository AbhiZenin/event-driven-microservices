import os
from datetime import datetime, timedelta, timezone

import httpx
from opentelemetry import propagate
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Request,
    status,
)
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from redis.asyncio import Redis

from services.common.metrics import install_metrics
from services.common.tracing import install_tracing


SERVICE_NAME = "gateway"

JWT_SECRET = os.getenv(
    "JWT_SECRET",
    "dev-only-change-me",
)

JWT_ALGORITHM = "HS256"

JWT_EXPIRE_MINUTES = int(
    os.getenv(
        "JWT_EXPIRE_MINUTES",
        "60",
    )
)

ORDERS_URL = os.getenv(
    "ORDERS_SERVICE_URL",
    "http://orders:8001",
)

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://redis:6379/0",
)

RATE_LIMIT = int(
    os.getenv(
        "RATE_LIMIT_REQUESTS",
        "60",
    )
)

RATE_WINDOW_SECONDS = int(
    os.getenv(
        "RATE_LIMIT_WINDOW_SECONDS",
        "60",
    )
)

DEMO_USER = os.getenv(
    "DEMO_USER",
    "demo",
)

DEMO_PASSWORD = os.getenv(
    "DEMO_PASSWORD",
    "demo-password",
)


app = FastAPI(
    title="Event-Driven Platform API Gateway",
    version="1.0.0",
)

install_metrics(
    app,
    SERVICE_NAME,
)

install_tracing(
    app,
    SERVICE_NAME,
)


redis_client = Redis.from_url(
    REDIS_URL,
    decode_responses=True,
)

bearer = HTTPBearer()


def tracing_headers() -> dict[str, str]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


class LoginRequest(BaseModel):
    username: str
    password: str


class OrderRequest(BaseModel):
    sku: str
    quantity: int
    amount: float
    simulate_payment_failure: bool = False
    simulate_inventory_error: bool = False


def create_access_token(
    subject: str,
) -> str:
    now = datetime.now(
        timezone.utc
    )

    payload = {
        "sub": subject,
        "iat": now,
        "exp": (
            now
            + timedelta(
                minutes=JWT_EXPIRE_MINUTES
            )
        ),
    }

    return jwt.encode(
        payload,
        JWT_SECRET,
        algorithm=JWT_ALGORITHM,
    )


async def current_user(
    credentials: HTTPAuthorizationCredentials = Depends(
        bearer
    ),
) -> str:
    try:
        payload = jwt.decode(
            credentials.credentials,
            JWT_SECRET,
            algorithms=[
                JWT_ALGORITHM
            ],
        )

        subject = payload.get(
            "sub"
        )

        if not subject:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token",
            )

        return subject

    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )


async def enforce_rate_limit(
    user: str = Depends(
        current_user
    ),
):
    current_window = int(
        datetime.now(
            timezone.utc
        ).timestamp()
        // RATE_WINDOW_SECONDS
    )

    key = (
        f"rate-limit:"
        f"{user}:"
        f"{current_window}"
    )

    count = await redis_client.incr(
        key
    )

    if count == 1:
        await redis_client.expire(
            key,
            RATE_WINDOW_SECONDS + 1,
        )

    if count > RATE_LIMIT:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
        )

    return user


@app.get("/health")
async def health():
    try:
        redis_ok = (
            await redis_client.ping()
        )

    except Exception:
        redis_ok = False

    return {
        "status":
            "ok"
            if redis_ok
            else "degraded",

        "service":
            SERVICE_NAME,

        "redis":
            bool(redis_ok),
    }


@app.post("/auth/token")
async def login(
    request: LoginRequest,
):
    if (
        request.username
        != DEMO_USER
        or
        request.password
        != DEMO_PASSWORD
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid credentials",
        )

    token = create_access_token(
        request.username
    )

    return {
        "access_token":
            token,

        "token_type":
            "bearer",

        "expires_in":
            JWT_EXPIRE_MINUTES
            * 60,
    }


@app.post(
    "/api/orders",
    status_code=201,
)
async def create_order(
    order: OrderRequest,
    user: str = Depends(
        enforce_rate_limit
    ),
):
    async with httpx.AsyncClient(
        timeout=10
    ) as client:

        response = await client.post(
            f"{ORDERS_URL}/orders",
            json=order.model_dump(),
            headers=tracing_headers(),
        )

    if response.status_code >= 400:
        raise HTTPException(
            status_code=
                response.status_code,

            detail=
                response.text,
        )

    result = response.json()

    result[
        "requested_by"
    ] = user

    return result


@app.get(
    "/api/orders/{order_id}"
)
async def get_order(
    order_id: str,
    user: str = Depends(
        enforce_rate_limit
    ),
):
    async with httpx.AsyncClient(
        timeout=10
    ) as client:

        response = await client.get(
            f"{ORDERS_URL}/orders/"
            f"{order_id}",
            headers=tracing_headers(),
        )

    if (
        response.status_code
        == 404
    ):
        raise HTTPException(
            status_code=404,
            detail="Order not found",
        )

    if response.status_code >= 400:
        raise HTTPException(
            status_code=
                response.status_code,

            detail=
                response.text,
        )

    return response.json()
