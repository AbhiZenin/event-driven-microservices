import os
import time
import uuid

import httpx
import pytest


GATEWAY_URL = os.getenv(
    "GATEWAY_URL",
    "http://localhost:8088",
)

REQUEST_TIMEOUT = 10.0
SAGA_TIMEOUT = 30.0


@pytest.fixture(scope="session")
def token():
    response = httpx.post(
        f"{GATEWAY_URL}/auth/token",
        json={
            "username": "demo",
            "password": "demo-password",
        },
        timeout=REQUEST_TIMEOUT,
    )

    assert response.status_code == 200

    body = response.json()

    assert "access_token" in body

    return body["access_token"]


def auth_headers(token):
    return {
        "Authorization": f"Bearer {token}"
    }


def wait_for_order(
    order_id,
    token,
    expected_status,
):
    deadline = time.time() + SAGA_TIMEOUT

    last_body = None

    while time.time() < deadline:
        response = httpx.get(
            f"{GATEWAY_URL}/api/orders/{order_id}",
            headers=auth_headers(token),
            timeout=REQUEST_TIMEOUT,
        )

        assert response.status_code == 200

        last_body = response.json()

        if (
            last_body.get("status")
            == expected_status
        ):
            return last_body

        time.sleep(0.5)

    pytest.fail(
        f"Order {order_id} did not reach "
        f"{expected_status}. "
        f"Last state: {last_body}"
    )


def test_gateway_health():
    response = httpx.get(
        f"{GATEWAY_URL}/health",
        timeout=REQUEST_TIMEOUT,
    )

    assert response.status_code == 200

    body = response.json()

    assert body["status"] == "ok"
    assert body["service"] == "gateway"
    assert body["redis"] is True


def test_protected_endpoint_requires_authentication():
    response = httpx.get(
        f"{GATEWAY_URL}/api/orders/test-order"
    )

    assert response.status_code in (
        401,
        403,
    )


def test_invalid_jwt_is_rejected():
    response = httpx.get(
        f"{GATEWAY_URL}/api/orders/test-order",
        headers={
            "Authorization":
                "Bearer invalid-token"
        },
    )

    assert response.status_code == 401


def test_happy_path_saga(token):
    sku = (
        "INTEGRATION-HAPPY-"
        + uuid.uuid4().hex[:8]
    )

    response = httpx.post(
        f"{GATEWAY_URL}/api/orders",
        headers=auth_headers(token),
        json={
            "sku": sku,
            "quantity": 1,
            "amount": 499.99,
            "simulate_payment_failure": False,
            "simulate_inventory_error": False,
        },
        timeout=REQUEST_TIMEOUT,
    )

    assert response.status_code == 201

    body = response.json()

    assert body["status"] == "PENDING"

    order_id = body["order_id"]

    final_order = wait_for_order(
        order_id,
        token,
        "CONFIRMED",
    )

    assert (
        final_order["inventory_status"]
        == "RESERVED"
    )

    assert (
        final_order["payment_status"]
        == "AUTHORIZED"
    )


def test_payment_failure_compensation(token):
    sku = (
        "INTEGRATION-COMPENSATION-"
        + uuid.uuid4().hex[:8]
    )

    response = httpx.post(
        f"{GATEWAY_URL}/api/orders",
        headers=auth_headers(token),
        json={
            "sku": sku,
            "quantity": 1,
            "amount": 199.99,
            "simulate_payment_failure": True,
            "simulate_inventory_error": False,
        },
        timeout=REQUEST_TIMEOUT,
    )

    assert response.status_code == 201

    order_id = response.json()[
        "order_id"
    ]

    final_order = wait_for_order(
        order_id,
        token,
        "CANCELLED",
    )

    assert (
        final_order["payment_status"]
        == "FAILED"
    )

    assert (
        final_order["inventory_status"]
        == "RELEASED"
    )
