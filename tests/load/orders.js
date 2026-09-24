import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL =
  __ENV.BASE_URL ||
  "http://gateway:8080";

export const options = {
  stages: [
    {
      duration: "15s",
      target: 5,
    },
    {
      duration: "30s",
      target: 10,
    },
    {
      duration: "15s",
      target: 0,
    },
  ],

  thresholds: {
    http_req_failed: [
      "rate<0.05",
    ],

    http_req_duration: [
      "p(95)<1000",
    ],

    checks: [
      "rate>0.95",
    ],
  },
};


export function setup() {
  const response = http.post(
    `${BASE_URL}/auth/token`,
    JSON.stringify({
      username: "demo",
      password: "demo-password",
    }),
    {
      headers: {
        "Content-Type":
          "application/json",
      },
    }
  );

  check(response, {
    "authentication succeeds":
      (r) => r.status === 200,
  });

  return {
    token:
      response.json(
        "access_token"
      ),
  };
}


export default function (data) {
  const payload = JSON.stringify({
    sku:
      `LOAD-${__VU}-${__ITER}-${Date.now()}`,

    quantity: 1,

    amount: 99.99,

    simulate_payment_failure:
      false,

    simulate_inventory_error:
      false,
  });

  const response = http.post(
    `${BASE_URL}/api/orders`,
    payload,
    {
      headers: {
        "Content-Type":
          "application/json",

        Authorization:
          `Bearer ${data.token}`,
      },
    }
  );

  check(response, {
    "order accepted":
      (r) => r.status === 201,

    "order id returned":
      (r) => {
        try {
          return Boolean(
            r.json("order_id")
          );
        } catch (_) {
          return false;
        }
      },
  });

  sleep(0.2);
}
