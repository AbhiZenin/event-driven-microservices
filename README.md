# Real-Time Event-Driven Microservices Platform

A runnable portfolio project demonstrating FastAPI microservices, Kafka eventing, PostgreSQL outbox processing, idempotent consumers, Redis, JWT-ready API boundaries, observability, Docker, Kubernetes, and CI.

## Services
- `orders` — creates orders and writes `OrderCreated` to an outbox table
- `payments` — consumes order events and emits `PaymentAuthorized`
- `inventory` — reserves stock idempotently
- `notifications` — receives domain events and records notification delivery
- Kafka/Redpanda — event backbone
- PostgreSQL — transactional store
- Redis — cache/idempotency support

## Patterns demonstrated
- Transactional outbox
- Saga-style event choreography
- Idempotent consumers
- Retry-safe event envelope
- Health/readiness endpoints
- Prometheus-compatible metrics endpoint
- Docker Compose
- Kubernetes manifests
- GitHub Actions

## Start locally

```bash
docker compose up --build
```

Create an order:

```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{"sku":"LAPTOP-001","quantity":1,"amount":1299.00}'
```

Check:
- Orders API: http://localhost:8001/docs
- Payments: http://localhost:8002/health
- Inventory: http://localhost:8003/health
- Notifications: http://localhost:8004/health

## Repository layout

```text
services/
  common/
  orders/
  payments/
  inventory/
  notifications/
infra/k8s/
tests/
```

## Resume alignment
The code implements the key architecture behind:
- event-driven microservices
- saga/event choreography
- outbox pattern
- idempotent consumers
- Redis-assisted resilience
- CI + Kubernetes manifests
- observability-ready endpoints
