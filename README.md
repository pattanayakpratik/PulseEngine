# PulseEngine ⚡

> **Distributed Multi-Channel Notification & Dynamic Segmentation Infrastructure**  
> High-Throughput • Event-Driven • Fault-Tolerant • ACID Guaranteed

[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D.svg?logo=redis&logoColor=white)](https://redis.io/)
[![Next.js](https://img.shields.io/badge/Next.js-16-black.svg?logo=next.js&logoColor=white)](https://nextjs.org/)
[![Python](https://img.shields.io/badge/Python-3.12%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/)

---

## 📌 Problem Statement

Modern applications (live gaming backends, fintech, SaaS) rely heavily on multi-channel notifications (Push via FCM, WhatsApp, Transactional Email via SMTP/SES, In-App WebSockets). Standard implementations suffer from four critical production bottlenecks:

1. **Head-of-Line Blocking:** Massive marketing campaigns clog worker queues, causing critical OTPs and payment alerts to stall for minutes.
2. **Dual-Write Data Loss:** Emitting events directly to brokers inside API handlers loses data when network partitions occur post DB-commit.
3. **Duplicate Notifications:** Network retries cause users to receive repeated alerts without strict deduplication.
4. **Hardcoded User Segments:** Manual database queries are required every time growth/product teams define a new user cohort.

**PulseEngine** resolves these challenges by uniting an **ACID Transactional Outbox**, **Two-Lane Priority Queues**, **Redis Lua Token-Bucket Rate Limiter**, **Idempotency Middleware**, and an **AST-based Dynamic JSON-to-SQL Segment Resolver**.

---

## 🏛 System Architecture

```
                       +-----------------------------------+
                       |    Client Apps / Game Backends    |
                       +-----------------+-----------------+
                                         |
                         POST /v1/notify | (Header: Idempotency-Key)
                                         v
+---------------------------------------------------------------------------------+
| FastAPI Gateway Layer                                                           |
|  1. Redis Idempotency Check (24h TTL Lock)                                      |
|  2. Payload Validation (Pydantic v2)                                            |
|  3. Dynamic Segment Evaluation (JSON AST -> Parameterized SQL)                  |
+----------------------------------------+----------------------------------------+
                                         |
                      Single ACID Commit | (Notifications + Outbox_Events)
                                         v
+---------------------------------------------------------------------------------+
| PostgreSQL 16 (Primary Data Store)                                              |
|  - users (B-Tree + GIN JSONB Indexes)    - notification_logs (Audit Trail)      |
|  - segments (Dynamic JSON Rules)         - outbox_events (Pending Relay State)  |
+----------------------------------------+----------------------------------------+
                                         |
                    Async Outbox Poller  | (SELECT ... FOR UPDATE SKIP LOCKED)
                                         v
+---------------------------------------------------------------------------------+
| Redis 7 Broker & State Store                                                    |
|  +----------------------------------+    +-----------------------------------+  |
|  | CRITICAL LANE (Priority Queue)   |    | BULK LANE (Campaign Queue)        |  |
|  | - OTPs, Security, Payment Alerts |    | - Marketing, Game Broadcasts      |  |
|  | - SLA: p99 < 500ms               |    | - Batched via Server-Side Cursors |  |
|  +-----------------+----------------+    +-----------------+-----------------+  |
|                    |                                       |                    |
|                    +-------------------+-------------------+                    |
|                                        |                                        |
|  +-------------------------------------v-------------------------------------+  |
|  | Token-Bucket Rate Limiter (Atomic Redis Lua Script per Provider)          |  |
|  | Circuit Breaker State (Closed / Open / Half-Open per Provider)            |  |
|  +-------------------------------------+-------------------------------------+  |
+----------------------------------------|----------------------------------------+
                                         |
                                         v
+---------------------------------------------------------------------------------+
| Async Delivery Worker Pool (Python httpx Connection Pool)                       |
|  ├──> Firebase Cloud Messaging (FCM Push)                                       |
|  ├──> WhatsApp Cloud API / Twilio                                               |
|  ├──> SMTP / AWS SES (Transactional Email)                                      |
|  └──> Dead-Letter Queue (DLQ) & Exponential Backoff + Full Jitter Retry Engine |
+---------------------------------------------------------------------------------+
```

---

## 🚀 Core Engineering Features

- **🛡️ 24-Hour Idempotency Guard:** `Idempotency-Key` headers are processed atomically via Redis locks with cached responses to eliminate duplicate delivery under retries.
- **⚡ Dynamic JSON-to-SQL Resolver:** Evaluates arbitrary JSON filter ASTs into parameterized SQLAlchemy queries targeting indexed PostgreSQL JSONB columns, using `yield_per(5000)` streaming cursors.
- **📦 ACID Transactional Outbox:** Zero dual-write loss using background relay sweeps with PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED`.
- **🚦 Isolated Priority Lanes:** Critical operational notifications jump ahead of high-volume marketing batches.
- **⏱️ Redis Lua Token-Bucket Limiting:** Atomic rate limiting guarantees provider quotas (e.g. 80 msgs/sec WhatsApp, 50/sec SES) are strictly honored.
- **🔌 Resilient Circuit Breakers & DLQ:** Provider health tracking automatically trips to OPEN when failures exceed thresholds, redirecting to exponential backoff retries with full jitter and DLQ retention.
- **📊 Next.js 16 Mission Control UI:** Live dashboard featuring Visual Segment Builder, Chaos/Load Simulator, and real-time SSE telemetry streams.

---

## 📂 Repository Layout

```
pulse-engine/
├── docker-compose.yml              # Postgres 16, Redis 7, FastAPI, Workers, Prometheus
├── Dockerfile                      # Production container image
├── pyproject.toml                  # Python package specifications & dependencies
├── alembic/                        # Async database migrations
├── app/
│   ├── main.py                     # FastAPI app initialization & lifespan hooks
│   ├── config.py                   # Pydantic Settings (environment configurations)
│   ├── database.py                 # AsyncEngine & AsyncSession maker
│   ├── redis_client.py             # Redis connection pool & Lua script loader
│   ├── api/
│   │   ├── deps.py                 # Auth & Idempotency-Key dependencies
│   │   └── v1/
│   │       ├── notify.py           # Transactional dispatch routes
│   │       ├── segments.py         # Segment CRUD & live query preview
│   │       ├── campaigns.py        # Bulk fan-out triggers
│   │       └── metrics.py          # Prometheus & SSE telemetry endpoints
│   ├── core/
│   │   ├── segment_compiler.py     # JSON AST -> SQLAlchemy query compiler
│   │   ├── rate_limiter.py         # Token-Bucket Lua wrapper
│   │   ├── circuit_breaker.py      # Provider health state machine
│   │   └── telemetry.py            # OpenTelemetry spans & latency histograms
│   ├── models/                     # SQLAlchemy 2.0 ORM models
│   ├── schemas/                    # Pydantic v2 request/response schemas
│   ├── providers/                  # Pluggable channel adapters (Strategy Pattern)
│   │   ├── base.py                 # Abstract BaseProvider interface
│   │   ├── fcm_provider.py
│   │   ├── whatsapp_provider.py
│   │   └── smtp_provider.py
│   └── workers/
│       ├── outbox_relay.py         # Sweeps PostgreSQL outbox -> Redis queues
│       ├── critical_worker.py      # High-priority consumer loop
│       └── bulk_worker.py          # Batch consumer loop
├── scripts/
│   └── seed_100k_users.py          # Generates 100k realistic users for load testing
├── tests/
│   ├── test_idempotency.py
│   ├── test_segment_compiler.py
│   └── locustfile.py               # Stress test simulating 2,000 RPS spike
└── dashboard/                      # Next.js 16 (Turbopack) Live Control Plane
```

---

## 🗺️ Implementation Phases

- [x] **Phase 0:** Project Conception, Architecture Blueprints & Git Repo Setup
- [ ] **Phase 1:** Core Data Layer, Async SQLAlchemy 2.0 Models & Dynamic Segment Compiler
- [ ] **Phase 2:** Idempotency Middleware & Transactional Outbox Poller
- [ ] **Phase 3:** Priority Workers, Redis Lua Token Bucket, Circuit Breaker & Channel Adapters
- [ ] **Phase 4:** Next.js 16 Mission Control UI, Chaos Simulator & Locust Load Testing

---

## 📄 License

MIT License. Designed and engineered by [Pratik Pattanayak](https://github.com/pattanayakpratik).
