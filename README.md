# PulseEngine ⚡

> **Distributed Multi-Channel Notification & Dynamic Segmentation Infrastructure**  
> High-Throughput • Event-Driven • Fault-Tolerant • ACID Guaranteed

[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D.svg?logo=redis&logoColor=white)](https://redis.io/)
[![Next.js](https://img.shields.io/badge/Next.js-16-black.svg?logo=next.js&logoColor=white)](https://nextjs.org/)
[![Vercel Deployment](https://img.shields.io/badge/Vercel-Deployed-black.svg?logo=vercel&logoColor=white)](https://pulse-engine-dashboard-ten.vercel.app)
[![Python](https://img.shields.io/badge/Python-3.12%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Tests Passing](https://img.shields.io/badge/Tests-13%2F13%20Passing-emerald.svg)](tests/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📌 Executive Summary & Problem Statement

Modern applications—ranging from live gaming backends to fintech and SaaS platforms—require reliable, multi-channel communication (Push Notifications via FCM, WhatsApp Cloud API, Transactional Email via SMTP/SES, and Webhooks). However, naive notification systems suffer from four fatal production bottlenecks:

1. **Head-of-Line Blocking:** A 500,000-user marketing broadcast clogs worker queues, delaying time-critical OTPs and security alerts by 15+ minutes.
2. **Dual-Write Data Loss:** Emitting events directly to brokers inside HTTP route handlers causes data loss when the broker momentarily disconnects post-commit.
3. **Duplicate Notifications:** Network retries cause users to receive repeated alerts without strict, atomic deduplication.
4. **Hardcoded User Segments:** Engineering teams waste hours writing manual SQL queries whenever product teams target dynamic user cohorts.

**PulseEngine** solves these problems by combining an **ACID Transactional Outbox**, **Two-Lane Priority Isolation**, **Redis Lua Token-Bucket Rate Limiter**, **24-hour Idempotency Guard**, and an **AST-based Dynamic JSON-to-SQL Segment Resolver**.

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
|  1. Redis Idempotency Check (24h TTL Lock: SET NX EX 86400)                     |
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

- **🛡️ 24-Hour Idempotency Guard:** `Idempotency-Key` headers are processed atomically via Redis locks with cached responses to eliminate duplicate delivery under retries (`SET NX EX 86400`).
- **⚡ Dynamic JSON-to-SQL Resolver:** Evaluates arbitrary JSON filter ASTs into parameterized SQLAlchemy queries targeting indexed PostgreSQL JSONB columns, utilizing `yield_per(5000)` streaming server-side cursors.
- **📦 ACID Transactional Outbox:** Zero dual-write loss using background relay sweeps with PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED`.
- **🚦 Isolated Priority Lanes:** Critical operational notifications (`queue:critical`) jump ahead of high-volume marketing batches (`queue:bulk`).
- **⏱️ Redis Lua Token-Bucket Limiting:** Atomic rate limiting guarantees provider quotas (e.g., 500 RPS FCM, 80 RPS WhatsApp, 50 RPS SES) are strictly honored.
- **🔌 Resilient Circuit Breakers & DLQ:** Provider health tracking automatically trips to `OPEN` when failure rates exceed 50%, redirecting messages to exponential backoff with full jitter and DLQ retention.
- **📊 Next.js 16 Mission Control UI:** Sleek dashboard featuring Visual Segment Builder, Chaos/Load Simulator, and real-time SSE telemetry streams.

---

## 🖥️ Next.js 16 Interactive Portfolio Demo

> 🌐 **Live Production Deployment:** [https://pulse-engine-dashboard-ten.vercel.app](https://pulse-engine-dashboard-ten.vercel.app)

Built with **Next.js 16 (Turbopack)** and deployed directly on Vercel:

1. **Visual Segment Builder & SQL Inspector:** Click rules in the UI to dynamically compile complex AST filters into parameterized SQL queries with PostgreSQL GIN index scan timing (e.g. *Matched 14,280 users in 11.4ms via GIN Index*).
2. **Chaos & Priority Simulator:**
   - **"Trigger 10,000 Bulk Campaign + Send 1 Critical OTP":** Visually proves in real time that the Critical OTP jumps the line and delivers in **< 150ms** while the bulk queue drains steadily at the rate-limited cap.
   - **"Simulate SMTP Provider Outage":** Trips the **Circuit Breaker** badge from Green (`CLOSED`) to Red (`OPEN`) and routes failed messages cleanly into the Dead-Letter Queue with zero thread stalls.
3. **Live Metrics Terminal:** Streams `p99_latency_ms`, active worker states, and queue depths via Server-Sent Events (SSE).

---

## 📖 REST API Specification

| Method | Endpoint | Description | Auth / Headers |
| :--- | :--- | :--- | :--- |
| `POST` | `/v1/notify` | Send a single transactional notification (Critical or Bulk) | `X-API-Key`, `Idempotency-Key` |
| `POST` | `/v1/segments` | Create a new dynamic user segment using JSON AST rules | `X-API-Key` |
| `POST` | `/v1/segments/preview` | Preview AST query, count matching users, and inspect compiled SQL | `X-API-Key` |
| `POST` | `/v1/campaigns/dispatch` | Trigger a bulk notification blast to a specific `segment_id` | `X-API-Key`, `Idempotency-Key` |
| `POST` | `/v1/dlq/{log_id}/replay` | Manually replay a dead-lettered message from the dashboard | `X-API-Key` |
| `POST` | `/v1/chaos/circuit-breaker` | Chaos engineering hook: toggle provider circuit breaker in Redis | Public / Demo |
| `GET` | `/v1/metrics/summary` | Real-time RPS, queue depths, circuit breaker states, and p99 latency | Public / Read-Only |
| `GET` | `/v1/stream/telemetry` | Server-Sent Events (SSE) telemetry stream pushing live metrics | Public / Read-Only |

### Sample Request: `POST /v1/notify`
```bash
curl -X POST http://localhost:8000/v1/notify \
  -H "Content-Type: application/json" \
  -H "X-API-Key: pulse_live_test_api_key_884920" \
  -H "Idempotency-Key: c9b29fd0-5bb2-402f-b4b3-066cb5fb4902" \
  -d '{
    "external_user_id": "usr_99481a",
    "channel": "WHATSAPP",
    "priority": "CRITICAL",
    "template_id": "otp_login_v1",
    "variables": {
      "user_name": "Pratik",
      "otp_code": "482910",
      "expiry_minutes": 5
    }
  }'
```

### Sample Request: `POST /v1/segments/preview`
```json
{
  "filter_ast": {
    "logic": "AND",
    "rules": [
      {"field": "country", "op": "eq", "value": "IN"},
      {"field": "last_active_at", "op": "lt", "value": "NOW() - INTERVAL '7 days'"},
      {"field": "metadata.tier", "op": "eq", "value": "whale"},
      {"field": "metadata.lifetime_spend", "op": "gte", "value": 5000}
    ]
  },
  "limit": 10
}
```

---

## 🛠️ Quickstart & Local Setup

### Option A: Using Docker Compose (Recommended)
Ensure Docker Desktop is running, then execute:
```bash
docker compose up --build
```
This boots up:
- **PostgreSQL 16** (port `5432`)
- **Redis 7** (port `6379`)
- **FastAPI Gateway** (port `8000`)
- **Outbox Relay Daemon**
- **Critical Worker Pool**
- **Bulk Campaign Worker Pool**

### Option B: Local Development Environment

#### 1. Python Backend Setup
```powershell
# Create and activate virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Run database migrations
alembic upgrade head

# Seed 100,000 realistic users for load testing
python scripts/seed_100k_users.py 100000

# Start FastAPI Gateway
uvicorn app.main:app --reload --port 8000
```

#### 2. Start Background Workers (in separate terminals)
```powershell
# Terminal 2: Outbox Sweeper Relay
python -m app.workers.outbox_relay

# Terminal 3: Dedicated Critical Lane Worker
python -m app.workers.critical_worker

# Terminal 4: Bulk Campaign Worker
python -m app.workers.bulk_worker
```

#### 3. Start Next.js 16 Dashboard
```powershell
cd dashboard
npm install
npm run dev
```
Access the control dashboard at [http://localhost:3000](http://localhost:3000).

---

## 🧪 Testing & Load Benchmarks

### Run Automated Unit & Integration Tests
```powershell
pytest tests/ -v
```
Verified Test Suite (13/13 Passing):
- `tests/test_segment_compiler.py`: Parameterized SQL AST compiler, nested `AND`/`OR` logic, JSONB `@>` GIN index containment.
- `tests/test_idempotency.py`: Atomic 24h Redis lock, 409 Conflict rejection, and 200 OK cached replay.
- `tests/test_circuit_breaker.py`: Rolling failure rate evaluation, state machine transitions (`CLOSED` → `OPEN` → `HALF_OPEN` → `CLOSED`), EMAIL/SMTP mapping.
- `tests/test_dlq_replay.py`: Full contact restoration (`external_user_id`, `user_email`, `user_phone`, `user_fcm`) on dead-lettered message replay.
- `tests/test_campaign_dispatch.py`: Memory-safe batch flushing with `flush()` and `expunge()` preserving PostgreSQL server-side cursors without mid-stream commit.
- `tests/test_bulk_worker.py`: Non-blocking concurrent 50-item processing with `asyncio.gather` and zero-loss message preservation on shutdown.
- `tests/test_critical_worker.py`: Non-blocking retry scheduling in dedicated high-priority queue protecting strict p99 < 500ms SLA.

### Run 2,000 RPS Stress Benchmark (Locust)
```powershell
locust -f tests/locustfile.py --host http://localhost:8000
```
Open [http://localhost:8089](http://localhost:8089) to start load generation simulating concurrent user blasts.

---

## 📂 Repository Layout

```
pulse-engine/
├── docker-compose.yml              # Postgres 16, Redis 7, FastAPI, Workers
├── Dockerfile                      # Production container image
├── pyproject.toml                  # Python package specifications & dependencies
├── requirements.txt                # Pinned dependencies
├── alembic.ini                     # Alembic configuration
├── alembic/                        # Async database migrations
│   ├── env.py
│   └── versions/
│       └── 0001_initial_schema.py
├── app/
│   ├── main.py                     # FastAPI app initialization & lifespan hooks
│   ├── config.py                   # Pydantic Settings (environment configurations)
│   ├── database.py                 # AsyncEngine & AsyncSession maker
│   ├── redis_client.py             # Redis connection pool & IdempotencyManager
│   ├── api/
│   │   ├── deps.py                 # Auth & Idempotency-Key dependencies
│   │   └── v1/
│   │       ├── notify.py           # Transactional dispatch routes
│   │       ├── segments.py         # Segment CRUD & live query preview
│   │       ├── campaigns.py        # Bulk fan-out triggers & DLQ replay
│   │       └── metrics.py          # Prometheus & SSE telemetry endpoints
│   ├── core/
│   │   ├── segment_compiler.py     # JSON AST -> SQLAlchemy query compiler
│   │   ├── rate_limiter.py         # Token-Bucket Lua wrapper
│   │   └── circuit_breaker.py      # Provider health state machine
│   ├── models/                     # SQLAlchemy 2.0 ORM models (User, Segment, Outbox, Logs)
│   ├── schemas/                    # Pydantic v2 request/response schemas
│   ├── providers/                  # Pluggable channel adapters (Strategy Pattern)
│   │   ├── base.py                 # Abstract BaseProvider interface
│   │   ├── fcm_provider.py         # Firebase Cloud Messaging
│   │   ├── whatsapp_provider.py    # Meta WhatsApp Cloud API
│   │   ├── smtp_provider.py        # Transactional Email (SES/SMTP)
│   │   └── chaos_provider.py       # Fault-injection chaos simulator
│   └── workers/
│       ├── outbox_relay.py         # Sweeps PostgreSQL outbox -> Redis queues
│       ├── critical_worker.py      # High-priority consumer loop (p99 < 500ms)
│       └── bulk_worker.py          # Batch consumer loop
├── scripts/
│   └── seed_100k_users.py          # Generates 100k realistic users for load testing
├── tests/
│   ├── test_idempotency.py         # Redis lock & replay tests
│   ├── test_segment_compiler.py    # AST -> SQL validation tests
│   ├── test_circuit_breaker.py     # State machine & channel mapping tests
│   ├── test_dlq_replay.py          # DLQ recipient detail restoration tests
│   ├── test_campaign_dispatch.py   # Server-side cursor memory safety tests
│   ├── test_bulk_worker.py         # Concurrent batch & zero-loss shutdown tests
│   ├── test_critical_worker.py     # Non-blocking p99 SLA protection tests
│   └── locustfile.py               # Stress test simulating 2,000 RPS spike
└── dashboard/                      # Next.js 16 (Turbopack) Live Mission Control UI
```

---

## 🗺️ Implementation Roadmap Status

- [x] **Phase 0:** Project Conception, Architecture Blueprints & Git Repo Setup
- [x] **Phase 1:** Core Data Layer, Async SQLAlchemy 2.0 Models & Dynamic Segment Compiler
- [x] **Phase 2:** Idempotency Middleware & Transactional Outbox Poller
- [x] **Phase 3:** Priority Workers, Redis Lua Token Bucket, Circuit Breaker & Channel Adapters
- [x] **Phase 4:** Next.js 16 Dashboard, Chaos Simulator, SSE Telemetry & Locust Load Testing

---

## 📄 License

MIT License. Designed and engineered by [Pratik Pattanayak](https://github.com/pattanayakpratik).
