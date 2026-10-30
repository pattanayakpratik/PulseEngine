# **PulseEngine: Distributed Multi-Channel Notification & Dynamic Segmentation Infrastructure**

**Document Version:** 1.0.0 \| **Architecture Tier:** Production Microservice (High-Throughput / Event-Driven)  
**Primary Stack:** Python 3.12 (FastAPI), PostgreSQL 16 (Async SQLAlchemy 2.0), Redis 7 (Streams \+ Lua), Celery / ARQ, OpenTelemetry, Next.js 16 (Admin Telemetry UI)

# **1\. Executive Summary & Problem Statement**

Modern applications—ranging from live gaming backends to SaaS platforms—require reliable, multi-channel communication (Push Notifications via FCM, WhatsApp, Email via SMTP/SES, and In-App WebSockets). However, naive notification implementations suffer from four fatal production flaws:

1. **Head-of-Line Blocking:** A 500,000-user marketing campaign clogs the worker queue, delaying critical transactional OTPs by 15+ minutes.  
2. **Dual-Write Data Loss:** Publishing directly to Redis/RabbitMQ inside an HTTP route loses notifications if the broker momentarily disconnects after a database commit.  
3. **Duplicate Spams:** Network retries from client services cause users to receive the same alert multiple times.  
4. **Hardcoded User Segments:** Engineering teams waste hours writing manual SQL queries whenever product teams want to target a new user cohort (e.g., "Users in India inactive for 7 days with a Pro tier").

**PulseEngine** solves these problems by combining an **ACID Transactional Outbox**, **Two-Lane Priority Queues**, **Redis Lua Token-Bucket Rate Limiting**, **Idempotency Middleware**, and a **JSON-to-SQL Dynamic Segment Resolver**.

# **2\. High-Level System Architecture**

                       \+-----------------------------------+

                       \|   Client Apps / Game Backends     \|

\|---\|

                                         \|

                         POST /v1/notify \| (Header: Idempotency-Key)

                                         v

\+---------------------------------------------------------------------------------+

\| FastAPI Gateway Layer                                                           \|

\|  1\. Redis Idempotency Check (24h TTL Lock)                                      \|

\|  2\. Payload Validation (Pydantic v2)                                            \|

\|  3\. Dynamic Segment Evaluation (JSON AST \-\> Parameterized SQL)                  \|

\+----------------------------------------+----------------------------------------+

                                         \|

                      Single ACID Commit \| (Notifications \+ Outbox\_Events)

                                         v

\+---------------------------------------------------------------------------------+

\| PostgreSQL 16 (Primary Data Store)                                              \|

\|  \- users (B-Tree \+ GIN JSONB Indexes)    \- notification\_logs (Partitioned)      \|

\|  \- segments (Dynamic JSON Rules)         \- outbox\_events (Unprocessed State)    \|

\+----------------------------------------+----------------------------------------+

                                         \|

                    Async Outbox Poller  \| (SELECT ... FOR UPDATE SKIP LOCKED)

                                         v

\+---------------------------------------------------------------------------------+

\| Redis 7 Broker & State Store                                                    \|

\|  \+----------------------------------+    \+-----------------------------------+  \|

\|  \| CRITICAL LANE (Priority Queue)   \|    \| BULK LANE (Campaign Queue)        \|  \|

\|  \| \- OTPs, Security, Payment Alerts \|    \| \- Marketing, Game Broadcasts      \|  \|

\|  \| \- SLA: p99 \< 500ms               \|    \| \- Batched via Server-Side Cursors \|  \|

\|  \+-----------------+----------------+    \+-----------------+-----------------+  \|

\|                    \|                                       \|                    \|

\|                    \+-------------------+-------------------+                    \|

\|                                        \|                                        \|

\|  \+-------------------------------------v-------------------------------------+  \|

\|  \| Token-Bucket Rate Limiter (Atomic Redis Lua Script per Provider)          \|  \|

\|  \| Circuit Breaker State (Closed / Open / Half-Open per Provider)            \|  \|

\|  \+-------------------------------------+-------------------------------------+  \|

\+----------------------------------------\|----------------------------------------+

                                         \|

                                         v

\+---------------------------------------------------------------------------------+

\| Async Delivery Worker Pool (Python httpx Connection Pool)                       \|

\|  ├──\> Firebase Cloud Messaging (FCM Push)                                       \|

\|  ├──\> WhatsApp Cloud API / Twilio                                               \|

\|  ├──\> SMTP / AWS SES (Transactional Email)                                      \|

\|  └──\> Dead-Letter Queue (DLQ) & Exponential Backoff \+ Jitter Retry Engine       \|

\+---------------------------------------------------------------------------------+

# **3\. Core Engineering Modules & Specifications**

## **Module A: Idempotency & Deduplication Layer**

* **Mechanism:** Every mutating API call (`POST /v1/notify` and `POST /v1/campaigns/trigger`) requires an `Idempotency-Key` HTTP header (UUIDv4).  
* **Execution Flow:**  
  1. Middleware executes `SET idemp:{key} "PROCESSING" NX EX 86400` in Redis.  
  2. If the key already exists with status `"PROCESSING"`, return `409 Conflict` (concurrent duplicate request in flight).  
  3. If the key exists with a cached JSON response payload, immediately return `200 OK` with the cached response without hitting PostgreSQL.  
  4. Once the DB transaction commits, update the Redis key with the serialized HTTP response.

## **Module B: Dynamic JSON-to-SQL Segment Resolver**

Instead of hardcoding SQL filters, product/admin users define reusable segments using a structured JSON AST:{

  "segment\_name": "mumbai\_inactive\_whales",

  "logic": "AND",

  "rules": \[

    {"field": "country", "op": "eq", "value": "IN"},

    {"field": "last\_active\_at", "op": "lt", "value": "NOW() \- INTERVAL '7 days'"},

    {"field": "metadata.city", "op": "eq", "value": "Mumbai"},

    {"field": "metadata.lifetime\_spend", "op": "gte", "value": 5000}

  \]

}

* **Compilation Engine:** A recursive parser validates operators (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `contains`) and translates them into parameterized SQLAlchemy 2.0 `select()` expressions.  
* **Memory-Safe Streaming:** When a campaign targets 200,000+ users, the resolver uses PostgreSQL **server-side cursors** (`yield_per(5000)`) to stream `user_id` chunks into the Bulk Queue without spiking FastAPI's RAM usage.

## **Module C: Transactional Outbox Relay**

* **Problem Solved:** Guarantees **At-Least-Once Delivery** even if Redis crashes right as a request arrives.  
* **Execution Flow:**  
  1. FastAPI inserts the notification record and an `outbox_events` row in the **same PostgreSQL transaction**.  
  2. A lightweight async background daemon runs every 100ms (or listens via PostgreSQL `LISTEN/NOTIFY`) and executes:SELECT id, payload, priority\_lane   
       
     FROM outbox\_events   
       
     WHERE status \= 'PENDING'   
       
     ORDER BY created\_at ASC   
       
     LIMIT 500   
       
     FOR UPDATE SKIP LOCKED;  
       
  3. The poller pushes the batch into Redis via a pipeline and marks the rows `PROCESSED`.

## **Module D: Priority Lane Isolation, Rate Limiting & Circuit Breakers**

* **Two-Lane Worker Topology:**  
  * `worker-critical`: Dedicated processes polling only `queue:critical` (OTPs, password resets, transaction alerts). Never blocked by campaigns.  
  * `worker-bulk`: Processes polling `queue:bulk` in batches.  
* **Atomic Token-Bucket Rate Limiter (Redis Lua):**  
  * External providers enforce strict quotas (e.g., WhatsApp allows 80 msgs/sec; SES allows 50 emails/sec).  
  * Before firing an `httpx` request, the worker executes a Redis Lua script checking available tokens for `ratelimit:provider:{name}`. If depleted, the task is rescheduled with an exact millisecond delay.  
* **Circuit Breaker Pattern:**  
  * Tracks rolling 60-second failure rates per provider in Redis. If SMTP or FCM returns `5xx` errors on greater than 50% of requests (minimum 20 requests), the circuit trips to `OPEN` for 30 seconds, immediately routing tasks to the retry queue instead of hanging worker sockets.  
* **Exponential Backoff with Full Jitter:**  
  * Failed deliveries retry using the formula: `T_sleep = random(0, min(T_max, T_base * 2^attempt))`  
  * After 5 failed attempts, the message moves to the **Dead-Letter Queue (DLQ)** for manual inspection and 1-click replay in the Next.js dashboard.

# **4\. Database Schema Design (PostgreSQL 16\)**

\-- 1\. Users Table with GIN Index for Fast Dynamic Segment Queries

CREATE TABLE users (

    id UUID PRIMARY KEY DEFAULT gen\_random\_uuid(),

    external\_user\_id VARCHAR(128) UNIQUE NOT NULL,

    email VARCHAR(255),

    phone\_e164 VARCHAR(20),

    fcm\_token TEXT,

    country CHAR(2) NOT NULL DEFAULT 'IN',

    last\_active\_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    created\_at TIMESTAMPTZ NOT NULL DEFAULT NOW()

);

CREATE INDEX idx\_users\_country\_active ON users (country, last\_active\_at);

CREATE INDEX idx\_users\_metadata\_gin ON users USING GIN (metadata jsonb\_path\_ops);

\-- 2\. Dynamic Segments Table

CREATE TABLE segments (

    id UUID PRIMARY KEY DEFAULT gen\_random\_uuid(),

    name VARCHAR(128) UNIQUE NOT NULL,

    description TEXT,

    filter\_ast JSONB NOT NULL, \-- Stores the JSON rule tree

    estimated\_count INTEGER DEFAULT 0,

    created\_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated\_at TIMESTAMPTZ NOT NULL DEFAULT NOW()

);

\-- 3\. Transactional Outbox Table

CREATE TABLE outbox\_events (

    id BIGSERIAL PRIMARY KEY,

    aggregate\_id UUID NOT NULL,

    priority\_lane VARCHAR(16) NOT NULL CHECK (priority\_lane IN ('CRITICAL', 'BULK')),

    payload JSONB NOT NULL,

    status VARCHAR(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED')),

    created\_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    processed\_at TIMESTAMPTZ

);

CREATE INDEX idx\_outbox\_pending ON outbox\_events (created\_at ASC) WHERE status \= 'PENDING';

\-- 4\. Notification Delivery Logs (Audit & Telemetry)

CREATE TABLE notification\_logs (

    id UUID PRIMARY KEY DEFAULT gen\_random\_uuid(),

    idempotency\_key VARCHAR(128) UNIQUE NOT NULL,

    user\_id UUID REFERENCES users(id) ON DELETE SET NULL,

    channel VARCHAR(16) NOT NULL CHECK (channel IN ('FCM', 'WHATSAPP', 'EMAIL', 'WEBHOOK')),

    priority VARCHAR(16) NOT NULL CHECK (priority IN ('CRITICAL', 'BULK')),

    status VARCHAR(20) NOT NULL DEFAULT 'QUEUED' 

        CHECK (status IN ('QUEUED', 'SENT', 'DELIVERED', 'RETRYING', 'DLQ\_FAILED')),

    attempt\_count SMALLINT NOT NULL DEFAULT 0,

    provider\_response JSONB,

    latency\_ms INTEGER,

    error\_message TEXT,

    created\_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated\_at TIMESTAMPTZ NOT NULL DEFAULT NOW()

);

CREATE INDEX idx\_notif\_logs\_status\_channel ON notification\_logs (status, channel, created\_at DESC);

# **5\. REST API Specification**

| Method | Endpoint | Description | Auth / Headers |
| :---- | :---- | :---- | :---- |
| `POST` | `/v1/notify` | Send a single transactional notification (Critical or Standard). | `X-API-Key, Idempotency-Key` |
| `POST` | `/v1/segments` | Create a new dynamic user segment using JSON AST rules. | `X-API-Key` |
| `POST` | `/v1/segments/preview` | Run `EXPLAIN` & count matching users \+ return sample 10 users in less than 50ms. | `X-API-Key` |
| `POST` | `/v1/campaigns/dispatch` | Trigger a bulk notification blast to a specific `segment_id`. | `X-API-Key, Idempotency-Key` |
| `POST` | `/v1/dlq/{log_id}/replay` | Manually replay a dead-lettered notification from the dashboard. | `X-API-Key` |
| `GET` | `/v1/metrics/summary` | Real-time RPS, queue depth, circuit breaker states, and `p99` latency. | Public / Read-Only |
| `GET` | `/v1/stream/telemetry` | Server-Sent Events (SSE) stream pushing live delivery logs to the UI. | Public / Read-Only |

## **Sample Request: `POST /v1/notify`**

{

  "external\_user\_id": "usr\_99481a",

  "channel": "WHATSAPP",

  "priority": "CRITICAL",

  "template\_id": "otp\_login\_v1",

  "variables": {

    "user\_name": "Pratik",

    "otp\_code": "482910",

    "expiry\_minutes": 5

  }

}

# **6\. Production Folder Structure**

t  
pulse-engine/  
├── docker-compose.yml              \# Postgres 16, Redis 7, FastAPI, Workers, Prometheus  
├── Dockerfile  
├── pyproject.toml                  \# Poetry / uv dependencies  
├── alembic/                        \# Async database migrations  
├── app/  
│   ├── main.py                     \# FastAPI app initialization & lifespan hooks  
│   ├── config.py                   \# Pydantic Settings (env vars)  
│   ├── database.py                 \# AsyncEngine & AsyncSession maker  
│   ├── redis\_client.py             \# Redis connection pool & Lua script loader  
│   ├── api/  
│   │   ├── deps.py                 \# Auth & Idempotency-Key dependencies  
│   │   └── v1/  
│   │       ├── notify.py           \# Transactional dispatch routes  
│   │       ├── segments.py         \# Segment CRUD & live query preview  
│   │       ├── campaigns.py        \# Bulk fan-out triggers  
│   │       └── metrics.py          \# Prometheus & SSE telemetry endpoints  
│   ├── core/  
│   │   ├── segment\_compiler.py     \# JSON AST \-\> SQLAlchemy query compiler  
│   │   ├── rate\_limiter.py         \# Token-Bucket Lua wrapper  
│   │   ├── circuit\_breaker.py      \# Provider health state machine  
│   │   └── telemetry.py            \# OpenTelemetry spans & latency histograms  
│   ├── models/                     \# SQLAlchemy 2.0 ORM models  
│   ├── schemas/                    \# Pydantic v2 request/response schemas  
│   ├── providers/                  \# Pluggable channel adapters (Strategy Pattern)  
│   │   ├── base.py                 \# Abstract BaseProvider interface  
│   │   ├── fcm\_provider.py  
│   │   ├── whatsapp\_provider.py  
│   │   └── smtp\_provider.py  
│   └── workers/  
│       ├── outbox\_relay.py         \# Sweeps PostgreSQL outbox \-\> Redis queues  
│       ├── critical\_worker.py      \# High-priority consumer loop  
│       └── bulk\_worker.py          \# Batch consumer loop  
├── scripts/  
│   └── seed\_100k\_users.py          \# Generates 100k realistic users for load testing  
├── tests/  
│   ├── test\_idempotency.py  
│   ├── test\_segment\_compiler.py  
│   └── locustfile.py               \# Stress test simulating 2,000 RPS spike  
└── dashboard/                      \# Next.js 16 (Turbopack) Live Control Plane

\#\# 7\. Next.js 16 Interactive Portfolio Demo (The "Recruiter Hook")

To make this project shine on pattanayakpratik.vercel.app, build a sleek single-page \*\*Mission Control Dashboard\*\* connected to your FastAPI backend with three interactive panels:

1\. \*\*Visual Segment Builder & SQL Inspector:\*\* Visitors click rules in the UI, and the dashboard displays the generated SQL query alongside the live PostgreSQL execution time (e.g., \`Matched 14,280 users in 11.4ms via GIN Index\`).

2\. \*\*Chaos & Load Simulator:\*\* Two buttons any visitor can click:

   \* \`"Trigger 10,000 Bulk Campaign \+ Send 1 Critical OTP"\` \-\> Visually proves in real time that the Critical OTP jumps the line and delivers in \*\*less than 200ms\*\* while the bulk queue drains steadily at the rate-limited cap.

   \* \`"Simulate SMTP Provider Outage"\` \-\> Trips the \*\*Circuit Breaker\*\* badge from Green (\`CLOSED\`) to Red (\`OPEN\`) and routes messages cleanly into the Dead-Letter Queue.

3\. \*\*Live Metrics Terminal:\*\* Directly mirrors the aesthetic of your portfolio hero code block, streaming real \`p99\_latency\_ms\`, \`active\_workers\`, and \`rps\` via Server-Sent Events (SSE).

\#\# 8\. 4-Phase Implementation Roadmap

\* \*\*Phase 1: Core Data Layer & Segment Compiler (Days 1–3)\*\*  

  Set up Docker Compose (Postgres 16 \+ Redis 7), write the SQLAlchemy 2.0 async models, run the \`seed\_100k\_users.py\` script, and build the JSON-to-SQL segment compiler with \`EXPLAIN ANALYZE\` verification.

\* \*\*Phase 2: Idempotency & Transactional Outbox (Days 4–6)\*\*  

  Build the \`POST /v1/notify\` endpoint, Redis idempotency middleware, and the async \`outbox\_relay.py\` background sweeper using \`FOR UPDATE SKIP LOCKED\`.

\* \*\*Phase 3: Priority Workers, Lua Rate Limiter & Circuit Breaker (Days 7–10)\*\*  

  Implement the two-lane worker pools, the atomic Redis Lua token bucket script, provider adapters (with a built-in Mock Chaos Provider for safe load testing), and exponential backoff DLQ routing.

\* \*\*Phase 4: Next.js 16 Dashboard & Locust Load Testing (Days 11–14)\*\*  

  Build the SSE telemetry endpoint, connect the Next.js 16 control dashboard, run Locust benchmarks, and publish the architecture case study to your \`/notes\` section on your portfolio.  
