# HookRelay v2: Enterprise Webhook Infrastructure & Alert Gateway

[![CI & Security](https://github.com/Abhishek-Gali/HookRelay/actions/workflows/ci.yml/badge.svg)](https://github.com/Abhishek-Gali/HookRelay/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)](https://fastapi.tiangolo.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **HookRelay** is an enterprise-grade, observable webhook delivery gateway and event processor. Engineered with cybersecurity and distributed systems patterns: raw-byte HMAC-SHA256 verification, API key & RBAC authentication, atomic database idempotency, durable job queueing with per-attempt tracking, a dead letter queue (DLQ) with manual replay, multi-destination routing (Discord, Slack, HTTP), rate limiting, and a live web management console.

---

## 1. System Architecture

```
GitHub Webhook (or any Provider)
              │ (Raw bytes + HMAC-SHA256)
              ▼
   ┌────────────────────────────────────────┐
   │ API Gateway (FastAPI)                  │
   │  • Sliding-Window Rate Limiter         │
   │  • Constant-time HMAC Verifier         │
   │  • Fast Acknowledgment (< 3ms)         │
   └───────────────────┬────────────────────┘
                       │
                       ▼
   ┌────────────────────────────────────────┐
   │ Event Processor & Routing Engine       │
   │  • Atomic Idempotency Check (DB)       │
   │  • Event & Branch Filtering            │
   │  • Multi-Destination Fan-out           │
   └───────────────────┬────────────────────┘
                       │
                       ▼
   ┌────────────────────────────────────────┐
   │ Durable Queue Broker                   │
   │  • MemoryQueueBroker (Local/CI)        │
   │  • RedisQueueBroker (Cloud Clusters)   │
   └───────────────────┬────────────────────┘
                       │
                       ▼
   ┌────────────────────────────────────────┐
   │ Resilient Dispatcher Workers           │
   │  • Discord (Embed cards)               │
   │  • Slack (Block Kit)                   │
   │  • Generic HTTP Webhooks               │
   │  • Exponential Backoff & Jitter        │
   └───────┬────────────────────────┬───────┘
           │                        │
           ▼ (Granular logs)        ▼ (Exhausted retries)
   ┌─────────────────┐      ┌─────────────────┐
   │ delivery_attempts│      │ Dead Letter (DLQ│
   │ (per-call trace)│      │ (Safe Replay)   │
   └─────────────────┘      └─────────────────┘
```

---

## 2. Key Enterprise Features

### 🛡️ Cybersecurity & Hardening
- **Timing-Safe HMAC-SHA256 Verification**: Computed strictly over **raw incoming wire bytes** (never re-serialized JSON) and compared with `hmac.compare_digest` to defeat timing side-channel attacks.
- **API Key & RBAC Protection (`app/auth.py`)**: All management endpoints (`/api/*`) are secured by constant-time SHA-256 hashed API keys with Role-Based Access Control (`ADMIN`, `OPERATOR`, `VIEWER`).
- **Immutable Audit Trail (`audit_logs`)**: Administrative actions (redriving, replaying, discarding DLQ events) are permanently logged with timestamps, roles, and IP addresses.
- **Gateway Rate Limiting (`app/ratelimit.py`)**: Sliding-window rate limiter (default: 300 req/min/IP) protects the webhook endpoint from volumetric denial-of-service floods.
- **Defensive Mention Sanitization**: Prevents `@everyone` and `@here` mention spam by injecting zero-width spaces (`@\u200beveryone`).
- **STRIDE Threat Model**: Detailed formal threat model available in [docs/threat-model.md](docs/threat-model.md).

### ⚙️ Reliability & Fault Tolerance
- **Atomic Database Idempotency**: `INSERT INTO deliveries ... ON CONFLICT (delivery_id) DO NOTHING RETURNING delivery_id` eliminates Time-of-Check-to-Time-of-Use (TOCTOU) race conditions across concurrent workers.
- **Durable Queue & Worker Loop (`app/queue_broker.py`)**: Decouples webhook ingestion from downstream dispatch.
- **Granular Attempt Tracing (`delivery_attempts`)**: Logs each delivery attempt with HTTP status, response time in ms, error descriptions, and headers (`Retry-After: 2s`).
- **Dead Letter Queue (DLQ)**: Deliveries that exhaust retry attempts transition to `dead_letter`, where operators can inspect, discard, or safely replay them via API or UI.
- **Crash Recovery Reconciliation Engine**: Periodic background sweep worker rescues deliveries stranded in `received` status if a container crashes mid-flight.

### 🌐 Multi-Provider Routing & Formatting
- **Provider Abstraction**: First-class support for:
  - **Discord**: Color-coded embed cards (green for opened/commits, purple for merged PRs, red for unmerged closed PRs).
  - **Slack**: Block Kit formatting with repository badges and author handles.
  - **Generic HTTP**: Forward events to internal microservices with custom webhook headers.
- **Filtering Rules Engine**: Declaratively filter events by event type, branch patterns (`refs/heads/main`), or repository wildcards.

### 🖥️ Live Operations Dashboard
HookRelay includes a built-in dark-mode operations console served directly at `/dashboard`:
- Live counters (Total Ingested, Dispatched, DLQ, Success Rate).
- Ingestion feed with status badges (`SENT`, `RECEIVED`, `DEAD_LETTER`).
- Modal inspecting granular attempt histories (`Attempt 1 -> 429`, `Attempt 2 -> 204`).
- One-click replay button for DLQ events.

---

## 3. Threat Model (STRIDE)

| Threat Category | Potential Impact | HookRelay Defense Control |
|---|---|---|
| **Spoofing** | Forged webhook requests | Raw-byte HMAC-SHA256 signature check (`X-Hub-Signature-256`) |
| **Spoofing** | Unauthorized admin calls | Constant-time hashed `X-API-Key` headers (`app/auth.py`) |
| **Tampering** | Man-in-the-middle modification | Cryptographic digest over wire bytes |
| **Tampering** | Mention injection (`@everyone`) | `sanitize_mentions()` defanging with zero-width spaces |
| **Repudiation**| Denying administrative actions | Persistent immutable `audit_logs` table |
| **Information Disclosure** | Timing side-channels | `hmac.compare_digest` on signatures and API keys |
| **Denial of Service** | Volumetric HTTP floods | Sliding-window IP rate limiter (`app/ratelimit.py`) |
| **Denial of Service** | Large payload memory exhaustion | Strict 5MB pre-parsing byte cutoff |
| **Elevation of Privilege** | Viewers modifying DLQ | Strict RBAC dependency checks (`require_role`) |

---

## 4. Benchmark & Performance Results

Simulated with 100 concurrent signed webhook deliveries (including 10 duplicate replays):

```
============================================================
  HOOKRELAY BENCHMARK RESULTS
============================================================
Total Requests Dispatched  : 100
Accepted Deliveries (New)  : 90 (Target: 90)
Duplicates Suppressed      : 10 (Target: 10)
Unexpected Rejections      : 0 (Target: 0)
Duplicate Suppression Rate : 100.0%
Average Gateway Latency    : 2.11 ms
P95 Gateway Latency        : 2.53 ms
Automated Pytest Suite     : 34 / 34 PASSED (100%)
============================================================
```

---

## 5. Quick Start & Setup

### Prerequisites
- Python 3.11+
- (Optional) Docker & Docker Compose

### 1. Clone & Install
```bash
git clone https://github.com/Abhishek-Gali/HookRelay.git
cd HookRelay
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
cp .env.example .env
# Edit .env with your secrets:
# GITHUB_WEBHOOK_SECRET=your_github_secret
# DISCORD_WEBHOOK_URL=your_discord_webhook_url
# ADMIN_API_KEY=hr_admin_secret_key_12345
```

### 3. Run the Service
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 4. Access Live Interfaces
- **Live Operations Dashboard**: [http://localhost:8000/dashboard](http://localhost:8000/dashboard)
- **Interactive OpenAPI Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Prometheus Metrics**: [http://localhost:8000/metrics](http://localhost:8000/metrics)
- **Healthcheck**: [http://localhost:8000/healthz](http://localhost:8000/healthz)

### 5. Run Automated Tests
```bash
python -m pytest tests/ -v
```

---

## 6. API Reference

All management endpoints require `X-API-Key` authentication.

| Method | Endpoint | Required Role | Description |
|---|---|---|---|
| `POST` | `/webhook/github` | None (HMAC) | Ingests GitHub webhook deliveries |
| `GET` | `/dashboard` | None (Browser) | Web operations console |
| `GET` | `/api/deliveries` | `VIEWER`+ | List deliveries with status filters |
| `GET` | `/api/deliveries/{id}`| `VIEWER`+ | Get delivery details & attempt history |
| `POST` | `/api/deliveries/{id}/redrive` | `OPERATOR`+ | Re-queue delivery for dispatch |
| `GET` | `/api/dlq` | `VIEWER`+ | List dead-letter deliveries |
| `POST` | `/api/dlq/{id}/discard` | `ADMIN` | Discard an unfixable event |
| `GET` | `/api/stats` | `VIEWER`+ | Aggregated counts for dashboards |
| `GET` | `/api/audit-logs` | `ADMIN` | Query security audit trail |
| `GET` | `/healthz` | None | Service & queue health probe |
| `GET` | `/metrics` | None | Prometheus telemetry metrics |

---

## 7. Production Docker Deployment

```bash
docker compose up -d
```
Runs HookRelay alongside PostgreSQL with persistent data volumes and health checks.

---

## 8. License

MIT License. Designed and maintained by [Abhishek Gali](https://github.com/Abhishek-Gali).
