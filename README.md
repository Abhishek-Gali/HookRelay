# HookRelay: Hardened Webhook Delivery Infrastructure & Event Gateway

[![CI & DevSecOps](https://github.com/Abhishek-Gali/HookRelay/actions/workflows/ci.yml/badge.svg)](https://github.com/Abhishek-Gali/HookRelay/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)](https://fastapi.tiangolo.com)
[![Tests: 55 Passed](https://img.shields.io/badge/tests-55%20passed-success.svg)](https://github.com/Abhishek-Gali/HookRelay)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **HookRelay** is a security-hardened, production-oriented webhook ingestion and multi-provider delivery gateway (`GitHub ➔ HookRelay ➔ Discord / Slack / Generic HTTP`). Engineered with raw-byte HMAC-SHA256 verification, SQL-backed durable job queueing with atomic worker leases (`worker_id` + `locked_until`), unified retry policies respecting `Retry-After`, SSRF-validated multi-destination routing, a Dead Letter Queue (DLQ) with destination-preserving replay, RBAC-protected management APIs, and an XSS-hardened operations console.

---

## 1. Architecture & Durable Lease Lifecycle

```
GitHub Webhook
      │ (Streaming 5MB Cutoff + Raw-Byte HMAC-SHA256)
      ▼
┌──────────────────────────────────────────────────────────┐
│ FastAPI Ingestion Gateway (/webhook/github)              │
│  • Sliding-Window Rate Limiter (300 req/min/IP)          │
│  • Constant-Time HMAC Verification (hmac.compare_digest) │
│  • Routing Engine (Branch/Repo Filters + SSRF Checks)    │
└─────────────────────────────┬────────────────────────────┘
                              │ Atomic INSERT ... ON CONFLICT DO NOTHING
                              ▼
┌──────────────────────────────────────────────────────────┐
│ PostgreSQL / SQLite Durable Store                        │
│  • deliveries (status, payload, destinations JSON)       │
│  • Lease Columns: worker_id, locked_until, next_run_at   │
│  • delivery_attempts (per-call HTTP status, latency, err)│
│  • audit_logs (auth failures, RBAC denials, replays)     │
└──────────────┬─────────────────────────────┬─────────────┘
               │ Atomic Lease Acquisition    │ Expired Lease / Stale Sweep
               │ (UPDATE ... RETURNING)      │ (Multi-batch atomic lease)
               ▼                             ▼
┌───────────────────────────┐   ┌──────────────────────────┐
│ Durable Queue Worker      │   │ Reconciliation Worker    │
└──────────────┬────────────┘   └────────────┬─────────────┘
               └──────────────┬──────────────┘
                              │ Unified RetryPolicy (Backoff + Jitter + Retry-After)
               ┌──────────────┼──────────────┐
               ▼              ▼              ▼
           Discord          Slack       Generic HTTP
        (Rich Embeds)    (Block Kit)  (X-HookRelay-Delivery-ID)
```

---

## 2. Core Security & Reliability Guarantees

### 🛡️ Security Hardening
- **Zero Production Default Credentials (`app/config.py`)**: Startup validation rejects blank, weak (`<32` char), or known placeholder keys (`hr_admin_secret_key_12345`, etc.) when `ENVIRONMENT=production`.
- **Timing-Safe Raw-Byte HMAC-SHA256 (`app/security.py`)**: Computed strictly over raw request stream bytes before JSON parsing and compared via `hmac.compare_digest`.
- **Streaming Payload DoS Protection (`read_bounded_body_stream` in `app/main.py`)**: Enforces the 5 MB limit chunk-by-chunk on the incoming stream before buffering into memory, returning HTTP 413 immediately on oversized chunked transfers.
- **API Key Authentication, RBAC & Brute-Force Lockout (`app/auth.py`, `app/ratelimit.py`)**:
  - Management endpoints (`/api/*`) require `X-API-Key` headers verified against SHA-256 hashes in constant time.
  - Role hierarchy: `ADMIN` (full control + DLQ discard + audit logs), `OPERATOR` (read + redrive), `VIEWER` (read-only).
  - Separate rate limits for `/webhook/github` (300/min), `/api/*` (60/min), `/api/*/redrive` (10/min), and failed auth lockout (10 failures/min/IP).
- **SSRF Protection on Destination URLs (`app/routing.py`)**: Blocks non-HTTPS schemes, `localhost`, loopback (`127.0.0.0/8`, `::1`), RFC1918 private networks (`10/8`, `172.16/12`, `192.168/16`), and cloud metadata endpoints (`169.254.169.254`, `metadata.google.internal`).
- **Stored-XSS & CSP Hardening (`app/ui/dashboard.html`, `app/main.py`)**:
  - Operations console renders all untrusted webhook and downstream error data exclusively via `document.createElement()` and `.textContent` (zero `innerHTML` interpolation).
  - Zero external CDN dependencies; protected by `Content-Security-Policy: default-src 'self'; frame-ancestors 'none'; object-src 'none'`.

### ⚙️ Distributed Systems & Queue Reliability
- **SQL-Backed Durable Job Queue (`DatabaseQueueBroker` in `app/queue_broker.py`)**:
  - Jobs are persisted in the SQL `deliveries` table at ingestion time—surviving process crashes and container restarts without data loss.
- **Atomic Worker Leases (`acquire_lease` in `app/store.py`)**:
  - Workers and reconciliation sweeps acquire exclusive time-bounded leases (`status = 'processing'`, `worker_id`, `locked_until`) via atomic `UPDATE ... WHERE ... RETURNING delivery_id`.
  - Guarantees two concurrent workers or reconciliation replicas can **never** double-claim the same job.
- **Destination-Preserving Reconciliation & Redrive**:
  - Resolved routing destinations (`[{"provider": "discord", "url": "..."}, ...]`) are persisted as structured JSON at ingestion time.
  - Both crash reconciliation and `POST /api/deliveries/{id}/redrive` restore the exact original destinations unless `?reroute=true` is explicitly requested.
- **Formal Delivery State Machine (`VALID_STATE_TRANSITIONS` in `app/models.py`)**:
  - Enforces legal state transitions (`received` ➔ `processing` ➔ `sent` | `retry_wait` | `dead_letter` ➔ `discarded`) and rejects illegal transitions (e.g., `sent ➔ dead_letter`).
- **Unified Retry Policy (`RetryPolicy` in `app/sender.py` & `app/dispatcher.py`)**:
  - Single retry engine across the codebase. Honors downstream HTTP 429 `Retry-After` headers up to `MAX_RETRY_AFTER_SECONDS` (default 60s) and uses exponential backoff with jitter for 5xx/network errors.
  - Bounds downstream error body reads to 2,048 characters (`read_bounded_error`) to prevent log/memory flooding.
- **Delivery Semantics (At-Least-Once)**:
  - Inbound GitHub webhooks are deduplicated atomically by `delivery_id`.
  - Outbound HTTP dispatches include `X-HookRelay-Delivery-ID: <delivery_id>` so custom consumers can deduplicate idempotently. Because Discord/Slack incoming webhooks do not support client-supplied idempotency keys, downstream chat delivery follows **at-least-once** semantics if a network timeout occurs after downstream acceptance.

---

## 3. Threat Model Summary (STRIDE)

Full analysis in [docs/threat-model.md](docs/threat-model.md).

| STRIDE Category | Threat Vector | Technical Control | Verified By |
|---|---|---|---|
| **Spoofing** | Forged GitHub webhook | Raw-byte HMAC-SHA256 + `compare_digest` | `tests/test_security.py` |
| **Spoofing** | Default credentials in prod | Startup validator rejects defaults / keys `<32` chars | `tests/test_auth.py` |
| **Tampering** | Stored XSS in dashboard | Pure DOM `.textContent` rendering + strict CSP | `tests/test_webhook_e2e.py` |
| **Tampering** | Mention injection (`@everyone`) | Universal `sanitize_mentions()` + HTTPS URL validation | `tests/test_formatter.py` |
| **Repudiation** | Unaudited auth failures / replays | Persistent `audit_logs` (never logs raw keys) | `tests/test_webhook_e2e.py` |
| **Info Disclosure** | SSRF to internal/metadata IPs | `validate_ssrf_safe_url()` blocks private/loopback/169.254 | `tests/test_routing.py` |
| **Denial of Service** | Chunked multi-GB payload | Streaming byte cutoff (`read_bounded_body_stream`) | `tests/test_webhook_e2e.py` |
| **Denial of Service** | API brute-force / redrive flood | Tiered sliding-window rate limiters (429) | `tests/test_webhook_e2e.py` |
| **Elevation of Privilege** | Viewer triggering redrive/discard | Strict RBAC `require_role()` dependency (403) | `tests/test_webhook_e2e.py` |

---

## 4. Quick Start & Local Development

### 1. Clone & Install
```bash
git clone https://github.com/Abhishek-Gali/HookRelay.git
cd HookRelay
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
cp .env.example .env
# Generate strong 32-byte secrets for production:
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### 3. Run the Server
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 4. Run the 55-Test Verification Suite
```bash
python -m pytest tests/ -v
```

---

## 5. API & Probe Reference

| Method | Endpoint | Auth / Role | Description |
|---|---|---|---|
| `POST` | `/webhook/github` | GitHub HMAC | Streaming size check, HMAC verify, atomic claim, durable enqueue |
| `GET` | `/dashboard` | Browser (API Key in UI) | XSS-hardened operations & DLQ replay console |
| `GET` | `/api/deliveries` | `VIEWER`+ | List deliveries with attempt histories |
| `GET` | `/api/deliveries/{id}` | `VIEWER`+ | Inspect single delivery and per-attempt trace |
| `POST` | `/api/deliveries/{id}/redrive` | `OPERATOR`+ | Replay failed/DLQ delivery (preserves original destinations by default) |
| `GET` | `/api/dlq` | `VIEWER`+ | List deliveries in `dead_letter` status |
| `POST` | `/api/dlq/{id}/discard` | `ADMIN` | Transition unfixable DLQ item to `discarded` |
| `GET` | `/api/stats` | `VIEWER`+ | Aggregated delivery & DLQ statistics |
| `GET` | `/api/audit-logs` | `ADMIN` | Security audit trail (`auth_failed`, `authz_denied`, `redrive`, `discard`) |
| `GET` | `/health/live` | Public | Liveness probe (process event loop alive) |
| `GET` | `/health/ready` (`/healthz`) | Public | Readiness probe (verifies SQL `SELECT 1` connectivity + queue depth) |
| `GET` | `/metrics` | Configurable (`REQUIRE_METRICS_AUTH`) | Prometheus telemetry metrics |

---

## 6. License

MIT License. Designed and maintained by [Abhishek Gali](https://github.com/Abhishek-Gali).
