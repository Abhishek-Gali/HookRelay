# HookRelay: Hardened Webhook Delivery Infrastructure & Event Gateway

[![CI & DevSecOps](https://github.com/Abhishek-Gali/HookRelay/actions/workflows/ci.yml/badge.svg)](https://github.com/Abhishek-Gali/HookRelay/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.135+-009688.svg)](https://fastapi.tiangolo.com)
[![Tests: 64 Passed](https://img.shields.io/badge/tests-64%20passed-success.svg)](https://github.com/Abhishek-Gali/HookRelay)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **HookRelay** is a security-hardened, production-oriented webhook ingestion and multi-provider delivery gateway (`GitHub ➔ HookRelay ➔ Discord / Slack / Generic HTTP`). Engineered with raw-byte HMAC-SHA256 verification, bounded signed-payload replay detection, SQL-backed durable job queueing with monotonic **fencing tokens** (`worker_id` + `lease_generation` + `locked_until` + `FOR UPDATE SKIP LOCKED`), per-destination partial-failure tracking, unified retry policies respecting `Retry-After`, DNS-validated SSRF-safe routing, automated data retention scrubbing, RBAC-protected management APIs, and an XSS/CSRF-hardened operations console.

---

## 1. Architecture & Durable Lease Lifecycle

```
GitHub Webhook
      │ (Streaming 5MB Cutoff + Raw-Byte HMAC-SHA256 + Payload Fingerprint Check)
      ▼
┌──────────────────────────────────────────────────────────┐
│ FastAPI Ingestion Gateway (/webhook/github)              │
│  • Redis Lua / Bounded LRU Rate Limiter (300 req/min/IP) │
│  • Constant-Time HMAC Verification (hmac.compare_digest) │
│  • Routing Engine (Branch/Repo Filters + DNS SSRF Check) │
└─────────────────────────────┬────────────────────────────┘
                              │ Atomic INSERT ... ON CONFLICT DO NOTHING
                              ▼
┌──────────────────────────────────────────────────────────┐
│ PostgreSQL / SQLite Durable Store                        │
│  • deliveries (status, payload_hash, per-dest JSON)      │
│  • Fencing Lease: worker_id, lease_generation, locked_til│
│  • delivery_attempts (status, trigger_type, latency, err)│
│  • audit_logs + Retention Policy (14d/30d/90d scrubbing) │
└──────────────┬─────────────────────────────┬─────────────┘
               │ Atomic Lease + Fencing Gen  │ Expired Lease + Retention Sweep
               │ (SKIP LOCKED / RETURNING)   │ (Multi-batch atomic lease)
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
- **Zero Production Default Credentials & Private-Target Guard (`app/config.py`)**: Startup validation rejects blank, weak (`<32` char), or known placeholder keys (`hr_admin_secret_key_12345`, etc.) and forbids `ALLOW_PRIVATE_DESTINATIONS=true` when `ENVIRONMENT=production`.
- **Timing-Safe Raw-Byte HMAC-SHA256 & Signed-Payload Replay Protection (`app/security.py`, `app/store.py`)**:
  - Computes HMAC-SHA256 strictly over raw request stream bytes before JSON parsing and compares via `hmac.compare_digest`.
  - Mitigates `X-GitHub-Delivery` header mutation replays (`HR-04`) by indexing `payload_hash = sha256(raw_body)` + `event_type` within a configurable `REPLAY_WINDOW_SECONDS` (default 300s).
- **Streaming Payload DoS Protection (`read_bounded_body_stream` in `app/main.py`)**: Enforces the 5 MB limit chunk-by-chunk on the incoming stream before buffering into memory, returning HTTP 413 immediately on oversized chunked transfers.
- **Dual Authentication (API Key + `HttpOnly` Session Cookie with CSRF), RBAC & Distributed Rate Limiting (`app/auth.py`, `app/ratelimit.py`)**:
  - Programmatic API clients authenticate via `X-API-Key` verified against SHA-256 hashes in constant time.
  - Browser console operators exchange their key at `POST /api/auth/login` for an `HttpOnly; SameSite=Strict` session cookie (`hr_session`) and must supply `X-CSRF-Token` on all state-changing requests (including `POST /api/auth/logout`).
  - Role hierarchy: `ADMIN` (full control + DLQ discard + audit logs), `OPERATOR` (read + redrive), `VIEWER` (read-only).
  - Supports multi-replica distributed sliding-window rate limiting via atomic Redis Lua scripts (`REDIS_URL`), with bounded-memory `OrderedDict` LRU fallback (`max_buckets=10,000`).
- **Connect-Time DNS-Rebinding & Redirect SSRF Protection (`app/routing.py`, `app/providers.py`)**: Blocks non-HTTPS schemes, `localhost`, loopback (`127.0.0.0/8`, `::1`), RFC1918 private networks (`10/8`, `172.16/12`, `192.168/16`), cloud metadata endpoints (`169.254.169.254`), resolves and pins DNS `A`/`AAAA` records (including IPv4-mapped and NAT64 prefixes) with TLS `sni_hostname` before outbound dispatch, and enforces `follow_redirects=False`.
- **Stored-XSS & Strict CSP without `'unsafe-inline'` (`app/ui/dashboard.html`, `app/ui/dashboard.js`, `app/main.py`)**:
  - Operations console renders all untrusted webhook and downstream error data exclusively via `document.createElement()` and `.textContent` (zero `innerHTML` interpolation).
  - Serves `/static/dashboard.js` and `/static/dashboard.css` from in-memory cached `Response` objects (eliminating `FileResponse` Range-header exposure, `CVE-2025-62727`) with `Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self'; frame-ancestors 'none'; object-src 'none'; base-uri 'self'` and `X-XSS-Protection: 0`.
- **Credential-Safe Error Categorization (`format_safe_exception`, `sanitize_error_message` in `app/providers.py`)**: Redacts Discord/Slack tokens, URL userinfo, and query strings from downstream errors, omits raw error bodies on `GenericHttpProvider`, and categorizes exceptions into structured JSON without leaking internal stack traces.

### ⚙️ Distributed Systems & Queue Reliability
- **SQL-Backed Durable Job Queue (`DatabaseQueueBroker` in `app/queue_broker.py`)**:
  - Jobs are persisted in the SQL `deliveries` table at ingestion time—surviving process crashes and container restarts without data loss.
- **Monotonic Fencing Tokens + Ownership-Conditional State Transitions (`acquire_lease`, `_transition_state` in `app/store.py`)**:
  - Every lease acquisition increments a monotonic `lease_generation` counter (`HR-02`, `HR-03`).
  - Workers verify `(worker_id, lease_generation)` before each outbound HTTP attempt and in the `WHERE` clause of `mark_sent`, `mark_failed_or_dlq`, and `mark_retry_wait`. A slow worker whose lease expired and was reclaimed by another worker immediately raises `StaleWorkerLeaseError` and cannot overwrite state.
- **Per-Destination Partial-Failure Tracking (`update_destination_status` in `app/store.py`)**:
  - Multi-destination deliveries track per-target completion (`"status": "pending" | "sent" | "failed"`) inside `deliveries.destinations` (`HR-06`).
  - Redrives and crash reconciliations (`only_unsent=True`) automatically skip destinations that already succeeded (`status == "sent"`), preventing duplicate alerts on partial failures.
- **Automated Data Retention & Payload Scrubbing (`enforce_retention_policy` in `app/store.py`)**:
  - Background reconciliation automatically scrubs raw webhook `payload = NULL` on terminal deliveries older than `PAYLOAD_RETENTION_DAYS` (14d) while preserving the `delivery_id` row for idempotency (`HR-09`), and prunes `delivery_attempts` (30d) and `audit_logs` (90d).
- **Unified Retry Policy (`RetryPolicy` in `app/sender.py` & `app/dispatcher.py`)**:
  - Honors downstream HTTP 429 `Retry-After` headers up to `MAX_RETRY_AFTER_SECONDS` (default 60s) and uses exponential backoff with jitter for 5xx/network errors.

---

## 3. Threat Model Summary (STRIDE)

Full analysis in [docs/threat-model.md](docs/threat-model.md).

| STRIDE Category | Threat Vector | Technical Control | Verified By |
|---|---|---|---|
| **Spoofing** | Forged GitHub webhook | Raw-byte HMAC-SHA256 + `compare_digest` | `tests/test_security.py` |
| **Spoofing** | Mutated `X-GitHub-Delivery` replay | Bounded `payload_hash` replay window (`HR-04`) | `tests/test_webhook_e2e.py` |
| **Spoofing** | Default credentials in prod | Startup validator rejects defaults / keys `<32` chars | `tests/test_auth.py` |
| **Tampering** | Stale worker overwriting state | Monotonic `lease_generation` fencing tokens (`HR-02/03`) | `tests/test_queue_and_leases.py` |
| **Tampering** | Stored XSS in dashboard | Pure DOM `.textContent` rendering + strict CSP | `tests/test_webhook_e2e.py` |
| **Tampering** | Mention injection (`@everyone`) | Universal `sanitize_mentions()` + HTTPS URL validation | `tests/test_formatter.py` |
| **Repudiation** | Unaudited auth failures / replays | Persistent `audit_logs` (never logs raw keys) | `tests/test_webhook_e2e.py` |
| **Info Disclosure** | SSRF to internal/metadata IPs | `resolve_and_pin_destination()` + `ALLOW_PRIVATE` prod block | `tests/test_routing.py`, `tests/test_auth.py` |
| **Denial of Service** | Chunked multi-GB payload | Streaming byte cutoff (`read_bounded_body_stream`) | `tests/test_webhook_e2e.py` |
| **Denial of Service** | API brute-force / redrive flood | Redis Lua / bounded LRU sliding-window rate limiters | `tests/test_webhook_e2e.py` |
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

### 4. Run the 64-Test Verification Suite
```bash
python -m pytest tests/ -v
```

---

## 5. API & Probe Reference

| Method | Endpoint | Auth / Role | Description |
|---|---|---|---|
| `POST` | `/webhook/github` | GitHub HMAC | Streaming size check, HMAC verify, replay check, atomic claim, durable enqueue |
| `GET` | `/dashboard` | Browser (Session Cookie) | XSS/CSRF-hardened operations & DLQ replay console |
| `POST` | `/api/auth/login` | API Key in body | Exchanges API key for `HttpOnly; SameSite=Strict` session cookie + CSRF token |
| `POST` | `/api/auth/logout` | `VIEWER`+ & CSRF | Revokes active browser session cookie |
| `GET` | `/api/deliveries` | `VIEWER`+ | List deliveries with attempt histories |
| `GET` | `/api/deliveries/{id}` | `VIEWER`+ | Inspect single delivery and per-attempt trace |
| `POST` | `/api/deliveries/{id}/redrive` | `OPERATOR`+ | Replay failed/DLQ delivery (retries only unsent destinations by default) |
| `GET` | `/api/dlq` | `VIEWER`+ | List deliveries in `dead_letter` status |
| `POST` | `/api/dlq/{id}/discard` | `ADMIN` | Transition unfixable DLQ item to `discarded` |
| `GET` | `/api/stats` | `VIEWER`+ | Aggregated delivery, queue depth, & DLQ statistics |
| `GET` | `/api/audit-logs` | `ADMIN` | Security audit trail (`auth_failed`, `authz_denied`, `redrive`, `discard`) |
| `GET` | `/health/live` | Public | Minimal liveness probe (`{"status": "alive"}`) |
| `GET` | `/health/ready` (`/healthz`) | Public | Minimal readiness probe (`{"status": "healthy", "database": "connected"}`) |
| `GET` | `/metrics` | `VIEWER`+ (`REQUIRE_METRICS_AUTH=true`) | Prometheus telemetry metrics |

---

## 6. License

MIT License. Designed and maintained by [Abhishek Gali](https://github.com/Abhishek-Gali).

