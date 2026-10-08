# HookRelay Comprehensive Threat Model (STRIDE)

This document formalizes the threat model for **HookRelay v2.1**, evaluating threats against the **STRIDE** methodology (Spoofing, Tampering, Repudiation, Information Disclosure, Denial of Service, Elevation of Privilege) and mapping each threat to concrete technical controls and automated tests.

---

## 1. System Architecture & Trust Boundaries

```
[ External Untrusted: GitHub / Internet Attackers ]
                  │
          (HTTPS Boundary)
                  ▼
[ Boundary 1: Streaming 5MB Cutoff + Tiered Rate Limiter ]
                  │
[ Boundary 2: Raw-Byte HMAC-SHA256 Verification (Constant-Time) ]
                  │
[ Boundary 3: SQL Atomic Deduplication + Structured Destination Persistence ]
                  │
[ Boundary 4: SQL-Backed Durable Queue + Atomic Worker Leases (worker_id + locked_until) ]
                  │
[ Boundary 5: SSRF-Validated Resilient Dispatcher (Discord / Slack / HTTP) ]
                  │
          (Authenticated API Boundary: Hashed API Key + RBAC + Brute-Force Lockout)
                  ▼
[ Boundary 6: XSS-Hardened Operations & DLQ Console (Strict CSP + DOM textContent) ]
```

---

## 2. STRIDE Threat Analysis & Defense Controls

### S — Spoofing (Impersonating something or someone else)
- **Threat S1: Forged webhook from malicious third party**
  - *Risk*: An attacker sends fabricated git push or deployment events to trigger false alarms or actions.
  - *Mitigation*: **HMAC-SHA256 signature verification (`app/security.py`)**. GitHub signs every request with a shared secret. HookRelay verifies the digest over **raw bytes** using `hmac.compare_digest`. Requests with missing, mismatched, or unauthenticated signatures are dropped with HTTP 401 before touching the database.
- **Threat S2: Unauthorized operator or default credential abuse on `/api/*`**
  - *Risk*: An adversary queries `/api/deliveries` or triggers redrives using default credentials or brute force.
  - *Mitigation*:
    1. **Zero Production Defaults (`app/config.py`)**: Startup validation rejects blank, short (`<32` char), or known placeholder keys in `production` mode.
    2. **Hashed API Key Authentication (`app/auth.py`)**: All `/api/*` endpoints require `X-API-Key` headers checked in constant time against SHA-256 digests.
    3. **Brute-Force Lockout**: IP addresses exceeding 10 failed authentication attempts per minute are locked out with HTTP 429 and logged in `audit_logs`.

### T — Tampering (Modifying data on wire, at rest, or in the UI)
- **Threat T1: Man-in-the-middle modifying webhook payload**
  - *Risk*: Modifying commit messages or issue IDs in transit.
  - *Mitigation*: Cryptographic HMAC covers the raw payload bytes directly from the network stream. Changing even 1 bit invalidates the hash.
- **Threat T2: Mention injection (`@everyone` / `@here`)**
  - *Risk*: A malicious contributor includes `@everyone` in a commit message, branch name, or username to spam a Discord/Slack server.
  - *Mitigation*: **Universal field sanitization (`app/formatter.py`)**. Every text field (`repo_name`, `ref`, `sender_name`, `title`, commit messages) is passed through `sanitize_mentions()` and URL fields are validated with `validate_safe_url()` (`https://` only).
- **Threat T3: Stored XSS in Operations Dashboard via webhook or downstream error payloads**
  - *Risk*: Malicious repository names or downstream HTTP error bodies containing `<img src=x onerror=...>` executing JavaScript in an operator's browser.
  - *Mitigation*:
    1. **Zero `innerHTML` for data (`app/ui/dashboard.html`)**: All table cells and modal items are constructed via `document.createElement()` and populated strictly with `.textContent`.
    2. **Content-Security-Policy (`app/main.py`)**: Enforces `default-src 'self'; frame-ancestors 'none'; object-src 'none'` with zero external CDN dependencies.
- **Threat T4: Illegal delivery state transitions**
  - *Risk*: Application bug or race condition overwriting a `sent` or `discarded` delivery into `dead_letter` or vice versa.
  - *Mitigation*: **Formal State Machine (`VALID_STATE_TRANSITIONS` in `app/models.py`)** enforced inside `DeliveryStore._transition_state()`.

### R — Repudiation (Claiming an action was not performed)
- **Threat R1: Denying an administrative action or unauthorized probe**
  - *Risk*: An operator redrives/discards a DLQ event, or an attacker probes `/api/*`, without leaving forensic evidence.
  - *Mitigation*: **Immutable Security Audit Log (`audit_logs`)**. Records `auth_failed`, `authz_denied`, `auth_lockout`, `redrive_delivery`, and `discard_dlq` with `actor_role`, `target_id`, `ip_address`, `user_agent`, and timestamp—while never logging raw API keys.
- **Threat R2: Disputing webhook delivery attempts**
  - *Risk*: Downstream service claims HookRelay never sent the event.
  - *Mitigation*: **Granular Attempt History (`delivery_attempts`)** plus `X-HookRelay-Delivery-ID` request header on outgoing calls.

### I — Information Disclosure (Leaking confidential data)
- **Threat I1: Secret leak in application logs or audit trails**
  - *Risk*: Webhook secret, API keys, or Discord tokens appear in log files.
  - *Mitigation*: Credentials are strictly loaded via environment variables and never logged in error strings or audit entries.
- **Threat I2: Timing attack side-channel on HMAC or API key comparison**
  - *Risk*: Byte-by-byte string comparison (`==`) leaks key characters based on execution time.
  - *Mitigation*: Constant-time comparison using `hmac.compare_digest()` for both webhook signatures and SHA-256 API key hashes.
- **Threat I3: Server-Side Request Forgery (SSRF) via webhook destinations**
  - *Risk*: Routing webhooks to internal loopback (`127.0.0.1`, `localhost`), RFC1918 private networks (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), or cloud metadata services (`169.254.169.254`).
  - *Mitigation*: **`validate_ssrf_safe_url()` (`app/routing.py`)** enforces `https://` and blocks loopback, private, link-local, multicast, and metadata hostnames/IPs.

### D — Denial of Service (Exhausting resources)
- **Threat D1: Flooding `/webhook/github` with chunked multi-gigabyte bodies**
  - *Risk*: RAM exhaustion before `Content-Length` is checked.
  - *Mitigation*: **Streaming Body Cutoff (`read_bounded_body_stream` in `app/main.py`)** reads `request.stream()` chunk-by-chunk and aborts with HTTP 413 the instant accumulated bytes exceed 5 MB.
- **Threat D2: Oversized error response from malicious downstream server**
  - *Risk*: Downstream endpoint returns a 100 MB error body on HTTP 500, exhausting memory/logs.
  - *Mitigation*: **`read_bounded_error()` (`app/providers.py`)** truncates downstream error responses to `max_error_body_bytes` (2,048 chars).
- **Threat D3: Volumetric request floods against webhook or management APIs**
  - *Risk*: Exhausting DB connections or triggering endless redrive loops.
  - *Mitigation*: Tiered sliding-window rate limiters (`app/ratelimit.py`): 300/min for webhooks, 60/min for `/api/*`, 10/min for redrives, and 10/min for failed auth.

### E — Elevation of Privilege (Gaining unauthorized capabilities)
- **Threat E1: Viewer or Operator escalating privileges**
  - *Risk*: Read-only `VIEWER` triggering redrives, or `OPERATOR` discarding DLQ items or reading `audit_logs`.
  - *Mitigation*: **Strict RBAC dependency enforcement (`require_role` in `app/auth.py`)** returning HTTP 403 Forbidden and recording `authz_denied` in `audit_logs`.
- **Threat E2: Container breakout**
  - *Risk*: Compromised web worker gaining root host access.
  - *Mitigation*: Dockerfile runs under an unprivileged non-root user (`appuser`, UID 1000).
