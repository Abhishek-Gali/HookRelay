# HookRelay Comprehensive Threat Model (STRIDE)

This document formalizes the threat model for **HookRelay v2**, evaluating threats against the **STRIDE** methodology (Spoofing, Tampering, Repudiation, Information Disclosure, Denial of Service, Elevation of Privilege) and mapping each threat to concrete technical controls.

---

## 1. System Architecture & Trust Boundaries

```
[ External Untrusted: GitHub / Attackers ]
                  │
          (HTTPS Boundary)
                  ▼
[ Boundary 1: Gateway Rate Limiting & DoS Shield ]
                  │
[ Boundary 2: Raw-Byte HMAC-SHA256 Verification ]
                  │
[ Boundary 3: Database Atomic Deduplication ]
                  │
[ Boundary 4: Durable Asynchronous Queue Broker ]
                  │
[ Boundary 5: Resilient Dispatcher (Discord/Slack/HTTP) ]
                  │
          (Authenticated API Boundary: API Key + RBAC)
                  ▼
[ Boundary 6: Management & DLQ Replay Console ]
```

---

## 2. STRIDE Threat Analysis & Defense Controls

### S — Spoofing (Impersonating something or someone else)
- **Threat S1: Forged webhook from malicious third party**
  - *Risk*: An attacker sends fabricated git push or deployment events to trigger false alarms or actions.
  - *Mitigation*: **HMAC-SHA256 signature verification**. GitHub signs every request with a shared secret. HookRelay verifies the digest over **raw bytes** using `hmac.compare_digest`. Requests with missing, mismatched, or unauthenticated signatures are dropped with HTTP 401 before hitting storage.
- **Threat S2: Unauthorized operator accessing management APIs**
  - *Risk*: An adversary queries `/api/deliveries` or triggers redrives by forging identity.
  - *Mitigation*: **Hashed API Key Authentication**. All `/api/*` endpoints require `X-API-Key` headers checked in constant time against SHA-256 precomputed key hashes.

### T — Tampering (Modifying data on wire or at rest)
- **Threat T1: Man-in-the-middle modifying webhook payload**
  - *Risk*: Modifying commit messages or issue IDs in transit.
  - *Mitigation*: Cryptographic HMAC covers the raw payload bytes directly from the network socket. Changing even 1 bit in transit invalidates the hash.
- **Threat T2: Mention injection (`@everyone` / `@here`)**
  - *Risk*: A malicious contributor includes `@everyone` in a commit message to spam a Discord/Slack server.
  - *Mitigation*: **Defensive mention sanitization**. `sanitize_mentions()` inserts zero-width spaces (`@\u200beveryone`), defanging pings without altering human readability.

### R — Repudiation (Claiming an action was not performed)
- **Threat R1: Denying an administrative action or DLQ replay**
  - *Risk*: An operator redrives or discards a dead-letter event and denies having triggered it.
  - *Mitigation*: **Immutable Audit Log Table (`audit_logs`)**. Every replay, redrive, and discard operation records `timestamp`, `actor_role`, `action`, `target_id`, and `ip_address`.
- **Threat R2: Disputing webhook delivery attempts**
  - *Risk*: Downstream service claims HookRelay never sent the event.
  - *Mitigation*: **Granular Attempt History (`delivery_attempts`)**. Every HTTP status (e.g., 429, 503, 204), response time in ms, and error response is persisted permanently per delivery attempt.

### I — Information Disclosure (Leaking confidential data)
- **Threat I1: Secret leak in application logs or stack traces**
  - *Risk*: Webhook secret or Discord tokens appear in log files.
  - *Mitigation*: Credentials are strictly loaded via Pydantic environment variables (`.env` or container env) and never included in log formatting or error strings.
- **Threat I2: Timing attack side-channel on HMAC or API key comparison**
  - *Risk*: Byte-by-byte string comparison (`==`) leaks key characters based on execution time.
  - *Mitigation*: Constant-time comparison using `hmac.compare_digest()` for both webhook signatures and API keys.

### D — Denial of Service (Exhausting resources)
- **Threat D1: Flooding `/webhook/github` with massive JSON bodies**
  - *Risk*: RAM exhaustion / Out-Of-Memory container termination.
  - *Mitigation*: Strict 5MB payload limit checked at `Content-Length` and raw byte streams before parsing.
- **Threat D2: High-frequency request flood**
  - *Risk*: Crashing the API gateway or overwhelming the database.
  - *Mitigation*: **Sliding-window IP rate limiter** capping requests to 300 requests/minute per client IP.
- **Threat D3: GitHub 10-second timeout exhaustion**
  - *Risk*: Slow downstream Discord response causes GitHub to mark delivery as failed.
  - *Mitigation*: Gateway responds to GitHub in **< 3ms**, offloading dispatch to the asynchronous queue broker.

### E — Elevation of Privilege (Gaining unauthorized capabilities)
- **Threat E1: Viewer role triggering redrive or DLQ modifications**
  - *Risk*: Read-only users triggering duplicate downstream traffic.
  - *Mitigation*: **Role-Based Access Control (RBAC)**. Endpoints enforce strict roles (`ADMIN`, `OPERATOR`, `VIEWER`). Read-only viewers attempting POST operations receive HTTP 403 Forbidden.
- **Threat E2: Container breakout**
  - *Risk*: Attacker compromises the web service and gains root host access.
  - *Mitigation*: Dockerfile runs under a dedicated, unprivileged non-root user (`appuser` with UID 1000).

---

## 3. Threat Matrix Summary

| STRIDE Category | Threat Description | Control Implemented | Status |
|---|---|---|---|
| **Spoofing** | Forged GitHub Webhook | Raw-bytes HMAC-SHA256 (`app/security.py`) | Verified |
| **Spoofing** | Unauthorized Admin Access | Hashed API Keys & RBAC (`app/auth.py`) | Verified |
| **Tampering** | Payload Modification | Raw-body HMAC verification | Verified |
| **Tampering** | Mention Injection | `sanitize_mentions` zero-width space | Verified |
| **Repudiation** | Denying Admin Replays | `audit_logs` persistent table | Verified |
| **Information Disclosure** | Timing Attacks | `hmac.compare_digest` | Verified |
| **Denial of Service** | Volumetric Request Floods | Sliding window rate limiting (`app/ratelimit.py`) | Verified |
| **Denial of Service** | Large Payload OOM | 5MB pre-parsing byte cutoff | Verified |
| **Elevation of Privilege** | Read-Only Users Modifying State | RBAC dependency injection (`require_role`) | Verified |
