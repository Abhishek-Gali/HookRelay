# Contributing to HookRelay

Thank you for your interest in contributing to **HookRelay**! Our goal is to build a rock-solid, self-hosted webhook delivery gateway that any engineering team can deploy in minutes.

---

## 1. Local Development Setup (Under 60 Seconds)

```bash
git clone https://github.com/Abhishek-Gali/HookRelay.git
cd HookRelay
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

### Run the Chaos & Recovery Demo
```bash
python -m scripts.demo_chaos_recovery
```

### Run the Verification & DevSecOps Gate
Before opening a Pull Request, ensure all checks pass locally:
```bash
python -m ruff check app/ tests/ scripts/
python -m mypy app/
python -m bandit -r app/ -ll -ii
python -m pytest tests/ -v
```

---

## 2. High-Impact Areas for Contributors

1. **New Destination Providers (`app/providers.py`)**:
   - Subclass `WebhookProvider` in `app/providers.py` to add native formatting for **PagerDuty Events API v2**, **Microsoft Teams Workflows**, **Telegram Bot API**, **Ntfy.sh**, or **Email (SMTP / Resend / SES)**.
2. **New Source Signature Verifiers (`app/security.py` & `app/main.py`)**:
   - Extend `/webhook/{source}` to verify **Shopify (`X-Shopify-Hmac-Sha256`)**, **Linear (`Linear-Signature`)**, **Svix**, or **Slack Event Subscriptions**.
3. **Observability & OpenTelemetry**:
   - Add optional OpenTelemetry (`OTLP`) span propagation across webhook ingestion and distributed worker dispatch.

---

## 3. Pull Request Guidelines

- Include unit or integration tests in `tests/` for any new feature or bug fix.
- Maintain strict raw-byte cryptographic verification (`hmac.compare_digest`) and connect-time SSRF validation (`resolve_and_pin_destination`) for all network paths.
- Never log raw API keys, webhook tokens, or unredacted destination query strings.
