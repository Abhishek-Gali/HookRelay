import json
import pytest
from app.config import settings
import app.main as main_module


@pytest.mark.asyncio
async def test_ping_event_handling(async_client, sign_payload):
    body = b'{"zen": "Practicality beats purity.", "repository": {"full_name": "owner/repo"}}'
    headers = {
        "X-Hub-Signature-256": sign_payload(body),
        "X-GitHub-Event": "ping",
        "X-GitHub-Delivery": "ping-delivery-001",
        "Content-Type": "application/json"
    }

    response = await async_client.post("/webhook/github", content=body, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data.get("ok") is True


@pytest.mark.asyncio
async def test_invalid_signature_returns_401(async_client):
    body = b'{"action": "opened"}'
    headers = {
        "X-Hub-Signature-256": "sha256=invalid0000000000000000000000000000000000000000000000000000000000",
        "X-GitHub-Event": "issues",
        "X-GitHub-Delivery": "del-invalid-sig",
        "Content-Type": "application/json"
    }

    response = await async_client.post("/webhook/github", content=body, headers=headers)
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_api_deliveries_requires_authentication_and_audits_failures(async_client):
    # Without X-API-Key header -> 401
    resp_unauthed = await async_client.get("/api/deliveries")
    assert resp_unauthed.status_code == 401

    # With invalid key -> 401 and audit log recorded WITHOUT leaking raw key
    secret_attempt = "super_sensitive_wrong_key_do_not_log"
    resp_bad = await async_client.get("/api/deliveries", headers={"X-API-Key": secret_attempt})
    assert resp_bad.status_code == 401

    # Verify audit log recorded auth_failed and did NOT log secret_attempt
    audit_resp = await async_client.get("/api/audit-logs", headers={"X-API-Key": settings.admin_api_key})
    assert audit_resp.status_code == 200
    logs = audit_resp.json()
    assert any(l["action"] == "auth_failed" for l in logs)
    assert secret_attempt not in json.dumps(logs)

    # With valid Viewer key -> 200
    resp_authed = await async_client.get("/api/deliveries", headers={"X-API-Key": settings.viewer_api_key})
    assert resp_authed.status_code == 200


@pytest.mark.asyncio
async def test_dlq_and_redrive_rbac(async_client):
    # Viewer cannot redrive -> 403 Forbidden
    resp_viewer = await async_client.post(
        "/api/deliveries/del-test-123/redrive",
        headers={"X-API-Key": settings.viewer_api_key}
    )
    assert resp_viewer.status_code == 403

    # Operator cannot discard DLQ -> 403 Forbidden (Admin only)
    resp_op_discard = await async_client.post(
        "/api/dlq/del-test-123/discard",
        headers={"X-API-Key": settings.operator_api_key}
    )
    assert resp_op_discard.status_code == 403

    # Admin with invalid delivery -> 404 Not Found (auth passed)
    resp_admin = await async_client.post(
        "/api/deliveries/del-non-existent/redrive",
        headers={"X-API-Key": settings.admin_api_key}
    )
    assert resp_admin.status_code == 404


@pytest.mark.asyncio
async def test_management_rate_limit(async_client, monkeypatch):
    """Proves management API rate limiting blocks excessive requests per API key."""
    monkeypatch.setattr(settings, "api_rate_limit_per_minute", 3)
    headers = {"X-API-Key": settings.viewer_api_key}

    for _ in range(3):
        r = await async_client.get("/api/stats", headers=headers)
        assert r.status_code == 200

    # 4th request within the same minute window must return 429
    r_blocked = await async_client.get("/api/stats", headers=headers)
    assert r_blocked.status_code == 429


@pytest.mark.asyncio
async def test_dashboard_xss_hardening_and_csp(async_client):
    """
    Proves the dashboard:
    1. Does not ship with hardcoded 'hr_admin_secret_key_12345'.
    2. Does not load external third-party CDNs (cdnjs.cloudflare.com).
    3. Does not use innerHTML to render untrusted delivery/error fields.
    4. Includes a strict Content-Security-Policy header.
    """
    resp = await async_client.get("/dashboard")
    assert resp.status_code == 200
    html = resp.text

    assert "hr_admin_secret_key_12345" not in html
    assert "cdnjs.cloudflare.com" not in html
    assert "innerHTML" not in html
    assert "textContent" in html

    csp = resp.headers.get("Content-Security-Policy", "")
    assert "default-src 'self'" in csp
    assert "frame-ancestors 'none'" in csp


@pytest.mark.asyncio
async def test_streaming_body_limit_enforced_413(async_client, monkeypatch):
    """Proves oversized payloads are rejected with 413 during streaming read."""
    monkeypatch.setattr(settings, "max_payload_bytes", 128)
    oversized_body = b'{"data": "' + (b"x" * 512) + b'"}'
    resp = await async_client.post(
        "/webhook/github",
        content=oversized_body,
        headers={
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "del-oversized-01",
            "Content-Type": "application/json",
        }
    )
    assert resp.status_code == 413


@pytest.mark.asyncio
async def test_redrive_preserves_original_destinations(async_client):
    """
    Proves that redriving a dead-letter delivery preserves the original destinations
    saved at ingestion time rather than silently rerouting to a changed default.
    """
    orig_dest = [{"provider": "slack", "url": "https://hooks.slack.com/services/T00/B00/ORIG"}]
    await main_module.store.claim_delivery(
        delivery_id="del-redrive-orig-01",
        event_type="push",
        repo="Abhishek-Gali/HookRelay",
        payload={"repository": {"full_name": "Abhishek-Gali/HookRelay"}},
        destinations=orig_dest,
    )
    await main_module.store.mark_failed_or_dlq("del-redrive-orig-01", attempts=5, error="Slack timeout")

    resp = await async_client.post(
        "/api/deliveries/del-redrive-orig-01/redrive",
        headers={"X-API-Key": settings.operator_api_key}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["redriving"] is True
    assert data["destinations"] == orig_dest


@pytest.mark.asyncio
async def test_metrics_access_control(async_client, monkeypatch):
    """Proves /metrics can enforce API key authentication when require_metrics_auth=True."""
    monkeypatch.setattr(settings, "require_metrics_auth", True)

    r_unauthed = await async_client.get("/metrics")
    assert r_unauthed.status_code == 401

    r_authed = await async_client.get("/metrics", headers={"X-API-Key": settings.viewer_api_key})
    assert r_authed.status_code == 200
    assert "hookrelay_webhook_requests_total" in r_authed.text


@pytest.mark.asyncio
async def test_valid_delivery_accepted_and_deduplicated(async_client, sign_payload):
    body = json.dumps({
        "action": "opened",
        "issue": {"title": "Critical login crash", "html_url": "https://github.com/app/issues/1"},
        "repository": {"full_name": "org/app"},
        "sender": {"login": "alice"}
    }).encode("utf-8")

    sig = sign_payload(body)
    headers = {
        "X-Hub-Signature-256": sig,
        "X-GitHub-Event": "issues",
        "X-GitHub-Delivery": "del-idempotency-test-01",
        "Content-Type": "application/json"
    }

    response_1 = await async_client.post("/webhook/github", content=body, headers=headers)
    assert response_1.status_code == 200
    assert response_1.json() == {"accepted": True, "delivery_id": "del-idempotency-test-01"}

    response_2 = await async_client.post("/webhook/github", content=body, headers=headers)
    assert response_2.status_code == 200
    assert response_2.json() == {"duplicate": True, "delivery_id": "del-idempotency-test-01"}


@pytest.mark.asyncio
async def test_healthz_live_and_ready_endpoints(async_client):
    live = await async_client.get("/health/live")
    assert live.status_code == 200
    assert live.json()["status"] == "alive"

    ready = await async_client.get("/health/ready")
    assert ready.status_code == 200
    assert ready.json()["status"] == "healthy"
    assert ready.json()["database"] == "connected"
    assert "queue_depth" in ready.json()
