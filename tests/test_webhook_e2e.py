import json
import pytest
import httpx
from app.config import settings
from app.store import DeliveryStore
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
async def test_api_deliveries_requires_authentication(async_client):
    # Without X-API-Key header -> 401
    resp_unauthed = await async_client.get("/api/deliveries")
    assert resp_unauthed.status_code == 401

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

    # Admin with invalid delivery -> 404 Not Found (auth passed)
    resp_admin = await async_client.post(
        "/api/deliveries/del-non-existent/redrive",
        headers={"X-API-Key": settings.admin_api_key}
    )
    assert resp_admin.status_code == 404


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

    # 1. First delivery should be accepted
    response_1 = await async_client.post("/webhook/github", content=body, headers=headers)
    assert response_1.status_code == 200
    assert response_1.json() == {"accepted": True, "delivery_id": "del-idempotency-test-01"}

    # 2. Duplicate recognized
    response_2 = await async_client.post("/webhook/github", content=body, headers=headers)
    assert response_2.status_code == 200
    assert response_2.json() == {"duplicate": True, "delivery_id": "del-idempotency-test-01"}


@pytest.mark.asyncio
async def test_healthz_and_metrics_endpoints(async_client):
    health = await async_client.get("/healthz")
    assert health.status_code == 200
    assert health.json().get("status") == "healthy"
    assert "queue_depth" in health.json()

    metrics = await async_client.get("/metrics")
    assert metrics.status_code == 200
    assert "hookrelay_webhook_requests_total" in metrics.text
