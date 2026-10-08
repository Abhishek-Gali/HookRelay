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
async def test_missing_signature_returns_401(async_client):
    body = b'{"action": "opened"}'
    headers = {
        "X-GitHub-Event": "issues",
        "X-GitHub-Delivery": "del-no-sig",
        "Content-Type": "application/json"
    }

    response = await async_client.post("/webhook/github", content=body, headers=headers)
    assert response.status_code == 401


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

    # 2. Second delivery with identical delivery_id must be recognized as duplicate
    response_2 = await async_client.post("/webhook/github", content=body, headers=headers)
    assert response_2.status_code == 200
    assert response_2.json() == {"duplicate": True, "delivery_id": "del-idempotency-test-01"}


@pytest.mark.asyncio
async def test_healthz_and_metrics_endpoints(async_client):
    health = await async_client.get("/healthz")
    assert health.status_code == 200
    assert health.json().get("status") == "healthy"

    metrics = await async_client.get("/metrics")
    assert metrics.status_code == 200
    assert "hookrelay_webhook_requests_total" in metrics.text
