import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Optional, List
import httpx
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, status, Query, Response
from fastapi.responses import JSONResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from app.config import settings
from app.security import verify_signature
from app.store import store, DeliveryStore
from app.formatter import format_payload
from app.sender import DiscordSender
from app.reconciliation import reconciliation_worker_loop
from app.models import DeliveryDTO
from app.metrics import (
    WEBHOOK_REQUESTS_TOTAL,
    DISCORD_DISPATCHES_TOTAL,
    DISCORD_RETRIES_TOTAL,
    WEBHOOK_RESPONSE_SECONDS
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("hookrelay.main")

# Shared resources
sender = DiscordSender(max_retries=settings.max_retries, timeout=settings.request_timeout_seconds)
http_client: Optional[httpx.AsyncClient] = None
reconciliation_task: Optional[asyncio.Task] = None
shutdown_event = asyncio.Event()


async def process_delivery(delivery_id: str, event_type: str, payload: dict):
    """
    Background worker task responsible for formatting and sending
    the webhook alert to Discord with full retry management and state update.
    """
    formatted_payload = format_payload(event_type, payload, use_embeds=settings.enable_embeds)
    try:
        attempts = await sender.send_to_discord(
            webhook_url=settings.discord_webhook_url,
            discord_payload=formatted_payload,
            client=http_client
        )
        if attempts > 1:
            DISCORD_RETRIES_TOTAL.inc(attempts - 1)
        await store.mark_sent(delivery_id=delivery_id, attempts=attempts)
        DISCORD_DISPATCHES_TOTAL.labels(event=event_type, status="sent").inc()
        logger.info(f"Delivery {delivery_id} marked as 'sent' after {attempts} attempt(s).")
    except Exception as exc:
        err_msg = str(exc)
        logger.error(f"Delivery {delivery_id} permanently failed: {err_msg}")
        await store.mark_failed(delivery_id=delivery_id, attempts=settings.max_retries, error=err_msg)
        DISCORD_DISPATCHES_TOTAL.labels(event=event_type, status="failed").inc()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown hooks."""
    global http_client, reconciliation_task
    logger.info("Initializing HookRelay database and HTTP pools...")
    await store.init_db()
    http_client = httpx.AsyncClient(timeout=settings.request_timeout_seconds)

    if settings.enable_reconciliation:
        reconciliation_task = asyncio.create_task(
            reconciliation_worker_loop(
                store=store,
                sender=sender,
                discord_webhook_url=settings.discord_webhook_url,
                client=http_client,
                interval_seconds=settings.reconciliation_interval_seconds,
                stale_threshold_seconds=settings.stale_threshold_seconds,
                stop_event=shutdown_event
            )
        )

    yield

    logger.info("Shutting down HookRelay...")
    shutdown_event.set()
    if reconciliation_task:
        reconciliation_task.cancel()
        try:
            await reconciliation_task
        except asyncio.CancelledError:
            pass

    if http_client:
        await http_client.aclose()
    await store.close()


app = FastAPI(
    title="HookRelay",
    description="Resilient GitHub Webhook to Discord Alert Service with HMAC & Idempotency",
    version="1.0.0",
    lifespan=lifespan
)


@app.post("/webhook/github", status_code=status.HTTP_200_OK)
async def github_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Primary ingestion endpoint for GitHub Webhooks.
    1. Check payload size constraint (< MAX_PAYLOAD_BYTES).
    2. Verify HMAC-SHA256 signature using raw bytes (401 if invalid).
    3. Extract headers: X-GitHub-Event and X-GitHub-Delivery.
    4. Handle ping events immediately.
    5. Atomically claim delivery ID in database (200 duplicate if seen).
    6. Return fast 200 acknowledgment to GitHub (< 20ms).
    7. Offload message formatting and Discord dispatch to background worker.
    """
    start_time = time.perf_counter()

    # Defend against oversized payloads (DoS)
    content_length = request.headers.get("Content-Length")
    if content_length and int(content_length) > settings.max_payload_bytes:
        WEBHOOK_REQUESTS_TOTAL.labels(event="unknown", outcome="rejected_size").inc()
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Payload size exceeds allowed limit."
        )

    raw_body = await request.body()
    if len(raw_body) > settings.max_payload_bytes:
        WEBHOOK_REQUESTS_TOTAL.labels(event="unknown", outcome="rejected_size").inc()
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Payload size exceeds allowed limit."
        )

    signature_header = request.headers.get("X-Hub-Signature-256")
    if not verify_signature(settings.github_webhook_secret, raw_body, signature_header):
        WEBHOOK_REQUESTS_TOTAL.labels(event="unknown", outcome="rejected_signature").inc()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-Hub-Signature-256 header."
        )

    event_type = request.headers.get("X-GitHub-Event", "unknown")
    delivery_id = request.headers.get("X-GitHub-Delivery", "")

    if not delivery_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing X-GitHub-Delivery header."
        )

    try:
        payload = json.loads(raw_body.decode("utf-8")) if raw_body else {}
    except json.JSONDecodeError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Malformed JSON body.")

    # GitHub ping event
    if event_type == "ping":
        WEBHOOK_REQUESTS_TOTAL.labels(event="ping", outcome="ping").inc()
        WEBHOOK_RESPONSE_SECONDS.observe(time.perf_counter() - start_time)
        return {"ok": True, "message": "HookRelay ping acknowledged."}

    repo_name = payload.get("repository", {}).get("full_name")

    # Race-proof atomic claim
    is_claimed = await store.claim_delivery(
        delivery_id=delivery_id,
        event_type=event_type,
        repo=repo_name,
        payload=payload
    )

    if not is_claimed:
        WEBHOOK_REQUESTS_TOTAL.labels(event=event_type, outcome="duplicate").inc()
        WEBHOOK_RESPONSE_SECONDS.observe(time.perf_counter() - start_time)
        return {"duplicate": True, "delivery_id": delivery_id}

    # Queue downstream work in background
    background_tasks.add_task(process_delivery, delivery_id, event_type, payload)

    WEBHOOK_REQUESTS_TOTAL.labels(event=event_type, outcome="accepted").inc()
    WEBHOOK_RESPONSE_SECONDS.observe(time.perf_counter() - start_time)
    return {"accepted": True, "delivery_id": delivery_id}


@app.get("/healthz")
async def healthz():
    """Health check endpoint for Docker, Kubernetes, and uptime probes."""
    return {
        "status": "healthy",
        "service": "HookRelay",
        "environment": settings.environment
    }


@app.get("/metrics")
async def metrics():
    """Exposes Prometheus metrics for scrapers."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/deliveries", response_model=List[DeliveryDTO])
async def list_deliveries(
    status: Optional[str] = Query(None, description="Filter by status: received, sent, failed"),
    limit: int = Query(50, ge=1, le=100)
):
    """Admin endpoint to inspect recent webhook deliveries."""
    records = await store.list_deliveries(status=status, limit=limit)
    return [DeliveryDTO.model_validate(r) for r in records]


@app.get("/api/deliveries/{delivery_id}", response_model=DeliveryDTO)
async def get_delivery_by_id(delivery_id: str):
    """Retrieve details for a specific delivery ID."""
    record = await store.get_delivery(delivery_id)
    if not record:
        raise HTTPException(status_code=404, detail="Delivery record not found.")
    return DeliveryDTO.model_validate(record)


@app.post("/api/deliveries/{delivery_id}/redrive")
async def redrive_delivery(delivery_id: str, background_tasks: BackgroundTasks):
    """Manually redrive a delivery (e.g. after downstream recovery)."""
    record = await store.get_delivery(delivery_id)
    if not record:
        raise HTTPException(status_code=404, detail="Delivery record not found.")

    payload = json.loads(record.payload) if record.payload else {}
    background_tasks.add_task(process_delivery, record.delivery_id, record.event_type, payload)
    return {"redriving": True, "delivery_id": delivery_id}
