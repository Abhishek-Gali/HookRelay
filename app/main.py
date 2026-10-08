import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any
import httpx
from fastapi import FastAPI, Request, HTTPException, Depends, status, Query, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from app.config import settings
from app.security import verify_signature
from app.store import store, DeliveryStore
from app.models import DeliveryDTO, AuditLogDTO
from app.auth import get_current_user_role, require_role, Role
from app.ratelimit import check_rate_limit
from app.routing import routing_engine, RouteDestination, RouteRule
from app.dispatcher import ResilientDispatcher
from app.queue_broker import get_queue_broker, BaseQueueBroker
from app.reconciliation import reconciliation_worker_loop
from app.metrics import (
    WEBHOOK_REQUESTS_TOTAL,
    DISCORD_DISPATCHES_TOTAL,
    WEBHOOK_RESPONSE_SECONDS
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("hookrelay.main")

# Core service singletons
dispatcher = ResilientDispatcher(store=store, max_retries=settings.max_retries, timeout=settings.request_timeout_seconds)
queue_broker: BaseQueueBroker = get_queue_broker(settings.redis_url)
http_client: Optional[httpx.AsyncClient] = None
worker_task: Optional[asyncio.Task] = None
reconciliation_task: Optional[asyncio.Task] = None
shutdown_event = asyncio.Event()


async def queue_worker_loop():
    """Continuous worker process consuming delivery jobs from the durable queue."""
    logger.info("Starting HookRelay durable queue worker loop...")
    while not shutdown_event.is_set():
        try:
            job = await queue_broker.dequeue()
            if not job:
                continue

            delivery_id = job["delivery_id"]
            event_type = job["event_type"]
            payload = job["payload"]
            destinations = [RouteDestination(**d) for d in job["destinations"]]

            logger.info(f"Worker processing job: {delivery_id} ({event_type}) to {len(destinations)} destination(s)")
            await dispatcher.dispatch_job(
                delivery_id=delivery_id,
                event_type=event_type,
                payload=payload,
                destinations=destinations,
                client=http_client
            )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error(f"Error in queue worker loop: {exc}", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown hooks."""
    global http_client, worker_task, reconciliation_task
    logger.info("Initializing HookRelay v2 Enterprise Engine...")
    await store.init_db()
    await queue_broker.start()
    http_client = httpx.AsyncClient(timeout=settings.request_timeout_seconds)

    # Start queue worker
    worker_task = asyncio.create_task(queue_worker_loop())

    # Start reconciliation crash-recovery worker
    if settings.enable_reconciliation:
        reconciliation_task = asyncio.create_task(
            reconciliation_worker_loop(
                store=store,
                sender=dispatcher,
                discord_webhook_url=settings.discord_webhook_url,
                client=http_client,
                interval_seconds=settings.reconciliation_interval_seconds,
                stale_threshold_seconds=settings.stale_threshold_seconds,
                stop_event=shutdown_event
            )
        )

    yield

    logger.info("Gracefully shutting down HookRelay...")
    shutdown_event.set()
    if worker_task:
        worker_task.cancel()
    if reconciliation_task:
        reconciliation_task.cancel()

    await queue_broker.stop()
    if http_client:
        await http_client.aclose()
    await store.close()


app = FastAPI(
    title="HookRelay v2 Enterprise",
    description="Secure, Observable, Multi-Provider Webhook Ingestion & Alert Gateway",
    version="2.0.0",
    lifespan=lifespan
)


# Security Headers Middleware
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


# =====================================================================
# 1. WEBHOOK INGESTION ENDPOINT
# =====================================================================
@app.post("/webhook/github", status_code=status.HTTP_200_OK, dependencies=[Depends(check_rate_limit)])
async def github_webhook(request: Request):
    """
    Primary ingestion endpoint for GitHub Webhooks.
    Protected by HMAC-SHA256, rate limiting, and size constraints.
    Enqueues tasks into the durable queue and responds in < 3ms.
    """
    start_time = time.perf_counter()

    # Payload size verification (DoS defense)
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

    # Constant-time HMAC-SHA256 verification
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

    # Fast ping ack
    if event_type == "ping":
        WEBHOOK_REQUESTS_TOTAL.labels(event="ping", outcome="ping").inc()
        WEBHOOK_RESPONSE_SECONDS.observe(time.perf_counter() - start_time)
        return {"ok": True, "message": "HookRelay ping acknowledged."}

    repo_name = payload.get("repository", {}).get("full_name")

    # Resolve routing destinations
    destinations = routing_engine.resolve_destinations(
        event_type=event_type,
        payload=payload,
        default_url=settings.discord_webhook_url
    )
    dest_str = ",".join(d.provider for d in destinations)

    # Atomic claim (prevents race condition replays)
    is_claimed = await store.claim_delivery(
        delivery_id=delivery_id,
        event_type=event_type,
        repo=repo_name,
        payload=payload,
        destinations=dest_str
    )

    if not is_claimed:
        WEBHOOK_REQUESTS_TOTAL.labels(event=event_type, outcome="duplicate").inc()
        WEBHOOK_RESPONSE_SECONDS.observe(time.perf_counter() - start_time)
        return {"duplicate": True, "delivery_id": delivery_id}

    # Enqueue to durable worker queue
    job_data = {
        "delivery_id": delivery_id,
        "event_type": event_type,
        "payload": payload,
        "destinations": [d.model_dump() for d in destinations]
    }
    await queue_broker.enqueue(job_data)

    WEBHOOK_REQUESTS_TOTAL.labels(event=event_type, outcome="accepted").inc()
    WEBHOOK_RESPONSE_SECONDS.observe(time.perf_counter() - start_time)
    return {"accepted": True, "delivery_id": delivery_id}


# =====================================================================
# 2. AUTHENTICATED MANAGEMENT & OPERATIONAL APIS (/api/*)
# =====================================================================
@app.get("/api/deliveries", response_model=List[DeliveryDTO])
async def list_deliveries(
    status: Optional[str] = Query(None, description="Filter: received, sent, failed, dead_letter"),
    limit: int = Query(50, ge=1, le=100),
    role: Role = Depends(require_role([Role.ADMIN, Role.OPERATOR, Role.VIEWER]))
):
    """Secure endpoint: list recent deliveries with attempt histories."""
    records = await store.list_deliveries(status=status, limit=limit)
    return [DeliveryDTO.model_validate(r) for r in records]


@app.get("/api/deliveries/{delivery_id}", response_model=DeliveryDTO)
async def get_delivery_by_id(
    delivery_id: str,
    role: Role = Depends(require_role([Role.ADMIN, Role.OPERATOR, Role.VIEWER]))
):
    """Retrieve full delivery record with its granular attempt history."""
    record = await store.get_delivery(delivery_id, include_attempts=True)
    if not record:
        raise HTTPException(status_code=404, detail="Delivery record not found.")
    return DeliveryDTO.model_validate(record)


@app.post("/api/deliveries/{delivery_id}/redrive")
async def redrive_delivery(
    delivery_id: str,
    request: Request,
    role: Role = Depends(require_role([Role.ADMIN, Role.OPERATOR]))
):
    """Replays or redrives a delivery via durable queue."""
    record = await store.get_delivery(delivery_id)
    if not record:
        raise HTTPException(status_code=404, detail="Delivery record not found.")

    payload = json.loads(record.payload) if record.payload else {}
    destinations = routing_engine.resolve_destinations(
        event_type=record.event_type,
        payload=payload,
        default_url=settings.discord_webhook_url
    )

    job_data = {
        "delivery_id": record.delivery_id,
        "event_type": record.event_type,
        "payload": payload,
        "destinations": [d.model_dump() for d in destinations]
    }
    await queue_broker.enqueue(job_data)

    client_ip = request.client.host if request.client else "unknown"
    await store.record_audit_log(
        actor_role=role.value,
        action="redrive_delivery",
        target_id=delivery_id,
        ip_address=client_ip,
        status="success"
    )

    return {"redriving": True, "delivery_id": delivery_id}


# Dead Letter Queue (DLQ) Management
@app.get("/api/dlq", response_model=List[DeliveryDTO])
async def list_dlq(
    limit: int = Query(50, ge=1, le=100),
    role: Role = Depends(require_role([Role.ADMIN, Role.OPERATOR, Role.VIEWER]))
):
    """Fetch deliveries that failed all retry attempts."""
    records = await store.fetch_dlq(limit=limit)
    return [DeliveryDTO.model_validate(r) for r in records]


@app.post("/api/dlq/{delivery_id}/discard")
async def discard_dlq(
    delivery_id: str,
    request: Request,
    role: Role = Depends(require_role([Role.ADMIN]))
):
    """Admin-only: discard an unfixable event from DLQ."""
    success = await store.discard_dlq(delivery_id)
    if not success:
        raise HTTPException(status_code=404, detail="DLQ delivery not found or already processed.")

    client_ip = request.client.host if request.client else "unknown"
    await store.record_audit_log(
        actor_role=role.value,
        action="discard_dlq",
        target_id=delivery_id,
        ip_address=client_ip,
        status="success"
    )
    return {"discarded": True, "delivery_id": delivery_id}


@app.get("/api/stats")
async def get_stats(
    role: Role = Depends(require_role([Role.ADMIN, Role.OPERATOR, Role.VIEWER]))
):
    """Aggregated stats for the live console."""
    return await store.get_stats()


@app.get("/api/audit-logs", response_model=List[AuditLogDTO])
async def list_audit_logs(
    limit: int = Query(50, ge=1, le=100),
    role: Role = Depends(require_role([Role.ADMIN]))
):
    """Admin-only: view security audit trail."""
    logs = await store.list_audit_logs(limit=limit)
    return [AuditLogDTO.model_validate(l) for l in logs]


# =====================================================================
# 3. OBSERVABILITY, HEALTH & LIVE DASHBOARD
# =====================================================================
@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    """Serves the live interactive operations console."""
    ui_path = os.path.join(os.path.dirname(__file__), "ui", "dashboard.html")
    if os.path.exists(ui_path):
        with open(ui_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Dashboard file not found</h1>", status_code=404)


@app.get("/healthz")
async def healthz():
    """Health check endpoint for Docker, Kubernetes, and uptime probes."""
    return {
        "status": "healthy",
        "service": "HookRelay",
        "version": "2.0.0",
        "queue_depth": queue_broker.size(),
        "environment": settings.environment
    }


@app.get("/metrics")
async def metrics():
    """Exposes Prometheus metrics for scrapers."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
