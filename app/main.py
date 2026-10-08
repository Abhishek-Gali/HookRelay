import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional, List
import httpx
from fastapi import FastAPI, Request, HTTPException, Depends, status, Query, Response
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from app.config import settings
from app.security import verify_signature
from app.store import store, IllegalStateTransitionError
from app.models import DeliveryDTO, AuditLogDTO
from app.auth import (
    get_current_user_role,
    require_role,
    Role,
    authenticate_raw_key,
    session_manager,
    _audit_security_event,
)
from app.ratelimit import (
    check_rate_limit,
    check_api_rate_limit,
    check_redrive_rate_limit,
    auth_failure_limiter,
)
from app.routing import routing_engine, RouteDestination, parse_persisted_destinations
from app.dispatcher import ResilientDispatcher
from app.queue_broker import get_queue_broker, BaseQueueBroker, DatabaseQueueBroker
from app.reconciliation import reconciliation_worker_loop
from app.metrics import (
    WEBHOOK_REQUESTS_TOTAL,
    WEBHOOK_RESPONSE_SECONDS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("hookrelay.main")

# Core service singletons
dispatcher = ResilientDispatcher(
    store=store,
    max_retries=settings.max_retries,
    timeout=settings.request_timeout_seconds
)
queue_broker: BaseQueueBroker = get_queue_broker(delivery_store=store)
http_client: Optional[httpx.AsyncClient] = None
worker_task: Optional[asyncio.Task] = None
reconciliation_task: Optional[asyncio.Task] = None
shutdown_event = asyncio.Event()


def _sync_store_bindings() -> None:
    """Ensures dispatcher and queue_broker always reference the active module store."""
    dispatcher.store = store
    if isinstance(queue_broker, DatabaseQueueBroker):
        queue_broker.store = store


async def read_bounded_body_stream(request: Request, max_bytes: int) -> bytes:
    """
    Streams the incoming HTTP request body chunk-by-chunk and enforces a hard
    streaming memory ceiling BEFORE buffering an arbitrarily large payload.
    Raises HTTP 413 immediately if cumulative bytes exceed max_bytes.
    """
    content_length = request.headers.get("Content-Length")
    if content_length:
        try:
            if int(content_length) > max_bytes:
                WEBHOOK_REQUESTS_TOTAL.labels(event="unknown", outcome="rejected_size").inc()
                raise HTTPException(
                    status_code=413,
                    detail="Payload size exceeds allowed limit."
                )
        except ValueError:
            pass

    buffer = bytearray()
    async for chunk in request.stream():
        buffer.extend(chunk)
        if len(buffer) > max_bytes:
            WEBHOOK_REQUESTS_TOTAL.labels(event="unknown", outcome="rejected_size").inc()
            raise HTTPException(
                status_code=413,
                detail="Payload stream exceeds maximum allowed size."
            )
    return bytes(buffer)


async def queue_worker_loop():
    """Continuous worker process consuming leased jobs from the durable SQL queue."""
    logger.info("Starting HookRelay SQL-backed durable queue worker loop...")
    while not shutdown_event.is_set():
        try:
            _sync_store_bindings()
            job = await queue_broker.dequeue()
            if not job:
                continue

            delivery_id = job["delivery_id"]
            event_type = job["event_type"]
            payload = job["payload"]
            destinations = [RouteDestination(**d) for d in job["destinations"]]
            wid = job.get("worker_id")
            lease_gen = job.get("lease_generation")
            trigger_type = job.get("trigger_type", "initial")

            logger.info(
                f"Worker [{wid} gen={lease_gen}] processing leased job: "
                f"{delivery_id} ({event_type}) -> {len(destinations)} destination(s)"
            )
            await dispatcher.dispatch_job(
                delivery_id=delivery_id,
                event_type=event_type,
                payload=payload,
                destinations=destinations,
                client=http_client,
                worker_id=wid,
                lease_generation=lease_gen,
                trigger_type=trigger_type
            )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error(f"Error in queue worker loop: {exc}", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown hooks."""
    global http_client, worker_task, reconciliation_task
    logger.info("Initializing HookRelay v2.1 Hardened Engine...")
    _sync_store_bindings()
    await store.init_db()
    await queue_broker.start()
    http_client = httpx.AsyncClient(timeout=settings.request_timeout_seconds, follow_redirects=False)

    worker_task = asyncio.create_task(queue_worker_loop())

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
    title="HookRelay",
    description="Hardened, Observable, Multi-Provider Webhook Ingestion & Alert Gateway",
    version="2.1.0",
    lifespan=lifespan
)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "0"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self'; "
        "style-src 'self'; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "frame-ancestors 'none'; "
        "object-src 'none'; "
        "base-uri 'self';"
    )
    return response


# =====================================================================
# 1. WEBHOOK INGESTION ENDPOINT
# =====================================================================
@app.post("/webhook/github", status_code=status.HTTP_200_OK, dependencies=[Depends(check_rate_limit)])
async def github_webhook(request: Request):
    """
    Primary ingestion endpoint for GitHub Webhooks.
    1. Enforces streaming payload size cutoff (< MAX_PAYLOAD_BYTES).
    2. Verifies HMAC-SHA256 signature over raw bytes in constant time.
    3. Resolves routing rules & persists structured destination JSON atomically in SQL.
    4. Signals durable queue worker and returns fast 200 acknowledgment.
    """
    start_time = time.perf_counter()
    _sync_store_bindings()

    raw_body = await read_bounded_body_stream(request, settings.max_payload_bytes)

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
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Malformed JSON body.")

    if event_type == "ping":
        WEBHOOK_REQUESTS_TOTAL.labels(event="ping", outcome="ping").inc()
        WEBHOOK_RESPONSE_SECONDS.observe(time.perf_counter() - start_time)
        return {"ok": True, "message": "HookRelay ping acknowledged."}

    repo_name = payload.get("repository", {}).get("full_name")

    destinations = routing_engine.resolve_destinations(
        event_type=event_type,
        payload=payload,
        default_url=settings.discord_webhook_url
    )
    destinations_list = [d.model_dump() for d in destinations]

    # Atomic SQL claim persists payload, SHA-256 replay fingerprint, and exact structured destinations
    is_claimed = await store.claim_delivery(
        delivery_id=delivery_id,
        event_type=event_type,
        repo=repo_name,
        payload=payload,
        destinations=destinations_list,
        raw_body=raw_body,
        replay_window_seconds=settings.replay_window_seconds
    )

    if not is_claimed:
        WEBHOOK_REQUESTS_TOTAL.labels(event=event_type, outcome="duplicate").inc()
        WEBHOOK_RESPONSE_SECONDS.observe(time.perf_counter() - start_time)
        return {"duplicate": True, "delivery_id": delivery_id}

    job_data = {
        "delivery_id": delivery_id,
        "event_type": event_type,
        "payload": payload,
        "destinations": destinations_list
    }
    await queue_broker.enqueue(job_data)

    WEBHOOK_REQUESTS_TOTAL.labels(event=event_type, outcome="accepted").inc()
    WEBHOOK_RESPONSE_SECONDS.observe(time.perf_counter() - start_time)
    return {"accepted": True, "delivery_id": delivery_id}


# =====================================================================
# 2. AUTHENTICATED & RATE-LIMITED MANAGEMENT APIS (/api/*)
# =====================================================================
@app.get(
    "/api/deliveries",
    response_model=List[DeliveryDTO],
    dependencies=[Depends(check_api_rate_limit)]
)
async def list_deliveries(
    status: Optional[str] = Query(None, description="Filter: received, queued, processing, sent, dead_letter"),
    limit: int = Query(50, ge=1, le=100),
    role: Role = Depends(require_role([Role.ADMIN, Role.OPERATOR, Role.VIEWER]))
):
    """Secure endpoint: list recent deliveries with attempt histories."""
    _sync_store_bindings()
    records = await store.list_deliveries(status=status, limit=limit)
    return [DeliveryDTO.model_validate(r) for r in records]


@app.get(
    "/api/deliveries/{delivery_id}",
    response_model=DeliveryDTO,
    dependencies=[Depends(check_api_rate_limit)]
)
async def get_delivery_by_id(
    delivery_id: str,
    role: Role = Depends(require_role([Role.ADMIN, Role.OPERATOR, Role.VIEWER]))
):
    """Retrieve full delivery record with its granular attempt history."""
    _sync_store_bindings()
    record = await store.get_delivery(delivery_id, include_attempts=True)
    if not record:
        raise HTTPException(status_code=404, detail="Delivery record not found.")
    return DeliveryDTO.model_validate(record)


@app.post(
    "/api/deliveries/{delivery_id}/redrive",
    dependencies=[Depends(check_api_rate_limit), Depends(check_redrive_rate_limit)]
)
async def redrive_delivery(
    delivery_id: str,
    request: Request,
    reroute: bool = Query(False, description="If true, re-evaluates current routing rules instead of original persisted destinations"),
    role: Role = Depends(require_role([Role.ADMIN, Role.OPERATOR]))
):
    """
    Replays or redrives a failed/dead_letter delivery via the durable SQL queue.
    By default preserves the exact destinations persisted at original ingestion time,
    filtering out destinations that already succeeded ('status' == 'sent') so partial
    failures never duplicate already-successful targets (HR-06).
    """
    _sync_store_bindings()
    record = await store.get_delivery(delivery_id)
    if not record:
        raise HTTPException(status_code=404, detail="Delivery record not found.")

    payload = json.loads(record.payload) if record.payload else {}
    if reroute:
        destinations = routing_engine.resolve_destinations(
            event_type=record.event_type,
            payload=payload,
            default_url=settings.discord_webhook_url
        )
    else:
        destinations = parse_persisted_destinations(
            record.destinations,
            default_url=settings.discord_webhook_url,
            only_unsent=True
        )

    dest_dicts = [{"provider": d.provider, "url": d.url} for d in destinations]
    try:
        await store.prepare_for_redrive(
            delivery_id=delivery_id,
            new_destinations=dest_dicts if reroute else None
        )
    except IllegalStateTransitionError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))

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
        user_agent=request.headers.get("User-Agent", ""),
        status="success",
        details=f"reroute={reroute}"
    )

    return {"redriving": True, "delivery_id": delivery_id, "destinations": dest_dicts}


@app.get(
    "/api/dlq",
    response_model=List[DeliveryDTO],
    dependencies=[Depends(check_api_rate_limit)]
)
async def list_dlq(
    limit: int = Query(50, ge=1, le=100),
    role: Role = Depends(require_role([Role.ADMIN, Role.OPERATOR, Role.VIEWER]))
):
    """Fetch deliveries that failed all retry attempts."""
    _sync_store_bindings()
    records = await store.fetch_dlq(limit=limit)
    return [DeliveryDTO.model_validate(r) for r in records]


@app.post(
    "/api/dlq/{delivery_id}/discard",
    dependencies=[Depends(check_api_rate_limit), Depends(check_redrive_rate_limit)]
)
async def discard_dlq(
    delivery_id: str,
    request: Request,
    role: Role = Depends(require_role([Role.ADMIN]))
):
    """Admin-only: discard an unfixable event from DLQ."""
    _sync_store_bindings()
    success = await store.discard_dlq(delivery_id)
    if not success:
        raise HTTPException(status_code=404, detail="DLQ delivery not found or already processed.")

    client_ip = request.client.host if request.client else "unknown"
    await store.record_audit_log(
        actor_role=role.value,
        action="discard_dlq",
        target_id=delivery_id,
        ip_address=client_ip,
        user_agent=request.headers.get("User-Agent", ""),
        status="success"
    )
    return {"discarded": True, "delivery_id": delivery_id}


@app.get("/api/stats", dependencies=[Depends(check_api_rate_limit)])
async def get_stats(
    role: Role = Depends(require_role([Role.ADMIN, Role.OPERATOR, Role.VIEWER]))
):
    """Aggregated stats for the live console."""
    _sync_store_bindings()
    return await store.get_stats()


@app.get(
    "/api/audit-logs",
    response_model=List[AuditLogDTO],
    dependencies=[Depends(check_api_rate_limit)]
)
async def list_audit_logs(
    limit: int = Query(50, ge=1, le=100),
    role: Role = Depends(require_role([Role.ADMIN]))
):
    """Admin-only: view security audit trail."""
    _sync_store_bindings()
    logs = await store.list_audit_logs(limit=limit)
    return [AuditLogDTO.model_validate(l) for l in logs]


# =====================================================================
# 3. DASHBOARD SESSION AUTHENTICATION (HttpOnly Cookie + CSRF)
# =====================================================================
class LoginRequest(BaseModel):
    api_key: str


@app.post("/api/auth/login", dependencies=[Depends(check_api_rate_limit)])
async def login_session(body: LoginRequest, request: Request, response: Response):
    """
    Exchanges a valid API key for an HttpOnly, SameSite=Strict session cookie
    and a per-session CSRF token so the browser console does not retain raw keys.
    """
    _sync_store_bindings()
    client_ip = request.client.host if request.client else "unknown"
    if auth_failure_limiter.count_recent(client_ip) >= settings.auth_failure_limit_per_minute:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many failed authentication attempts. Try again later."
        )

    role = authenticate_raw_key(body.api_key)
    if role is None:
        auth_failure_limiter.is_allowed(client_ip, settings.auth_failure_limit_per_minute)
        await _audit_security_event(
            request,
            actor_role="UNAUTHENTICATED",
            action="auth_failed",
            status_str="denied",
            details="Invalid API key at /api/auth/login"
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API Key.")

    session_id, csrf_token = session_manager.create_session(role)
    response.set_cookie(
        key="hr_session",
        value=session_id,
        httponly=True,
        samesite="strict",
        secure=(settings.environment == "production"),
        max_age=28800,
        path="/"
    )
    await _audit_security_event(
        request,
        actor_role=role.value,
        action="session_login",
        status_str="success",
        details="Browser session created"
    )
    return {"authenticated": True, "role": role.value, "csrf_token": csrf_token}


@app.post(
    "/api/auth/logout",
    dependencies=[Depends(check_api_rate_limit)]
)
async def logout_session(
    request: Request,
    response: Response,
    role: Role = Depends(require_role([Role.ADMIN, Role.OPERATOR, Role.VIEWER]))
):
    """Revokes the active browser session (requiring auth + CSRF token) and clears the HttpOnly cookie (HR-14)."""
    session_cookie = request.cookies.get("hr_session")
    if session_cookie:
        session_manager.revoke_session(session_cookie)
    response.delete_cookie(key="hr_session", path="/")
    await _audit_security_event(
        request,
        actor_role=role.value,
        action="session_logout",
        status_str="success",
        details="Browser session terminated"
    )
    return {"authenticated": False}


@app.get("/api/auth/me", dependencies=[Depends(check_api_rate_limit)])
async def current_session_info(request: Request):
    """Returns current session state and CSRF token if an HttpOnly session is active."""
    session_cookie = request.cookies.get("hr_session")
    sess = session_manager.get_session(session_cookie)
    if not sess:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No active session.")
    role, csrf_token = sess
    return {"authenticated": True, "role": role.value, "csrf_token": csrf_token}


# =====================================================================
# 4. OBSERVABILITY, LIVENESS/READINESS PROBES & LIVE DASHBOARD
# =====================================================================
_ALLOWED_STATIC_ASSETS = {
    "dashboard.css": ("text/css; charset=utf-8", "dashboard.css"),
    "dashboard.js": ("application/javascript; charset=utf-8", "dashboard.js"),
}


@app.get("/static/{asset_name}")
async def serve_static_asset(asset_name: str):
    """
    Serves whitelisted static dashboard assets from memory using plain Response
    instead of FileResponse to eliminate Range-header parsing attack surface (HR-01).
    """
    if asset_name not in _ALLOWED_STATIC_ASSETS:
        raise HTTPException(status_code=404, detail="Static asset not found.")
    media_type, filename = _ALLOWED_STATIC_ASSETS[asset_name]
    asset_path = os.path.join(os.path.dirname(__file__), "ui", filename)
    if not os.path.exists(asset_path):
        raise HTTPException(status_code=404, detail="Static asset file missing.")
    with open(asset_path, "rb") as f:
        content = f.read()
    return Response(
        content=content,
        media_type=media_type,
        headers={"Cache-Control": "public, max-age=300"}
    )


@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    """Serves the XSS-hardened operations console."""
    ui_path = os.path.join(os.path.dirname(__file__), "ui", "dashboard.html")
    if os.path.exists(ui_path):
        with open(ui_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Dashboard file not found</h1>", status_code=404)


@app.get("/health/live")
async def health_live():
    """Minimal liveness probe without operational info disclosure (HR-13)."""
    return {"status": "alive"}


@app.get("/health/ready")
@app.get("/healthz")
async def health_ready():
    """
    Readiness probe: actively verifies SQL database connectivity (SELECT 1)
    without leaking environment or internal queue depth on unauthenticated endpoints (HR-13).
    """
    _sync_store_bindings()
    db_ok = await store.check_health()
    if not db_ok:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unhealthy", "database": "unreachable"}
        )

    return {
        "status": "healthy",
        "database": "connected"
    }


@app.get("/metrics")
async def metrics(request: Request):
    """
    Exposes Prometheus metrics.
    Requires authentication by default (settings.require_metrics_auth = True, HR-12).
    """
    if settings.require_metrics_auth:
        await get_current_user_role(request=request, x_api_key=request.headers.get("X-API-Key"))
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
