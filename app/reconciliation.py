import asyncio
from datetime import datetime, timezone, timedelta
import json
import logging
from typing import Optional, Union
import uuid
import httpx

from app.config import settings
from app.dispatcher import ResilientDispatcher
from app.formatter import format_payload
from app.routing import parse_persisted_destinations
from app.sender import DiscordSender
from app.store import DeliveryStore
from app.metrics import RECONCILIATION_RUNS_TOTAL, RECONCILIATION_RECOVERED_TOTAL

logger = logging.getLogger("hookrelay.reconciliation")


async def run_reconciliation_cycle(
    store: DeliveryStore,
    sender: Union[ResilientDispatcher, DiscordSender],
    discord_webhook_url: str,
    client: httpx.AsyncClient,
    stale_threshold_seconds: int = 120,
    batch_size: Optional[int] = None,
    max_batches: Optional[int] = None,
    worker_id: Optional[str] = None,
) -> int:
    """
    Executes a reconciliation sweep with atomic worker leasing and original destination preservation.
    1. Queries candidate stale deliveries in batches.
    2. Attempts an atomic lease (UPDATE ... WHERE ... RETURNING) per delivery.
       If another worker or replica already claimed it, skips it (preventing duplicate sends).
    3. Reconstructs the exact persisted destinations (Discord, Slack, HTTP) and dispatches.
    """
    RECONCILIATION_RUNS_TOTAL.inc()
    effective_batch_size = batch_size or settings.reconciliation_batch_size
    effective_max_batches = max_batches or settings.reconciliation_max_batches_per_cycle
    wid = worker_id or f"reconciler-{uuid.uuid4().hex[:8]}"
    recovered_count = 0

    for _ in range(effective_max_batches):
        stale_candidates = await store.fetch_stale_deliveries(
            older_than_seconds=stale_threshold_seconds,
            batch_size=effective_batch_size
        )
        if not stale_candidates:
            break

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=stale_threshold_seconds)
        leased_in_batch = 0

        for candidate in stale_candidates:
            # Atomic lease acquisition prevents race conditions across multiple workers/replicas
            leased = await store.acquire_lease(
                delivery_id=candidate.delivery_id,
                worker_id=wid,
                lease_seconds=settings.worker_lease_seconds,
                require_stale_before=cutoff
            )
            if leased is None:
                continue

            leased_in_batch += 1
            payload = {}
            if leased.payload:
                try:
                    payload = json.loads(leased.payload)
                except Exception:
                    payload = {}

            destinations = parse_persisted_destinations(
                leased.destinations,
                default_url=discord_webhook_url,
                only_unsent=True
            )

            if isinstance(sender, ResilientDispatcher):
                ok = await sender.dispatch_job(
                    delivery_id=leased.delivery_id,
                    event_type=leased.event_type,
                    payload=payload,
                    destinations=destinations,
                    client=client,
                    worker_id=wid,
                    lease_generation=leased.lease_generation,
                    trigger_type="reconciliation"
                )
                if ok:
                    RECONCILIATION_RECOVERED_TOTAL.inc()
                    recovered_count += 1
            else:
                # Support direct DiscordSender in unit tests while still honoring multi-destination if present
                dispatcher = ResilientDispatcher(store=store, max_retries=sender.max_retries)
                ok = await dispatcher.dispatch_job(
                    delivery_id=leased.delivery_id,
                    event_type=leased.event_type,
                    payload=payload,
                    destinations=destinations,
                    client=client,
                    worker_id=wid,
                    lease_generation=leased.lease_generation,
                    trigger_type="reconciliation"
                )
                if ok:
                    RECONCILIATION_RECOVERED_TOTAL.inc()
                    recovered_count += 1

        if len(stale_candidates) < effective_batch_size or leased_in_batch == 0:
            break

    # HR-09: Enforce payload, attempt, and audit log retention pruning
    try:
        await store.enforce_retention_policy()
    except Exception as exc:
        logger.warning(f"Retention policy cleanup encountered error: {exc}")

    return recovered_count


async def reconciliation_worker_loop(
    store: DeliveryStore,
    sender: Union[ResilientDispatcher, DiscordSender],
    discord_webhook_url: str,
    client: httpx.AsyncClient,
    interval_seconds: int = 60,
    stale_threshold_seconds: int = 120,
    stop_event: Optional[asyncio.Event] = None
):
    """
    Continuous background loop running reconciliation cycles periodically.
    """
    logger.info("Starting background reconciliation worker loop...")
    while True:
        try:
            if stop_event and stop_event.is_set():
                break
            await run_reconciliation_cycle(
                store=store,
                sender=sender,
                discord_webhook_url=discord_webhook_url,
                client=client,
                stale_threshold_seconds=stale_threshold_seconds
            )
        except asyncio.CancelledError:
            logger.info("Reconciliation worker cancelled.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in reconciliation cycle: {e}", exc_info=True)

        try:
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            break
