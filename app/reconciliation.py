import asyncio
import json
import logging
from typing import Optional
import httpx

from app.config import settings
from app.formatter import format_payload
from app.sender import DiscordSender
from app.store import DeliveryStore
from app.metrics import RECONCILIATION_RUNS_TOTAL, RECONCILIATION_RECOVERED_TOTAL

logger = logging.getLogger("hookrelay.reconciliation")


async def run_reconciliation_cycle(
    store: DeliveryStore,
    sender: DiscordSender,
    discord_webhook_url: str,
    client: httpx.AsyncClient,
    stale_threshold_seconds: int = 120
) -> int:
    """
    Executes a single reconciliation sweep.
    Detects deliveries that remain in status 'received' beyond the threshold,
    indicating the worker crashed or restart occurred before delivery was finalized.
    Re-drives each delivery safely.
    """
    RECONCILIATION_RUNS_TOTAL.inc()
    stale_deliveries = await store.fetch_stale_deliveries(older_than_seconds=stale_threshold_seconds)
    if not stale_deliveries:
        return 0

    logger.warning(f"Reconciliation engine detected {len(stale_deliveries)} stranded 'received' deliveries.")
    recovered_count = 0

    for item in stale_deliveries:
        logger.info(f"Re-driving stranded delivery: {item.delivery_id} (event: {item.event_type})")
        payload = {}
        if item.payload:
            try:
                payload = json.loads(item.payload)
            except Exception:
                payload = {}

        formatted_msg = format_payload(item.event_type, payload, use_embeds=settings.enable_embeds)

        try:
            attempts = await sender.send_to_discord(discord_webhook_url, formatted_msg, client=client)
            await store.mark_sent(item.delivery_id, attempts=item.attempts + attempts)
            RECONCILIATION_RECOVERED_TOTAL.inc()
            recovered_count += 1
            logger.info(f"Successfully recovered stranded delivery: {item.delivery_id}")
        except Exception as exc:
            err_msg = f"Recovery attempt failed: {str(exc)}"
            logger.error(f"Failed re-driving {item.delivery_id}: {err_msg}")
            await store.mark_failed(item.delivery_id, attempts=item.attempts + 1, error=err_msg)

    return recovered_count


async def reconciliation_worker_loop(
    store: DeliveryStore,
    sender: DiscordSender,
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
