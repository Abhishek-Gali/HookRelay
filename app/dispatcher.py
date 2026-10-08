import asyncio
import json
import logging
import time
from typing import Dict, Any, List, Optional
import httpx

from app.config import settings
from app.providers import get_provider
from app.routing import RouteDestination
from app.store import DeliveryStore
from app.metrics import (
    DISCORD_DISPATCHES_TOTAL,
    DISCORD_RETRIES_TOTAL
)

logger = logging.getLogger("hookrelay.dispatcher")


class ResilientDispatcher:
    """
    Handles robust, retrying dispatch of webhook payloads to target destinations
    (Discord, Slack, HTTP), recording granular attempts and updating DLQ state on failure.
    """
    def __init__(self, store: DeliveryStore, max_retries: int = 5, timeout: float = 10.0):
        self.store = store
        self.max_retries = max_retries
        self.timeout = timeout

    async def dispatch_job(
        self,
        delivery_id: str,
        event_type: str,
        payload: Dict[str, Any],
        destinations: List[RouteDestination],
        client: httpx.AsyncClient
    ) -> bool:
        """
        Iterates over destinations, executing exponential backoff retries with jitter
        and logging every attempt in the database.
        """
        all_succeeded = True
        overall_attempts = 0

        for dest in destinations:
            provider = get_provider(dest.provider)
            dest_url = dest.url
            success = False

            for attempt_num in range(1, self.max_retries + 1):
                overall_attempts += 1
                t0 = time.perf_counter()
                status_code: Optional[int] = None
                error_msg: Optional[str] = None
                headers_summary: Optional[str] = None

                try:
                    status_code, error_msg, headers_summary = await provider.send(
                        event_type=event_type,
                        payload=payload,
                        destination_url=dest_url,
                        client=client,
                        use_embeds=settings.enable_embeds
                    )
                except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout) as net_err:
                    error_msg = f"Network error: {str(net_err)}"
                    status_code = None
                except Exception as exc:
                    error_msg = f"Unexpected exception: {str(exc)}"
                    status_code = None

                duration_ms = (time.perf_counter() - t0) * 1000

                # Record granular attempt in database
                await self.store.record_attempt(
                    delivery_id=delivery_id,
                    destination=dest.provider,
                    attempt_number=attempt_num,
                    http_status=status_code,
                    response_time_ms=round(duration_ms, 2),
                    error_message=error_msg,
                    response_headers=headers_summary
                )

                # Check outcome
                if status_code and 200 <= status_code < 300:
                    logger.info(f"Delivered {delivery_id} to {dest.provider} on attempt {attempt_num}.")
                    success = True
                    DISCORD_DISPATCHES_TOTAL.labels(event=event_type, status="sent").inc()
                    break

                # Fatal non-retryable 4xx client errors (e.g. 400 Bad Request, 401 Unauthorized, 404)
                if status_code and 400 <= status_code < 500 and status_code != 429:
                    logger.error(f"Fatal non-retryable client error ({status_code}) for {delivery_id} to {dest.provider}: {error_msg}")
                    break

                # Handle Rate Limit (429)
                if status_code == 429:
                    sleep_duration = 1.0
                    if headers_summary and "Retry-After=" in headers_summary:
                        try:
                            sleep_duration = float(headers_summary.split("Retry-After=")[1])
                        except ValueError:
                            pass
                    logger.warning(f"Rate limited (429) on {delivery_id}. Sleeping {sleep_duration}s...")
                    DISCORD_RETRIES_TOTAL.inc()
                    await asyncio.sleep(min(sleep_duration, 5.0))
                    continue

                # 5xx or network timeout -> exponential backoff with jitter
                if attempt_num < self.max_retries:
                    backoff = min(1.0 * (2 ** (attempt_num - 1)), 10.0)
                    logger.warning(f"Transient error on {delivery_id} (attempt {attempt_num}). Backing off {backoff}s...")
                    DISCORD_RETRIES_TOTAL.inc()
                    await asyncio.sleep(backoff)

            if not success:
                all_succeeded = False
                logger.error(f"Delivery {delivery_id} failed permanently for {dest.provider}.")

        if all_succeeded:
            await self.store.mark_sent(delivery_id, attempts=overall_attempts)
            return True
        else:
            await self.store.mark_failed_or_dlq(delivery_id, attempts=overall_attempts, error=f"Exhausted retries for destinations")
            DISCORD_DISPATCHES_TOTAL.labels(event=event_type, status="failed").inc()
            return False
