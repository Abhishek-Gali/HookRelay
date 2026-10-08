import asyncio
import logging
import time
from typing import Dict, Any, List, Optional
import httpx

from app.config import settings
from app.providers import get_provider
from app.routing import RouteDestination
from app.sender import RetryPolicy
from app.store import DeliveryStore
from app.metrics import (
    DISCORD_DISPATCHES_TOTAL,
    DISCORD_RETRIES_TOTAL
)

logger = logging.getLogger("hookrelay.dispatcher")


class ResilientDispatcher:
    """
    Handles multi-destination resilient dispatch of webhook payloads
    (Discord, Slack, HTTP), using the unified RetryPolicy, recording granular
    attempts, and updating DLQ state on terminal failure.
    """
    def __init__(
        self,
        store: DeliveryStore,
        max_retries: int = 5,
        timeout: float = 10.0,
        policy: Optional[RetryPolicy] = None
    ):
        self.store = store
        self.max_retries = max_retries
        self.timeout = timeout
        self.policy = policy or RetryPolicy(max_attempts=max_retries)

    async def dispatch_job(
        self,
        delivery_id: str,
        event_type: str,
        payload: Dict[str, Any],
        destinations: List[RouteDestination],
        client: Optional[httpx.AsyncClient] = None
    ) -> bool:
        """
        Iterates over destinations, executing exponential backoff retries with jitter
        via RetryPolicy and logging every attempt in the database.
        """
        active_client = client
        should_close = False
        if active_client is None:
            active_client = httpx.AsyncClient(timeout=self.timeout)
            should_close = True

        all_succeeded = True
        overall_attempts = 0
        last_error_summary = "Exhausted retries for destinations"

        try:
            for dest in destinations:
                provider = get_provider(dest.provider)
                dest_url = dest.url
                success = False

                for attempt_num in range(1, self.policy.max_attempts + 1):
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
                            client=active_client,
                            use_embeds=settings.enable_embeds,
                            delivery_id=delivery_id
                        )
                    except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout) as net_err:
                        error_msg = f"Network error: {str(net_err)}"
                        status_code = None
                    except Exception as exc:
                        error_msg = f"Unexpected exception: {str(exc)}"
                        status_code = None

                    duration_ms = (time.perf_counter() - t0) * 1000

                    await self.store.record_attempt(
                        delivery_id=delivery_id,
                        destination=dest.provider,
                        attempt_number=attempt_num,
                        http_status=status_code,
                        response_time_ms=round(duration_ms, 2),
                        error_message=error_msg,
                        response_headers=headers_summary
                    )

                    if status_code and 200 <= status_code < 300:
                        logger.info(
                            f"Delivered {delivery_id} to {dest.provider} on attempt {attempt_num}."
                        )
                        success = True
                        DISCORD_DISPATCHES_TOTAL.labels(event=event_type, status="sent").inc()
                        break

                    if error_msg:
                        last_error_summary = error_msg

                    # Fatal non-retryable 4xx client errors (excluding 429)
                    if status_code and 400 <= status_code < 500 and status_code != 429:
                        logger.error(
                            f"Fatal non-retryable client error ({status_code}) for {delivery_id} to {dest.provider}."
                        )
                        break

                    # Parse Retry-After if 429
                    retry_after_val: Optional[float] = None
                    if status_code == 429 and headers_summary and "Retry-After=" in headers_summary:
                        try:
                            retry_after_val = float(headers_summary.split("Retry-After=")[1])
                        except (ValueError, TypeError):
                            retry_after_val = None

                    if attempt_num < self.policy.max_attempts:
                        sleep_s = self.policy.compute_sleep_seconds(attempt_num, retry_after=retry_after_val)
                        logger.warning(
                            f"Transient failure on {delivery_id} (attempt {attempt_num}/{self.policy.max_attempts}). "
                            f"Sleeping {sleep_s:.2f}s before retry..."
                        )
                        DISCORD_RETRIES_TOTAL.inc()
                        await asyncio.sleep(sleep_s)

                if not success:
                    all_succeeded = False
                    logger.error(f"Delivery {delivery_id} failed permanently for {dest.provider}.")

            if all_succeeded:
                await self.store.mark_sent(delivery_id, attempts=overall_attempts)
                return True
            else:
                await self.store.mark_failed_or_dlq(
                    delivery_id,
                    attempts=overall_attempts,
                    error=last_error_summary
                )
                DISCORD_DISPATCHES_TOTAL.labels(event=event_type, status="failed").inc()
                return False
        finally:
            if should_close and active_client:
                await active_client.aclose()
