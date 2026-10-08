import asyncio
import logging
import time
from typing import Dict, Any, List, Optional
import httpx

from app.config import settings
from app.providers import get_provider, sanitize_error_message
from app.routing import RouteDestination
from app.sender import RetryPolicy
from app.store import DeliveryStore
from app.metrics import (
    PROVIDER_DISPATCHES_TOTAL,
    PROVIDER_RETRIES_TOTAL,
    DISCORD_DISPATCHES_TOTAL,
    DISCORD_RETRIES_TOTAL,
)

logger = logging.getLogger("hookrelay.dispatcher")


class ResilientDispatcher:
    """
    Handles multi-destination resilient dispatch of webhook payloads
    (Discord, Slack, HTTP), using the unified RetryPolicy, renewing worker leases
    during long deliveries/retries, recording granular attempts with trigger source,
    and updating DLQ state on terminal failure.
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

    async def _lease_heartbeat_loop(
        self,
        delivery_id: str,
        worker_id: str,
        stop_event: asyncio.Event
    ) -> None:
        """Periodically extends the worker lease while a delivery is actively in flight."""
        interval = max(1.0, settings.worker_lease_seconds / 3.0)
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                try:
                    await self.store.renew_lease(
                        delivery_id=delivery_id,
                        worker_id=worker_id,
                        lease_seconds=settings.worker_lease_seconds
                    )
                except Exception:
                    pass

    async def dispatch_job(
        self,
        delivery_id: str,
        event_type: str,
        payload: Dict[str, Any],
        destinations: List[RouteDestination],
        client: Optional[httpx.AsyncClient] = None,
        worker_id: Optional[str] = None,
        trigger_type: str = "initial"
    ) -> bool:
        """
        Iterates over destinations, executing exponential backoff retries with jitter
        via RetryPolicy, renewing the worker lease on long sleeps, and logging every
        attempt with its trigger_type ('initial', 'retry', 'reconciliation', 'redrive').
        """
        active_client = client
        should_close = False
        if active_client is None:
            active_client = httpx.AsyncClient(timeout=self.timeout, follow_redirects=False)
            should_close = True

        heartbeat_stop = asyncio.Event()
        heartbeat_task: Optional[asyncio.Task] = None
        if worker_id:
            heartbeat_task = asyncio.create_task(
                self._lease_heartbeat_loop(delivery_id, worker_id, heartbeat_stop)
            )

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
                        error_msg = sanitize_error_message(f"Network error: {type(net_err).__name__}: {str(net_err)}")
                        status_code = None
                    except Exception as exc:
                        error_msg = sanitize_error_message(f"Dispatch error: {type(exc).__name__}: {str(exc)}")
                        status_code = None

                    duration_ms = (time.perf_counter() - t0) * 1000
                    attempt_trigger = "retry" if (attempt_num > 1 and trigger_type == "initial") else trigger_type

                    await self.store.record_attempt(
                        delivery_id=delivery_id,
                        destination=dest.provider,
                        attempt_number=attempt_num,
                        http_status=status_code,
                        response_time_ms=round(duration_ms, 2),
                        error_message=error_msg,
                        response_headers=headers_summary,
                        trigger_type=attempt_trigger
                    )

                    if status_code and 200 <= status_code < 300:
                        logger.info(
                            f"Delivered {delivery_id} to {dest.provider} on attempt {attempt_num} ({attempt_trigger})."
                        )
                        success = True
                        PROVIDER_DISPATCHES_TOTAL.labels(
                            provider=dest.provider, event=event_type, status="sent"
                        ).inc()
                        DISCORD_DISPATCHES_TOTAL.labels(event=event_type, status="sent").inc()
                        break

                    if error_msg:
                        last_error_summary = error_msg

                    # Fatal non-retryable 4xx client errors (excluding 429) or 3xx redirects
                    if status_code and 300 <= status_code < 500 and status_code != 429:
                        logger.error(
                            f"Fatal non-retryable status ({status_code}) for {delivery_id} to {dest.provider}."
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
                        # Extend worker lease before sleeping so long Retry-After waits never expire the lease
                        if worker_id:
                            await self.store.renew_lease(
                                delivery_id=delivery_id,
                                worker_id=worker_id,
                                lease_seconds=max(settings.worker_lease_seconds, int(sleep_s) + 60)
                            )
                        logger.warning(
                            f"Transient failure on {delivery_id} (attempt {attempt_num}/{self.policy.max_attempts}). "
                            f"Sleeping {sleep_s:.2f}s before retry..."
                        )
                        PROVIDER_RETRIES_TOTAL.labels(provider=dest.provider).inc()
                        DISCORD_RETRIES_TOTAL.inc()
                        await asyncio.sleep(sleep_s)

                if not success:
                    all_succeeded = False
                    PROVIDER_DISPATCHES_TOTAL.labels(
                        provider=dest.provider, event=event_type, status="failed"
                    ).inc()
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
            heartbeat_stop.set()
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except (asyncio.CancelledError, Exception):
                    pass
            if should_close and active_client:
                await active_client.aclose()
