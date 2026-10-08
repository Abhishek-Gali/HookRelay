import asyncio
import logging
import random
from typing import Any, Dict, Optional
import httpx

from app.config import settings
from app.providers import extract_retry_after, read_bounded_error

logger = logging.getLogger("hookrelay.sender")


class RetryableError(Exception):
    """Signals an error that may succeed upon retrying (429, 5xx, network timeout)."""
    def __init__(self, message: str, retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after


class NonRetryableError(Exception):
    """Signals a fatal error that will not succeed on retry (e.g. 400 Bad Request, 401, 404)."""
    pass


class RetryPolicy:
    """
    Unified retry policy for all downstream webhook deliveries.
    - Respects server Retry-After up to max_retry_after_seconds (default 60s).
    - Computes exponential backoff with jitter for 5xx and transient network errors.
    """
    def __init__(
        self,
        max_attempts: int = 5,
        initial_backoff: float = 0.5,
        max_backoff: float = 15.0,
        jitter: float = 0.25,
        max_retry_after_seconds: Optional[float] = None,
    ):
        self.max_attempts = max_attempts
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.jitter = jitter
        self.max_retry_after_seconds = (
            max_retry_after_seconds
            if max_retry_after_seconds is not None
            else settings.max_retry_after_seconds
        )

    def compute_sleep_seconds(self, attempt_num: int, retry_after: Optional[float] = None) -> float:
        """
        Determines exact sleep duration before the next attempt:
        - If Retry-After was provided by server (429), honors it up to max_retry_after_seconds.
        - Otherwise uses exponential backoff with random jitter.
        """
        if retry_after is not None and retry_after >= 0:
            return min(float(retry_after), self.max_retry_after_seconds)

        exp = self.initial_backoff * (2 ** max(0, attempt_num - 1))
        jitter_offset = random.uniform(0, self.jitter)  # nosec B311 - non-cryptographic backoff jitter
        return min(exp + jitter_offset, self.max_backoff)


class DiscordSender:
    """
    Direct sender adapter using the unified RetryPolicy.
    """
    def __init__(
        self,
        client: Optional[httpx.AsyncClient] = None,
        max_retries: int = 5,
        timeout: float = 10.0,
        max_retry_after_seconds: Optional[float] = None,
    ):
        self._external_client = client
        self.max_retries = max_retries
        self.timeout = timeout
        self.policy = RetryPolicy(
            max_attempts=max_retries,
            max_retry_after_seconds=max_retry_after_seconds
        )

    async def send_to_discord(
        self,
        webhook_url: str,
        discord_payload: Dict[str, Any],
        client: Optional[httpx.AsyncClient] = None
    ) -> int:
        active_client = client or self._external_client
        should_close_client = False
        if not active_client:
            active_client = httpx.AsyncClient(timeout=self.timeout)
            should_close_client = True

        last_err: Optional[Exception] = None

        try:
            for attempt_num in range(1, self.policy.max_attempts + 1):
                try:
                    response = await active_client.post(
                        webhook_url,
                        json=discord_payload,
                        headers={"Content-Type": "application/json"}
                    )
                except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout) as net_err:
                    logger.warning(
                        f"[Attempt {attempt_num}/{self.policy.max_attempts}] Network error: {net_err}"
                    )
                    last_err = RetryableError(f"Network error: {str(net_err)}")
                    if attempt_num < self.policy.max_attempts:
                        await asyncio.sleep(self.policy.compute_sleep_seconds(attempt_num))
                        continue
                    raise last_err

                if response.status_code == 429:
                    retry_after = extract_retry_after(response)
                    sleep_s = self.policy.compute_sleep_seconds(attempt_num, retry_after=retry_after)
                    logger.warning(
                        f"[Attempt {attempt_num}/{self.policy.max_attempts}] Rate limited (429). Sleeping {sleep_s:.2f}s."
                    )
                    last_err = RetryableError(f"Discord rate limited (429), waited {sleep_s}s", retry_after=sleep_s)
                    if attempt_num < self.policy.max_attempts:
                        await asyncio.sleep(sleep_s)
                        continue
                    raise last_err

                if response.status_code >= 500:
                    bounded_body = read_bounded_error(response) or ""
                    logger.warning(
                        f"[Attempt {attempt_num}/{self.policy.max_attempts}] Server error ({response.status_code})."
                    )
                    last_err = RetryableError(f"Discord 5xx server error ({response.status_code}): {bounded_body}")
                    if attempt_num < self.policy.max_attempts:
                        await asyncio.sleep(self.policy.compute_sleep_seconds(attempt_num))
                        continue
                    raise last_err

                if 400 <= response.status_code < 500:
                    bounded_body = read_bounded_error(response) or ""
                    err_msg = f"Non-retryable Discord error ({response.status_code}): {bounded_body}"
                    logger.error(err_msg)
                    raise NonRetryableError(err_msg)

                if 200 <= response.status_code < 300:
                    return attempt_num

                response.raise_for_status()
                return attempt_num

            if last_err:
                raise last_err
            return self.policy.max_attempts
        finally:
            if should_close_client:
                await active_client.aclose()
