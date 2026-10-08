import asyncio
import logging
from typing import Any, Dict, Optional
import httpx
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type
)

logger = logging.getLogger("hookrelay.sender")


class RetryableError(Exception):
    """Signals an error that may succeed upon retrying (429, 5xx, network timeout)."""
    def __init__(self, message: str, retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after


class NonRetryableError(Exception):
    """Signals a fatal error that will not succeed on retry (e.g. 400 Bad Request, 401 Unauthorized, 404)."""
    pass


class DiscordSender:
    def __init__(self, client: Optional[httpx.AsyncClient] = None, max_retries: int = 5, timeout: float = 10.0):
        self._external_client = client
        self.max_retries = max_retries
        self.timeout = timeout

    async def _get_client(self) -> httpx.AsyncClient:
        if self._external_client:
            return self._external_client
        return httpx.AsyncClient(timeout=self.timeout)

    async def send_to_discord(
        self,
        webhook_url: str,
        discord_payload: Dict[str, Any],
        client: Optional[httpx.AsyncClient] = None
    ) -> int:
        """
        Sends payload to Discord incoming webhook with resilient retries.
        - Respects Discord 429 rate limit via Retry-After header.
        - Retries on 5xx server errors and network timeouts with exponential backoff & jitter.
        - Aborts immediately on non-retryable 4xx client errors.
        - Returns the total number of attempts taken.
        """
        active_client = client or self._external_client
        should_close_client = False
        if not active_client:
            active_client = httpx.AsyncClient(timeout=self.timeout)
            should_close_client = True

        attempts_made = 0

        try:
            # We configure tenacity AsyncRetrying loop
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self.max_retries),
                wait=wait_exponential_jitter(initial=1, max=15, jitter=0.5),
                retry=retry_if_exception_type(RetryableError),
                reraise=True
            ):
                with attempt:
                    attempts_made = attempt.retry_state.attempt_number
                    try:
                        response = await active_client.post(
                            webhook_url,
                            json=discord_payload,
                            headers={"Content-Type": "application/json"}
                        )
                    except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout) as net_err:
                        logger.warning(
                            f"[Attempt {attempts_made}/{self.max_retries}] Network error communicating with Discord: {net_err}"
                        )
                        raise RetryableError(f"Network error: {str(net_err)}")

                    # Handle 429 Rate Limit
                    if response.status_code == 429:
                        retry_after = 1.0
                        # Check header
                        header_val = response.headers.get("Retry-After")
                        if header_val:
                            try:
                                retry_after = float(header_val)
                            except ValueError:
                                pass
                        else:
                            # Check body JSON if header absent
                            try:
                                data = response.json()
                                retry_after = float(data.get("retry_after", 1.0))
                            except Exception:
                                pass

                        logger.warning(
                            f"[Attempt {attempts_made}/{self.max_retries}] Discord rate limited (429). Waiting {retry_after}s before retry."
                        )
                        # Explicitly sleep for the rate-limit window requested by Discord
                        await asyncio.sleep(retry_after)
                        raise RetryableError(f"Discord rate limited (429), waited {retry_after}s", retry_after=retry_after)

                    # Handle 5xx Server Errors
                    if response.status_code >= 500:
                        logger.warning(
                            f"[Attempt {attempts_made}/{self.max_retries}] Discord server error ({response.status_code}): {response.text}"
                        )
                        raise RetryableError(f"Discord 5xx server error ({response.status_code})")

                    # Handle 4xx Client Errors (Non-retryable)
                    if 400 <= response.status_code < 500:
                        err_msg = f"Non-retryable Discord error ({response.status_code}): {response.text}"
                        logger.error(err_msg)
                        raise NonRetryableError(err_msg)

                    # 200 OK or 204 No Content indicates success
                    if 200 <= response.status_code < 300:
                        logger.info(f"Delivered to Discord successfully on attempt {attempts_made}.")
                        return attempts_made

                    # Any other unexpected code
                    response.raise_for_status()
                    return attempts_made

        finally:
            if should_close_client:
                await active_client.aclose()

        return attempts_made
