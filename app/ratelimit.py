from collections import OrderedDict
import hashlib
import time
from fastapi import Request, HTTPException, status
from app.config import settings


class SlidingWindowRateLimiter:
    """
    Bounded-memory sliding-window rate limiter supporting distinct buckets
    (webhook IP, API key hash, redrive actions, auth failures).
    Automatically deletes empty buckets and enforces an upper bound (max_buckets)
    with LRU eviction so spoofed-IP floods cannot cause unbounded memory growth.
    """
    def __init__(
        self,
        limit_per_minute: int = 300,
        window_seconds: int = 60,
        max_buckets: int = 10000
    ):
        self.limit_per_minute = limit_per_minute
        self.window_seconds = window_seconds
        self.max_buckets = max_buckets
        self.requests: OrderedDict[str, list[float]] = OrderedDict()
        self._last_sweep: float = time.time()

    def reset(self) -> None:
        self.requests.clear()

    def _evict_stale_buckets(self, now: float) -> None:
        cutoff = now - self.window_seconds
        stale_keys = [
            k for k, ts_list in self.requests.items()
            if not ts_list or ts_list[-1] <= cutoff
        ]
        for k in stale_keys:
            self.requests.pop(k, None)
        self._last_sweep = now

    def _prune_key(self, bucket_key: str, now: float) -> list[float]:
        cutoff = now - self.window_seconds
        timestamps = self.requests.get(bucket_key)
        if not timestamps:
            self.requests.pop(bucket_key, None)
            return []
        filtered = [t for t in timestamps if t > cutoff]
        if filtered:
            self.requests[bucket_key] = filtered
            self.requests.move_to_end(bucket_key)
        else:
            self.requests.pop(bucket_key, None)
        return filtered

    def is_allowed(self, bucket_key: str, override_limit: int | None = None) -> bool:
        limit = override_limit if override_limit is not None else self.limit_per_minute
        now = time.time()

        # Periodic sweep or capacity-triggered sweep
        if (now - self._last_sweep > self.window_seconds) or (len(self.requests) >= self.max_buckets):
            self._evict_stale_buckets(now)

        filtered = self._prune_key(bucket_key, now)

        if len(filtered) >= limit:
            return False

        # Enforce hard cap on bucket count (LRU eviction of oldest bucket)
        if bucket_key not in self.requests and len(self.requests) >= self.max_buckets:
            self.requests.popitem(last=False)

        filtered.append(now)
        self.requests[bucket_key] = filtered
        self.requests.move_to_end(bucket_key)
        return True

    def count_recent(self, bucket_key: str) -> int:
        now = time.time()
        filtered = self._prune_key(bucket_key, now)
        return len(filtered)


webhook_rate_limiter = SlidingWindowRateLimiter(settings.rate_limit_requests_per_minute)
api_rate_limiter = SlidingWindowRateLimiter(settings.api_rate_limit_per_minute)
redrive_rate_limiter = SlidingWindowRateLimiter(settings.redrive_rate_limit_per_minute)
auth_failure_limiter = SlidingWindowRateLimiter(settings.auth_failure_limit_per_minute)

# Backwards compatibility alias
rate_limiter = webhook_rate_limiter


def _client_key(request: Request, include_api_key: bool = True) -> str:
    client_ip = request.client.host if request.client else "unknown"
    if include_api_key:
        raw_key = request.headers.get("X-API-Key", "")
        if raw_key:
            key_fp = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:12]
            return f"{client_ip}:{key_fp}"
        session_cookie = request.cookies.get("hr_session", "")
        if session_cookie:
            sess_fp = hashlib.sha256(session_cookie.encode("utf-8")).hexdigest()[:12]
            return f"{client_ip}:sess:{sess_fp}"
    return client_ip


async def check_rate_limit(request: Request) -> None:
    """FastAPI dependency to rate limit webhook ingestion endpoints."""
    client_ip = _client_key(request, include_api_key=False)
    if not webhook_rate_limiter.is_allowed(client_ip, settings.rate_limit_requests_per_minute):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Webhook rate limit exceeded. Please reduce request frequency."
        )


async def check_api_rate_limit(request: Request) -> None:
    """FastAPI dependency to rate limit management /api/* endpoints."""
    bucket = _client_key(request, include_api_key=True)
    if not api_rate_limiter.is_allowed(bucket, settings.api_rate_limit_per_minute):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Management API rate limit exceeded."
        )


async def check_redrive_rate_limit(request: Request) -> None:
    """FastAPI dependency to rate limit redrive/replay operations (10/min)."""
    bucket = _client_key(request, include_api_key=True)
    if not redrive_rate_limiter.is_allowed(bucket, settings.redrive_rate_limit_per_minute):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Redrive rate limit exceeded (max 10/min)."
        )
