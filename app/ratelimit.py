import hashlib
import time
from collections import defaultdict
from fastapi import Request, HTTPException, status
from app.config import settings


class SlidingWindowRateLimiter:
    """
    Sliding-window rate limiter supporting distinct buckets (webhook IP, API key hash, redrive actions, auth failures).
    """
    def __init__(self, limit_per_minute: int = 300, window_seconds: int = 60):
        self.limit_per_minute = limit_per_minute
        self.window_seconds = window_seconds
        self.requests: dict[str, list[float]] = defaultdict(list)

    def reset(self) -> None:
        self.requests.clear()

    def is_allowed(self, bucket_key: str, override_limit: int | None = None) -> bool:
        limit = override_limit if override_limit is not None else self.limit_per_minute
        now = time.time()
        cutoff = now - self.window_seconds

        timestamps = self.requests[bucket_key]
        self.requests[bucket_key] = [t for t in timestamps if t > cutoff]

        if len(self.requests[bucket_key]) >= limit:
            return False

        self.requests[bucket_key].append(now)
        return True

    def count_recent(self, bucket_key: str) -> int:
        now = time.time()
        cutoff = now - self.window_seconds
        timestamps = self.requests[bucket_key]
        self.requests[bucket_key] = [t for t in timestamps if t > cutoff]
        return len(self.requests[bucket_key])


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
