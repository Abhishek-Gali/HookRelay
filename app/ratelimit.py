from collections import OrderedDict
import hashlib
import time
from typing import Any, Optional
import uuid
from fastapi import Request, HTTPException, status
from app.config import settings

# Atomic Redis Lua script for distributed sliding-window rate limiting (HR-07)
_REDIS_SLIDING_WINDOW_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]
local cutoff = now - window

redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)
local current = redis.call('ZCARD', key)
if current >= limit then
    return 0
end
redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, math.ceil(window) + 1)
return 1
"""


class SlidingWindowRateLimiter:
    """
    Hybrid distributed / bounded-memory sliding-window rate limiter (HR-07):
    - When settings.redis_url is configured, uses an atomic Redis Lua script across replicas.
    - Otherwise uses a bounded-memory OrderedDict with automatic empty-bucket deletion
      and LRU eviction (max_buckets) so spoofed-IP floods never cause memory leaks.
    """
    def __init__(
        self,
        limit_per_minute: int = 300,
        window_seconds: int = 60,
        max_buckets: int = 10000,
        namespace: str = "rl"
    ):
        self.limit_per_minute = limit_per_minute
        self.window_seconds = window_seconds
        self.max_buckets = max_buckets
        self.namespace = namespace
        self.requests: OrderedDict[str, list[float]] = OrderedDict()
        self._last_sweep: float = time.time()
        self._redis_client: Optional[Any] = None
        self._redis_url_bound: str = ""

    def reset(self) -> None:
        self.requests.clear()

    async def _get_redis(self) -> Optional[Any]:
        if not settings.redis_url:
            return None
        if self._redis_client is not None and self._redis_url_bound == settings.redis_url:
            return self._redis_client
        try:
            import redis.asyncio as aioredis  # type: ignore[import-not-found]
            self._redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)
            self._redis_url_bound = settings.redis_url
            return self._redis_client
        except Exception:
            return None

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

        if (now - self._last_sweep > self.window_seconds) or (len(self.requests) >= self.max_buckets):
            self._evict_stale_buckets(now)

        filtered = self._prune_key(bucket_key, now)

        if len(filtered) >= limit:
            return False

        if bucket_key not in self.requests and len(self.requests) >= self.max_buckets:
            self.requests.popitem(last=False)

        filtered.append(now)
        self.requests[bucket_key] = filtered
        self.requests.move_to_end(bucket_key)
        return True

    async def is_allowed_async(self, bucket_key: str, override_limit: int | None = None) -> bool:
        """
        Checks rate limit using Redis atomic Lua sliding window when REDIS_URL is configured,
        falling back to the bounded in-memory sliding window otherwise.
        """
        limit = override_limit if override_limit is not None else self.limit_per_minute
        redis_conn = await self._get_redis()
        if redis_conn is not None:
            try:
                now = time.time()
                redis_key = f"hookrelay:{self.namespace}:{bucket_key}"
                member = f"{now}:{uuid.uuid4().hex[:8]}"
                allowed = await redis_conn.eval(
                    _REDIS_SLIDING_WINDOW_LUA,
                    1,
                    redis_key,
                    str(now),
                    str(self.window_seconds),
                    str(limit),
                    member
                )
                return int(allowed) == 1
            except Exception:
                pass
        return self.is_allowed(bucket_key, override_limit=limit)

    def count_recent(self, bucket_key: str) -> int:
        now = time.time()
        filtered = self._prune_key(bucket_key, now)
        return len(filtered)


webhook_rate_limiter = SlidingWindowRateLimiter(settings.rate_limit_requests_per_minute, namespace="webhook")
api_rate_limiter = SlidingWindowRateLimiter(settings.api_rate_limit_per_minute, namespace="api")
redrive_rate_limiter = SlidingWindowRateLimiter(settings.redrive_rate_limit_per_minute, namespace="redrive")
auth_failure_limiter = SlidingWindowRateLimiter(settings.auth_failure_limit_per_minute, namespace="auth_fail")

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
    if not await webhook_rate_limiter.is_allowed_async(client_ip, settings.rate_limit_requests_per_minute):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Webhook rate limit exceeded. Please reduce request frequency."
        )


async def check_api_rate_limit(request: Request) -> None:
    """FastAPI dependency to rate limit management /api/* endpoints."""
    bucket = _client_key(request, include_api_key=True)
    if not await api_rate_limiter.is_allowed_async(bucket, settings.api_rate_limit_per_minute):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Management API rate limit exceeded."
        )


async def check_redrive_rate_limit(request: Request) -> None:
    """FastAPI dependency to rate limit redrive/replay operations (10/min)."""
    bucket = _client_key(request, include_api_key=True)
    if not await redrive_rate_limiter.is_allowed_async(bucket, settings.redrive_rate_limit_per_minute):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Redrive rate limit exceeded (max 10/min)."
        )
