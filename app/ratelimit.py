import time
from collections import defaultdict
from fastapi import Request, HTTPException, status
from app.config import settings


class SlidingWindowRateLimiter:
    """
    In-memory sliding-window rate limiter per client IP.
    Protects the webhook gateway against denial-of-service floods.
    Allows normal bursts while capping requests per window.
    """
    def __init__(self, limit_per_minute: int = 300):
        self.limit_per_minute = limit_per_minute
        self.window_seconds = 60
        self.requests = defaultdict(list)

    def is_allowed(self, client_ip: str) -> bool:
        now = time.time()
        cutoff = now - self.window_seconds

        # Clean timestamps older than 60 seconds
        timestamps = self.requests[client_ip]
        self.requests[client_ip] = [t for t in timestamps if t > cutoff]

        if len(self.requests[client_ip]) >= self.limit_per_minute:
            return False

        self.requests[client_ip].append(now)
        return True


rate_limiter = SlidingWindowRateLimiter(settings.rate_limit_requests_per_minute)


async def check_rate_limit(request: Request):
    """FastAPI dependency to rate limit webhook endpoints."""
    client_ip = request.client.host if request.client else "unknown"
    if not rate_limiter.is_allowed(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Please reduce request frequency."
        )
