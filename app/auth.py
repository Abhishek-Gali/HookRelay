import hashlib
import hmac
from enum import Enum
from typing import Optional
from fastapi import Header, HTTPException, Request, status, Depends
from app.config import settings
from app.ratelimit import auth_failure_limiter


class Role(str, Enum):
    ADMIN = "ADMIN"
    OPERATOR = "OPERATOR"
    VIEWER = "VIEWER"


def hash_key(key: str) -> str:
    """Returns SHA-256 hash of API key for constant-time comparison."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


async def _audit_security_event(
    request: Optional[Request],
    actor_role: str,
    action: str,
    status_str: str,
    details: str
) -> None:
    """Safely records authentication/authorization events to the store without logging raw keys."""
    if request is None:
        return
    try:
        import app.main as main_module
        ip_addr = request.client.host if request.client else "unknown"
        ua = request.headers.get("User-Agent", "")
        await main_module.store.record_audit_log(
            actor_role=actor_role,
            action=action,
            target_id=str(request.url.path),
            ip_address=ip_addr,
            user_agent=ua,
            status=status_str,
            details=details
        )
    except Exception:
        pass


async def get_current_user_role(
    request: Request = None,  # type: ignore[assignment]
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
) -> Role:
    """
    Validates X-API-Key header against configured role keys using constant-time comparison.
    Enforces brute-force lockout per IP and logs failed attempts to the security audit table.
    """
    client_ip = request.client.host if (request and request.client) else "unknown"

    if request is not None:
        if auth_failure_limiter.count_recent(client_ip) >= settings.auth_failure_limit_per_minute:
            await _audit_security_event(
                request,
                actor_role="UNAUTHENTICATED",
                action="auth_lockout",
                status_str="blocked",
                details="Too many failed authentication attempts from IP"
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many failed authentication attempts. Try again later."
            )

    if not x_api_key:
        if request is not None:
            auth_failure_limiter.is_allowed(client_ip, settings.auth_failure_limit_per_minute)
            await _audit_security_event(
                request,
                actor_role="UNAUTHENTICATED",
                action="auth_failed",
                status_str="denied",
                details="Missing X-API-Key header"
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing required X-API-Key header.",
            headers={"WWW-Authenticate": "ApiKey"}
        )

    provided_hash = hash_key(x_api_key)

    # Constant-time comparisons against configured keys
    if settings.admin_api_key and hmac.compare_digest(provided_hash, hash_key(settings.admin_api_key)):
        return Role.ADMIN
    if settings.operator_api_key and hmac.compare_digest(provided_hash, hash_key(settings.operator_api_key)):
        return Role.OPERATOR
    if settings.viewer_api_key and hmac.compare_digest(provided_hash, hash_key(settings.viewer_api_key)):
        return Role.VIEWER

    if request is not None:
        auth_failure_limiter.is_allowed(client_ip, settings.auth_failure_limit_per_minute)
        await _audit_security_event(
            request,
            actor_role="UNAUTHENTICATED",
            action="auth_failed",
            status_str="denied",
            details="Invalid X-API-Key provided"
        )

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid X-API-Key.",
        headers={"WWW-Authenticate": "ApiKey"}
    )


def require_role(allowed_roles: list[Role]):
    """
    Dependency factory enforcing Role-Based Access Control (RBAC) and logging 403 denials.
    """
    async def _role_checker(
        request: Request,
        role: Role = Depends(get_current_user_role)
    ) -> Role:
        if role not in allowed_roles:
            await _audit_security_event(
                request,
                actor_role=role.value,
                action="authz_denied",
                status_str="forbidden",
                details=f"Required roles: {[r.value for r in allowed_roles]}"
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Forbidden: Action requires one of {[r.value for r in allowed_roles]}. Your role: {role.value}."
            )
        return role

    return _role_checker
