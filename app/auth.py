from collections import OrderedDict
import hashlib
import hmac
import secrets
import time
from enum import Enum
from typing import Optional, Tuple
from fastapi import Header, HTTPException, Request, status, Depends
from app.config import settings
from app.ratelimit import auth_failure_limiter


class Role(str, Enum):
    ADMIN = "ADMIN"
    OPERATOR = "OPERATOR"
    VIEWER = "VIEWER"


def hash_key(key: str) -> str:
    """Returns SHA-256 hash of API key or session token for constant-time comparison."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def authenticate_raw_key(raw_key: Optional[str]) -> Optional[Role]:
    """Validates a raw API key in constant time and returns its Role, or None if invalid."""
    if not raw_key:
        return None
    provided_hash = hash_key(raw_key)
    if settings.admin_api_key and hmac.compare_digest(provided_hash, hash_key(settings.admin_api_key)):
        return Role.ADMIN
    if settings.operator_api_key and hmac.compare_digest(provided_hash, hash_key(settings.operator_api_key)):
        return Role.OPERATOR
    if settings.viewer_api_key and hmac.compare_digest(provided_hash, hash_key(settings.viewer_api_key)):
        return Role.VIEWER
    return None


class SessionManager:
    """
    Manages server-side HttpOnly browser sessions and per-session CSRF tokens
    for the operations console. Stores only SHA-256 hashes of session IDs in memory
    with strict TTL expiration and bounded capacity.
    """
    def __init__(self, ttl_seconds: int = 28800, max_sessions: int = 1000):
        self.ttl_seconds = ttl_seconds
        self.max_sessions = max_sessions
        # Maps sha256(session_id) -> (Role, csrf_token, expires_at_epoch)
        self._sessions: OrderedDict[str, Tuple[Role, str, float]] = OrderedDict()

    def _evict_expired(self, now: float) -> None:
        expired = [k for k, (_, _, exp) in self._sessions.items() if exp <= now]
        for k in expired:
            self._sessions.pop(k, None)

    def create_session(self, role: Role) -> Tuple[str, str]:
        now = time.time()
        self._evict_expired(now)
        if len(self._sessions) >= self.max_sessions:
            self._sessions.popitem(last=False)

        session_id = secrets.token_urlsafe(32)
        csrf_token = secrets.token_urlsafe(32)
        session_hash = hash_key(session_id)
        self._sessions[session_hash] = (role, csrf_token, now + self.ttl_seconds)
        return session_id, csrf_token

    def get_session(self, session_id: Optional[str]) -> Optional[Tuple[Role, str]]:
        if not session_id:
            return None
        now = time.time()
        session_hash = hash_key(session_id)
        entry = self._sessions.get(session_hash)
        if not entry:
            return None
        role, csrf_token, expires_at = entry
        if expires_at <= now:
            self._sessions.pop(session_hash, None)
            return None
        self._sessions.move_to_end(session_hash)
        return role, csrf_token

    def revoke_session(self, session_id: Optional[str]) -> None:
        if not session_id:
            return
        self._sessions.pop(hash_key(session_id), None)

    def clear(self) -> None:
        self._sessions.clear()


session_manager = SessionManager()


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
    Validates either:
    1. Programmatic X-API-Key header (constant-time SHA-256 comparison), OR
    2. Browser HttpOnly session cookie ('hr_session') + X-CSRF-Token on mutating methods.
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

    # 1. Check programmatic X-API-Key header first
    if x_api_key:
        role = authenticate_raw_key(x_api_key)
        if role is not None:
            return role

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

    # 2. Check HttpOnly session cookie (used by Dashboard UI)
    if request is not None:
        session_cookie = request.cookies.get("hr_session")
        if session_cookie:
            sess = session_manager.get_session(session_cookie)
            if sess is not None:
                role, expected_csrf = sess
                # Enforce CSRF token on state-changing HTTP methods when using cookie auth
                if request.method.upper() in ("POST", "PUT", "PATCH", "DELETE"):
                    provided_csrf = request.headers.get("X-CSRF-Token", "")
                    if not provided_csrf or not hmac.compare_digest(provided_csrf, expected_csrf):
                        await _audit_security_event(
                            request,
                            actor_role=role.value,
                            action="csrf_failed",
                            status_str="forbidden",
                            details="Missing or invalid X-CSRF-Token for cookie session"
                        )
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail="CSRF validation failed: Missing or invalid X-CSRF-Token header."
                        )
                return role

            auth_failure_limiter.is_allowed(client_ip, settings.auth_failure_limit_per_minute)
            await _audit_security_event(
                request,
                actor_role="UNAUTHENTICATED",
                action="auth_failed",
                status_str="denied",
                details="Expired or invalid session cookie"
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired session.",
                headers={"WWW-Authenticate": "ApiKey"}
            )

    if request is not None:
        auth_failure_limiter.is_allowed(client_ip, settings.auth_failure_limit_per_minute)
        await _audit_security_event(
            request,
            actor_role="UNAUTHENTICATED",
            action="auth_failed",
            status_str="denied",
            details="Missing authentication credentials"
        )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Missing required X-API-Key header or active session.",
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
