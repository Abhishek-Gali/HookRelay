import hashlib
import hmac
from enum import Enum
from typing import Optional
from fastapi import Header, HTTPException, status, Depends
from app.config import settings


class Role(str, Enum):
    ADMIN = "ADMIN"
    OPERATOR = "OPERATOR"
    VIEWER = "VIEWER"


def hash_key(key: str) -> str:
    """Returns SHA-256 hash of API key for safe comparison."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


# Precomputed hashes of configured keys
ADMIN_HASH = hash_key(settings.admin_api_key)
OPERATOR_HASH = hash_key(settings.operator_api_key)
VIEWER_HASH = hash_key(settings.viewer_api_key)


async def get_current_user_role(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
) -> Role:
    """
    Validates X-API-Key header against configured role keys using constant-time comparison.
    Raises 401 Unauthorized if missing or invalid.
    """
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing required X-API-Key header.",
            headers={"WWW-Authenticate": "ApiKey"}
        )

    provided_hash = hash_key(x_api_key)

    # Constant-time comparison prevents timing side-channels
    if hmac.compare_digest(provided_hash, ADMIN_HASH):
        return Role.ADMIN
    elif hmac.compare_digest(provided_hash, OPERATOR_HASH):
        return Role.OPERATOR
    elif hmac.compare_digest(provided_hash, VIEWER_HASH):
        return Role.VIEWER

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid X-API-Key.",
        headers={"WWW-Authenticate": "ApiKey"}
    )


def require_role(allowed_roles: list[Role]):
    """
    Dependency factory enforcing Role-Based Access Control (RBAC).
    """
    async def _role_checker(role: Role = Depends(get_current_user_role)) -> Role:
        if role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Forbidden: Action requires one of {[r.value for r in allowed_roles]}. Your role: {role.value}."
            )
        return role

    return _role_checker
