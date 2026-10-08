import pytest
from pydantic import ValidationError
from fastapi import HTTPException
from app.auth import get_current_user_role, Role
from app.config import Settings, settings


@pytest.mark.asyncio
async def test_admin_api_key_role():
    role = await get_current_user_role(x_api_key=settings.admin_api_key)
    assert role == Role.ADMIN


@pytest.mark.asyncio
async def test_operator_api_key_role():
    role = await get_current_user_role(x_api_key=settings.operator_api_key)
    assert role == Role.OPERATOR


@pytest.mark.asyncio
async def test_viewer_api_key_role():
    role = await get_current_user_role(x_api_key=settings.viewer_api_key)
    assert role == Role.VIEWER


@pytest.mark.asyncio
async def test_missing_api_key_raises_401():
    with pytest.raises(HTTPException) as exc:
        await get_current_user_role(x_api_key=None)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_invalid_api_key_raises_401():
    with pytest.raises(HTTPException) as exc:
        await get_current_user_role(x_api_key="wrong_fake_key_123")
    assert exc.value.status_code == 401


def test_default_credentials_rejected_in_production():
    """
    Proves that deploying in production with known default credentials or blank keys
    immediately raises a startup ValidationError.
    """
    with pytest.raises(ValidationError, match="ADMIN_API_KEY"):
        Settings(
            environment="production",
            github_webhook_secret="strong_production_webhook_secret_key_999",
            admin_api_key="hr_admin_secret_key_12345",
        )


def test_weak_secret_rejected_in_production():
    """Proves short/weak webhook secrets or short API keys are rejected in production."""
    with pytest.raises(ValidationError, match="GITHUB_WEBHOOK_SECRET"):
        Settings(
            environment="production",
            github_webhook_secret="short",
            admin_api_key="a" * 36,
        )

    with pytest.raises(ValidationError, match="ADMIN_API_KEY"):
        Settings(
            environment="production",
            github_webhook_secret="strong_production_webhook_secret_key_999",
            admin_api_key="too_short_key",
        )
