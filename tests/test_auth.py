import pytest
from fastapi import HTTPException
from app.auth import get_current_user_role, Role, hash_key
from app.config import settings


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
