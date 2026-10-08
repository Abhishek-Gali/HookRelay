"""Test fixtures and mock helpers for HookRelay test suite."""
import os
import pytest
import pytest_asyncio
import httpx
from httpx import ASGITransport

# Set test environment variables before importing app
os.environ["GITHUB_WEBHOOK_SECRET"] = "test_secret_key_xyz_at_least_16_chars"
os.environ["DISCORD_WEBHOOK_URL"] = "https://discord.com/api/webhooks/test/channel"
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
os.environ["ENVIRONMENT"] = "test"
os.environ["ENABLE_RECONCILIATION"] = "false"

from app.config import settings
from app.store import DeliveryStore
from app.ratelimit import (
    webhook_rate_limiter,
    api_rate_limiter,
    redrive_rate_limiter,
    auth_failure_limiter,
)
from app.auth import session_manager
import app.main as main_module
from app.main import app
from app.security import calculate_signature


@pytest_asyncio.fixture(autouse=True)
async def setup_test_db():
    """Provides a fresh in-memory SQLite database and clean rate limiters for each test."""
    webhook_rate_limiter.reset()
    api_rate_limiter.reset()
    redrive_rate_limiter.reset()
    auth_failure_limiter.reset()
    session_manager.clear()

    test_store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await test_store.init_db()

    main_module.store = test_store
    main_module._sync_store_bindings()
    yield test_store
    await test_store.close()


@pytest_asyncio.fixture
async def async_client():
    """Async test client for testing FastAPI endpoints."""
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest.fixture
def secret_key():
    return settings.github_webhook_secret


@pytest.fixture
def sign_payload(secret_key):
    """Helper fixture to compute valid HMAC signature header for raw bytes."""
    def _signer(raw_body: bytes) -> str:
        return calculate_signature(secret_key, raw_body)
    return _signer
