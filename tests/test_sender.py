import asyncio
import pytest
import httpx
from app.sender import DiscordSender, RetryableError, NonRetryableError


@pytest.mark.asyncio
async def test_discord_success_first_try():
    """Simulates Discord 204 No Content response on first attempt."""
    calls = []

    def mock_handler(request: httpx.Request):
        calls.append(request)
        return httpx.Response(204)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        sender = DiscordSender(client=client, max_retries=3)
        attempts = await sender.send_to_discord("https://discord.mock/webhook", {"content": "hello"})

    assert attempts == 1
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_discord_retry_on_500_then_success():
    """Simulates Discord returning 500 twice, then 200 on third attempt."""
    call_count = 0

    def mock_handler(request: httpx.Request):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            return httpx.Response(500, text="Internal Server Error")
        return httpx.Response(204)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        sender = DiscordSender(client=client, max_retries=4)
        attempts = await sender.send_to_discord("https://discord.mock/webhook", {"content": "retry test"})

    assert attempts == 3
    assert call_count == 3


@pytest.mark.asyncio
async def test_discord_rate_limit_429_handled():
    """Simulates Discord 429 rate limit with Retry-After header."""
    call_count = 0

    def mock_handler(request: httpx.Request):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(429, headers={"Retry-After": "0.01"}, json={"retry_after": 0.01})
        return httpx.Response(200)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        sender = DiscordSender(client=client, max_retries=3)
        attempts = await sender.send_to_discord("https://discord.mock/webhook", {"content": "rate limit test"})

    assert attempts == 2
    assert call_count == 2


@pytest.mark.asyncio
async def test_discord_fatal_400_aborts_without_retry():
    """Simulates 400 Bad Request which should immediately raise NonRetryableError."""
    call_count = 0

    def mock_handler(request: httpx.Request):
        nonlocal call_count
        call_count += 1
        return httpx.Response(400, text="Bad Request: payload too large")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        sender = DiscordSender(client=client, max_retries=5)
        with pytest.raises(NonRetryableError):
            await sender.send_to_discord("https://discord.mock/webhook", {"content": "bad payload"})

    # Must NOT retry 400 errors
    assert call_count == 1


@pytest.mark.asyncio
async def test_discord_exhaust_retries_raises():
    """Simulates persistent 500 error exhausting all retries."""
    call_count = 0

    def mock_handler(request: httpx.Request):
        nonlocal call_count
        call_count += 1
        return httpx.Response(503, text="Service Unavailable")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        sender = DiscordSender(client=client, max_retries=3)
        with pytest.raises(RetryableError):
            await sender.send_to_discord("https://discord.mock/webhook", {"content": "persistent failure"})

    assert call_count == 3
