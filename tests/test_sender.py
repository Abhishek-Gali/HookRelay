import pytest
import httpx
from app.providers import extract_retry_after, read_bounded_error
from app.sender import DiscordSender, RetryPolicy, RetryableError, NonRetryableError


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
        sender.policy.initial_backoff = 0.01
        sender.policy.jitter = 0.0
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


def test_retry_after_30_seconds_respected():
    """
    Proves RetryPolicy honors server Retry-After of 30 seconds instead of capping at 5 seconds,
    while enforcing the configurable safety ceiling (default 60s).
    """
    policy = RetryPolicy(max_attempts=5, max_retry_after_seconds=60.0)
    assert policy.compute_sleep_seconds(attempt_num=1, retry_after=30.0) == 30.0
    assert policy.compute_sleep_seconds(attempt_num=1, retry_after=120.0) == 60.0


def test_retry_after_malformed_value_handled_safely():
    """Proves malformed Retry-After headers do not crash parsing and fall back to backoff."""
    resp = httpx.Response(429, headers={"Retry-After": "not-a-number"}, text="rate limited")
    parsed = extract_retry_after(resp)
    assert parsed is None

    policy = RetryPolicy(max_attempts=3, initial_backoff=1.0, jitter=0.0)
    sleep_s = policy.compute_sleep_seconds(attempt_num=2, retry_after=parsed)
    assert sleep_s == 2.0


def test_downstream_error_body_bounded():
    """Proves oversized downstream error payloads are bounded to max_error_body_bytes (2048)."""
    huge_error = "E" * 25000
    resp = httpx.Response(500, text=huge_error)
    bounded = read_bounded_error(resp)
    assert bounded is not None
    assert len(bounded) == 2048


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
        sender.policy.initial_backoff = 0.01
        sender.policy.jitter = 0.0
        with pytest.raises(RetryableError):
            await sender.send_to_discord("https://discord.mock/webhook", {"content": "persistent failure"})

    assert call_count == 3


def test_webhook_secret_redacted_in_error_message():
    """Proves Discord and Slack webhook secret tokens are redacted from stored error messages."""
    from app.providers import sanitize_error_message

    raw_err = (
        "ConnectError for https://discord.com/api/webhooks/1234567890/super_secret_token_xyz "
        "and https://hooks.slack.com/services/T111/B222/slack_secret_token_999"
    )
    redacted = sanitize_error_message(raw_err)
    assert "super_secret_token_xyz" not in redacted
    assert "slack_secret_token_999" not in redacted
    assert "/api/webhooks/1234567890/[REDACTED]" in redacted
    assert "/services/T111/B222/[REDACTED]" in redacted

