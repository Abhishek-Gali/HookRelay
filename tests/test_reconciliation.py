from datetime import datetime, timezone, timedelta
import pytest
import httpx
from sqlalchemy import update
from app.dispatcher import ResilientDispatcher
from app.models import DeliveryModel
from app.reconciliation import run_reconciliation_cycle
from app.sender import DiscordSender
from app.store import DeliveryStore


@pytest.mark.asyncio
async def test_reconciliation_sweeps_stale_deliveries():
    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    await store.claim_delivery("del-stuck-001", "push", "org/repo", {"commits": []})

    old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
    async with store.session_factory() as session:
        await session.execute(
            update(DeliveryModel)
            .where(DeliveryModel.delivery_id == "del-stuck-001")
            .values(updated_at=old_time)
        )
        await session.commit()

    sent_requests = []

    def mock_handler(request: httpx.Request):
        sent_requests.append(request)
        return httpx.Response(204)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        sender = DiscordSender(client=client)
        recovered = await run_reconciliation_cycle(
            store=store,
            sender=sender,
            discord_webhook_url="https://discord.mock/webhook",
            client=client,
            stale_threshold_seconds=120
        )

    assert recovered == 1
    assert len(sent_requests) == 1

    row = await store.get_delivery("del-stuck-001")
    assert row.status == "sent"

    await store.close()


@pytest.mark.asyncio
async def test_reconciliation_preserves_destinations():
    """
    Proves that when a delivery was routed to multiple destinations (Discord + Slack + HTTP),
    crash reconciliation restores and dispatches to ALL original destinations rather than
    only the default Discord webhook.
    """
    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    original_destinations = [
        {"provider": "discord", "url": "https://discord.com/api/webhooks/111/aaa"},
        {"provider": "slack", "url": "https://hooks.slack.com/services/222/bbb"},
        {"provider": "http", "url": "https://events.example.org/ingest"},
    ]
    await store.claim_delivery(
        delivery_id="del-multi-route-001",
        event_type="push",
        repo="Abhishek-Gali/HookRelay",
        payload={"repository": {"full_name": "Abhishek-Gali/HookRelay"}, "commits": []},
        destinations=original_destinations
    )

    old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
    async with store.session_factory() as session:
        await session.execute(
            update(DeliveryModel)
            .where(DeliveryModel.delivery_id == "del-multi-route-001")
            .values(updated_at=old_time)
        )
        await session.commit()

    called_urls = []

    def mock_handler(request: httpx.Request):
        called_urls.append(str(request.url))
        return httpx.Response(200, text="ok")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        dispatcher = ResilientDispatcher(store=store, max_retries=2)
        recovered = await run_reconciliation_cycle(
            store=store,
            sender=dispatcher,
            discord_webhook_url="https://discord.com/api/webhooks/default/unused",
            client=client,
            stale_threshold_seconds=120
        )

    assert recovered == 1
    assert called_urls == [
        "https://discord.com/api/webhooks/111/aaa",
        "https://hooks.slack.com/services/222/bbb",
        "https://events.example.org/ingest",
    ]
    await store.close()


@pytest.mark.asyncio
async def test_reconciliation_does_not_duplicate_active_delivery():
    """
    Proves that if a normal worker holds an active lease on a delivery,
    reconciliation cannot claim or double-deliver it.
    """
    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    await store.claim_delivery("del-active-001", "push", "org/repo", {"commits": []})
    # Worker holds a valid lease for the next 120 seconds
    leased = await store.acquire_lease("del-active-001", worker_id="active-worker", lease_seconds=120)
    assert leased is not None

    sent_requests = []

    def mock_handler(request: httpx.Request):
        sent_requests.append(request)
        return httpx.Response(204)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        dispatcher = ResilientDispatcher(store=store)
        recovered = await run_reconciliation_cycle(
            store=store,
            sender=dispatcher,
            discord_webhook_url="https://discord.mock/webhook",
            client=client,
            stale_threshold_seconds=0
        )

    assert recovered == 0
    assert len(sent_requests) == 0
    await store.close()


@pytest.mark.asyncio
async def test_reconciliation_handles_multi_batch_stale_jobs():
    """
    Proves reconciliation sweeps multiple batches in a single cycle instead of stopping at 20.
    """
    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
    for i in range(45):
        did = f"del-batch-{i:03d}"
        await store.claim_delivery(did, "push", "org/repo", {"commits": []})

    async with store.session_factory() as session:
        await session.execute(update(DeliveryModel).values(updated_at=old_time))
        await session.commit()

    def mock_handler(request: httpx.Request):
        return httpx.Response(204)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        dispatcher = ResilientDispatcher(store=store)
        recovered = await run_reconciliation_cycle(
            store=store,
            sender=dispatcher,
            discord_webhook_url="https://discord.mock/webhook",
            client=client,
            stale_threshold_seconds=120,
            batch_size=15,
            max_batches=5
        )

    assert recovered == 45
    await store.close()
