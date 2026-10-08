import asyncio
from datetime import datetime, timezone, timedelta
import pytest
import httpx
from app.store import DeliveryStore
from app.sender import DiscordSender
from app.reconciliation import run_reconciliation_cycle


@pytest.mark.asyncio
async def test_reconciliation_sweeps_stale_deliveries():
    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    # Insert a delivery in 'received' state
    await store.claim_delivery("del-stuck-001", "push", "org/repo", {"commits": []})

    # Artificially age the delivery by manually updating updated_at to 10 minutes ago
    old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
    async with store.session_factory() as session:
        from app.models import DeliveryModel
        from sqlalchemy import update
        await session.execute(
            update(DeliveryModel)
            .where(DeliveryModel.delivery_id == "del-stuck-001")
            .values(updated_at=old_time)
        )
        await session.commit()

    # Mock Discord response
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

    # Verify status changed from 'received' to 'sent'
    row = await store.get_delivery("del-stuck-001")
    assert row.status == "sent"

    await store.close()
