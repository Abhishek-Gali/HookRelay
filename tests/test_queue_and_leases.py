import asyncio
from datetime import datetime, timezone, timedelta
import pytest
from sqlalchemy import update
from app.models import DeliveryModel
from app.queue_broker import DatabaseQueueBroker
from app.store import DeliveryStore


@pytest.mark.asyncio
async def test_queue_survives_restart():
    """
    Proves the SQL-backed queue is genuinely durable across broker/worker restarts.
    Broker 1 enqueues a job and is destroyed; Broker 2 connects to the same database
    and dequeues the persisted job with its original destinations intact.
    """
    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    broker_1 = DatabaseQueueBroker(delivery_store=store, lease_seconds=60)
    await broker_1.start()
    await broker_1.enqueue({
        "delivery_id": "del-durable-001",
        "event_type": "push",
        "payload": {"repository": {"full_name": "Abhishek-Gali/HookRelay"}},
        "destinations": [
            {"provider": "discord", "url": "https://discord.com/api/webhooks/111/aaa"},
            {"provider": "slack", "url": "https://hooks.slack.com/services/222/bbb"},
        ],
    })
    await broker_1.stop()
    del broker_1

    # Simulate process restart with a fresh broker instance
    broker_2 = DatabaseQueueBroker(delivery_store=store, lease_seconds=60)
    await broker_2.start()
    job = await broker_2.dequeue(worker_id="worker-restarted")

    assert job is not None
    assert job["delivery_id"] == "del-durable-001"
    assert len(job["destinations"]) == 2
    assert job["destinations"][0]["provider"] == "discord"
    assert job["destinations"][1]["provider"] == "slack"

    # Verify DB row is now leased by worker-restarted
    row = await store.get_delivery("del-durable-001", include_attempts=False)
    assert row.status == "processing"
    assert row.worker_id == "worker-restarted"
    assert row.locked_until is not None

    await broker_2.stop()
    await store.close()


@pytest.mark.asyncio
async def test_two_workers_cannot_claim_same_job():
    """
    Spawns 10 concurrent workers attempting to lease/dequeue the exact same delivery.
    Atomic SQL lease guarantees at most ONE worker wins the job.
    """
    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    await store.claim_delivery(
        delivery_id="del-lease-race-001",
        event_type="issues",
        repo="Abhishek-Gali/HookRelay",
        payload={"action": "opened"},
        destinations=[{"provider": "discord", "url": "https://discord.com/api/webhooks/1/a"}]
    )

    broker = DatabaseQueueBroker(delivery_store=store, lease_seconds=120)

    async def worker_try_dequeue(idx: int):
        return await broker.dequeue(worker_id=f"worker-{idx}")

    results = await asyncio.gather(*[worker_try_dequeue(i) for i in range(10)])
    winners = [r for r in results if r is not None]

    assert len(winners) == 1
    assert winners[0]["delivery_id"] == "del-lease-race-001"

    await store.close()


@pytest.mark.asyncio
async def test_worker_lease_prevents_duplicate_processing_and_reclaims_when_expired():
    """
    Proves an active worker lease blocks other workers, and an expired lease
    (crashed worker) can be safely reclaimed by another worker.
    """
    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    await store.claim_delivery("del-lease-exp-001", "push", "org/repo", {"commits": []})

    # Worker A acquires active lease
    lease_a = await store.acquire_lease("del-lease-exp-001", worker_id="worker-A", lease_seconds=120)
    assert lease_a is not None

    # Worker B tries to acquire lease while Worker A's lease is active -> must fail (None)
    lease_b = await store.acquire_lease("del-lease-exp-001", worker_id="worker-B", lease_seconds=120)
    assert lease_b is None

    # Simulate Worker A crashing and its lease expiring 5 minutes ago
    expired_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    async with store.session_factory() as session:
        await session.execute(
            update(DeliveryModel)
            .where(DeliveryModel.delivery_id == "del-lease-exp-001")
            .values(locked_until=expired_time)
        )
        await session.commit()

    # Now Worker B can reclaim the expired lease
    lease_b_reclaimed = await store.acquire_lease("del-lease-exp-001", worker_id="worker-B", lease_seconds=120)
    assert lease_b_reclaimed is not None
    assert lease_b_reclaimed.worker_id == "worker-B"

    await store.close()
