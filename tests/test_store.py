import asyncio
import pytest
from app.store import DeliveryStore


@pytest.mark.asyncio
async def test_atomic_claim_new_and_duplicate():
    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    # First claim succeeds
    claimed_1 = await store.claim_delivery("del-101", "push", "owner/repo", {"commits": []})
    assert claimed_1 is True

    # Immediate duplicate claim on same ID fails (idempotent)
    claimed_2 = await store.claim_delivery("del-101", "push", "owner/repo", {"commits": []})
    assert claimed_2 is False

    await store.close()


@pytest.mark.asyncio
async def test_concurrent_claim_race_condition():
    """
    Spawns 20 concurrent tasks trying to claim the exact same delivery ID simultaneously.
    Guarantees that database primary key + ON CONFLICT allows exactly 1 winner.
    """
    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    async def try_claim(task_id: int):
        return await store.claim_delivery("del-race-999", "pull_request", "owner/repo")

    results = await asyncio.gather(*[try_claim(i) for i in range(20)])
    # Exactly one True and 19 False
    assert results.count(True) == 1
    assert results.count(False) == 19

    await store.close()


@pytest.mark.asyncio
async def test_status_lifecycle_transitions():
    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    await store.claim_delivery("del-202", "issues", "owner/repo")
    row = await store.get_delivery("del-202")
    assert row.status == "received"
    assert row.attempts == 0

    # Mark sent
    await store.mark_sent("del-202", attempts=1)
    row = await store.get_delivery("del-202")
    assert row.status == "sent"
    assert row.attempts == 1

    # Mark failed / dead_letter
    await store.mark_failed("del-202", attempts=5, error="Discord 500 error")
    row = await store.get_delivery("del-202")
    assert row.status == "dead_letter"
    assert row.attempts == 5
    assert "Discord 500 error" in row.last_error

    await store.close()
