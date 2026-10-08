import asyncio
import pytest
from app.store import DeliveryStore, IllegalStateTransitionError


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
    assert results.count(True) == 1
    assert results.count(False) == 19

    await store.close()


@pytest.mark.asyncio
async def test_status_lifecycle_transitions_and_state_machine():
    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    await store.claim_delivery("del-202", "issues", "owner/repo")
    row = await store.get_delivery("del-202")
    assert row.status == "received"
    assert row.attempts == 0

    # Transition received -> processing -> sent
    leased = await store.acquire_lease("del-202", worker_id="w1")
    assert leased is not None
    assert leased.status == "processing"

    await store.mark_sent("del-202", attempts=1)
    row = await store.get_delivery("del-202")
    assert row.status == "sent"
    assert row.attempts == 1

    # Illegal transition: sent -> dead_letter must raise IllegalStateTransitionError
    with pytest.raises(IllegalStateTransitionError):
        await store.mark_failed("del-202", attempts=5, error="Cannot fail after sent")

    # Separate delivery transitioning received -> dead_letter -> discarded
    await store.claim_delivery("del-203", "issues", "owner/repo")
    await store.mark_failed("del-203", attempts=5, error="Discord 500 error")
    row_failed = await store.get_delivery("del-203")
    assert row_failed.status == "dead_letter"
    assert row_failed.attempts == 5
    assert "Discord 500 error" in row_failed.last_error

    discarded = await store.discard_dlq("del-203")
    assert discarded is True
    with pytest.raises(IllegalStateTransitionError):
        await store.mark_sent("del-203", attempts=6)

    await store.close()


@pytest.mark.asyncio
async def test_retention_policy_scrubs_payloads_and_prunes_old_logs():
    """
    HR-09: Proves enforce_retention_policy() scrubs raw webhook payloads on old terminal
    deliveries (while keeping the delivery_id row for idempotency) and deletes expired
    delivery_attempts and audit_logs rows.
    """
    from datetime import datetime, timezone, timedelta
    from sqlalchemy import update
    from app.models import DeliveryModel, DeliveryAttemptModel, AuditLogModel

    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    await store.claim_delivery("del-ret-001", "push", "org/repo", {"secret_commit": "abc"})
    await store.mark_sent("del-ret-001", attempts=1)
    await store.record_attempt(
        delivery_id="del-ret-001",
        destination="discord",
        attempt_number=1,
        http_status=204,
        response_time_ms=12.5,
    )
    await store.record_audit_log(actor_role="ADMIN", action="redrive", target_id="del-ret-001")

    old_date = datetime.now(timezone.utc) - timedelta(days=120)
    async with store.session_factory() as session:
        await session.execute(update(DeliveryModel).values(updated_at=old_date))
        await session.execute(update(DeliveryAttemptModel).values(created_at=old_date))
        await session.execute(update(AuditLogModel).values(created_at=old_date))
        await session.commit()

    summary = await store.enforce_retention_policy(
        payload_days=14,
        attempt_days=30,
        audit_days=90,
    )
    assert summary["scrubbed_payloads"] == 1
    assert summary["deleted_attempts"] == 1
    assert summary["deleted_audit_logs"] == 1

    # Delivery row still exists for idempotency, but payload is NULL
    row = await store.get_delivery("del-ret-001", include_attempts=True)
    assert row is not None
    assert row.payload is None
    assert len(row.attempt_history) == 0

    # Re-claiming the same delivery_id is still blocked
    assert await store.claim_delivery("del-ret-001", "push", "org/repo", {"secret_commit": "abc"}) is False

    await store.close()


