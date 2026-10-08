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

    # Worker B renews its lease (heartbeat) -> locked_until extends further
    renewed = await store.renew_lease("del-lease-exp-001", worker_id="worker-B", lease_seconds=300)
    assert renewed is True

    # Worker A cannot renew a lease owned by Worker B
    wrong_worker_renew = await store.renew_lease("del-lease-exp-001", worker_id="worker-A", lease_seconds=300)
    assert wrong_worker_renew is False

    await store.close()


@pytest.mark.asyncio
async def test_fencing_token_rejects_stale_worker_state_mutation():
    """
    HR-02 & HR-03: Proves that monotonic fencing tokens (lease_generation) prevent
    a slow/stalled worker whose lease expired from overwriting state after another
    worker has reclaimed the job.
    """
    from app.store import StaleWorkerLeaseError

    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    await store.claim_delivery("del-fencing-001", "push", "org/repo", {"commits": []})

    # Worker A acquires initial lease -> generation 1
    lease_a = await store.acquire_lease("del-fencing-001", worker_id="worker-A", lease_seconds=60)
    assert lease_a is not None
    assert lease_a.lease_generation == 1

    # Simulate Worker A stalling beyond lease expiry
    expired_time = datetime.now(timezone.utc) - timedelta(minutes=2)
    async with store.session_factory() as session:
        await session.execute(
            update(DeliveryModel)
            .where(DeliveryModel.delivery_id == "del-fencing-001")
            .values(locked_until=expired_time)
        )
        await session.commit()

    # Worker B reclaims expired lease -> generation increments to 2
    lease_b = await store.acquire_lease("del-fencing-001", worker_id="worker-B", lease_seconds=60)
    assert lease_b is not None
    assert lease_b.worker_id == "worker-B"
    assert lease_b.lease_generation == 2

    # Stale Worker A (generation 1) wakes up and tries to mark_sent or mark_failed -> MUST FAIL
    with pytest.raises(StaleWorkerLeaseError):
        await store.mark_sent(
            "del-fencing-001",
            attempts=1,
            worker_id="worker-A",
            lease_generation=1,
        )

    with pytest.raises(StaleWorkerLeaseError):
        await store.mark_failed_or_dlq(
            "del-fencing-001",
            attempts=5,
            error="stale worker failure",
            worker_id="worker-A",
            lease_generation=1,
        )

    # Stale Worker A cannot renew lease with old generation
    assert await store.renew_lease("del-fencing-001", worker_id="worker-A", lease_generation=1) is False

    # Active Worker B (generation 2) succeeds in marking sent
    await store.mark_sent(
        "del-fencing-001",
        attempts=1,
        worker_id="worker-B",
        lease_generation=2,
    )
    final_row = await store.get_delivery("del-fencing-001", include_attempts=False)
    assert final_row.status == "sent"

    await store.close()


@pytest.mark.asyncio
async def test_partial_destination_failure_redrive_skips_already_succeeded_destinations(monkeypatch):
    """
    HR-06: Proves that when a multi-destination delivery partially succeeds (e.g. Discord
    and Slack succeed, but Generic HTTP fails), redriving or re-dispatching the job only
    retries the failed destination and never duplicates messages to already-sent destinations.
    """
    import httpx
    from app.dispatcher import ResilientDispatcher
    from app.routing import parse_persisted_destinations
    from app import providers as providers_module

    # Mock outbound SSRF DNS resolution in unit test so MockTransport works offline
    monkeypatch.setattr(providers_module, "_enforce_outbound_ssrf_guard", lambda url: {})


    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()

    destinations = [
        {"provider": "discord", "url": "https://discord.com/api/webhooks/111/aaa", "status": "pending"},
        {"provider": "slack", "url": "https://hooks.slack.com/services/222/bbb", "status": "pending"},
        {"provider": "http", "url": "https://api.example.com/webhook", "status": "pending"},
    ]
    await store.claim_delivery(
        delivery_id="del-partial-dest-001",
        event_type="push",
        repo="Abhishek-Gali/HookRelay",
        payload={"repository": {"full_name": "Abhishek-Gali/HookRelay"}},
        destinations=destinations,
    )

    call_counts = {"discord": 0, "slack": 0, "http": 0}
    http_should_fail = True

    def mock_handler(request: httpx.Request):
        host = request.url.host or ""
        if "discord.com" in host:
            call_counts["discord"] += 1
            return httpx.Response(204)
        if "slack.com" in host:
            call_counts["slack"] += 1
            return httpx.Response(200, text="ok")
        call_counts["http"] += 1
        if http_should_fail:
            return httpx.Response(502, text="Bad Gateway")
        return httpx.Response(200, text="ok")

    disp = ResilientDispatcher(store=store, max_retries=1)
    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        # First run: Discord and Slack succeed, HTTP fails -> job enters dead_letter
        lease_1 = await store.acquire_lease("del-partial-dest-001", worker_id="w1", lease_seconds=60)
        dest_objs_1 = parse_persisted_destinations(destinations, "https://discord.com/api/webhooks/111/aaa", only_unsent=True)
        await disp.dispatch_job(
            delivery_id="del-partial-dest-001",
            event_type="push",
            payload={"repository": {"full_name": "Abhishek-Gali/HookRelay"}},
            destinations=dest_objs_1,
            client=client,
            worker_id="w1",
            lease_generation=lease_1.lease_generation,
        )

        row_after_fail = await store.get_delivery("del-partial-dest-001", include_attempts=False)
        assert row_after_fail.status == "dead_letter"
        assert call_counts == {"discord": 1, "slack": 1, "http": 1}

        # Now HTTP endpoint recovers and operator redrives / broker dequeues unsent destinations
        http_should_fail = False
        await store.prepare_for_redrive("del-partial-dest-001")
        broker = DatabaseQueueBroker(delivery_store=store, lease_seconds=60)
        redriven_job = await broker.dequeue(worker_id="w2")
        assert redriven_job is not None
        # Only the failed HTTP destination should be scheduled for retry
        assert len(redriven_job["destinations"]) == 1
        assert redriven_job["destinations"][0]["provider"] == "http"

        dest_objs_2 = parse_persisted_destinations(redriven_job["destinations"], "https://discord.com/api/webhooks/111/aaa", only_unsent=True)
        await disp.dispatch_job(
            delivery_id="del-partial-dest-001",
            event_type="push",
            payload=redriven_job["payload"],
            destinations=dest_objs_2,
            client=client,
            worker_id="w2",
            lease_generation=redriven_job["lease_generation"],
            trigger_type="redrive",
        )


        row_final = await store.get_delivery("del-partial-dest-001", include_attempts=False)
        assert row_final.status == "sent"
        # Discord and Slack were NEVER called a second time; HTTP was called once more
        assert call_counts == {"discord": 1, "slack": 1, "http": 2}

    await store.close()


@pytest.mark.asyncio
async def test_multi_worker_distributed_pool_drains_100_jobs_without_duplicates(monkeypatch):
    """
    Proves 4 concurrent distributed workers (worker-0..worker-3) can drain 100 queued
    deliveries simultaneously with zero duplicate dispatches and 100% completion.
    """
    import httpx
    from app.dispatcher import ResilientDispatcher
    from app.routing import parse_persisted_destinations
    from app import providers as providers_module

    monkeypatch.setattr(providers_module, "_enforce_outbound_ssrf_guard", lambda url: {})

    store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await store.init_db()
    broker = DatabaseQueueBroker(delivery_store=store, lease_seconds=60)
    await broker.start()

    for i in range(100):
        await store.claim_delivery(
            delivery_id=f"del-pool-{i:03d}",
            event_type="push",
            repo="Abhishek-Gali/HookRelay",
            payload={"repository": {"full_name": "Abhishek-Gali/HookRelay"}, "commits": [{"id": str(i)}]},
            destinations=[{"provider": "discord", "url": "https://discord.com/api/webhooks/111/pool", "status": "pending"}],
        )

    dispatched_ids = []

    def mock_handler(request: httpx.Request):
        dispatched_ids.append(request.headers.get("X-HookRelay-Delivery-ID"))
        return httpx.Response(204)

    disp = ResilientDispatcher(store=store, max_retries=1)
    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        async def worker_drain(idx: int):
            wid = f"dist-worker-{idx}"
            count = 0
            while True:
                job = await broker.dequeue(worker_id=wid)
                if not job:
                    break
                dests = parse_persisted_destinations(
                    job["destinations"],
                    "https://discord.com/api/webhooks/111/pool",
                    only_unsent=True,
                )
                await disp.dispatch_job(
                    delivery_id=job["delivery_id"],
                    event_type=job["event_type"],
                    payload=job["payload"],
                    destinations=dests,
                    client=client,
                    worker_id=wid,
                    lease_generation=job["lease_generation"],
                    trigger_type="worker",
                )
                count += 1
            return count

        counts = await asyncio.gather(*(worker_drain(w) for w in range(4)))

    assert sum(counts) == 100
    assert len(dispatched_ids) == 100
    assert len(set(dispatched_ids)) == 100  # Zero duplicates

    await broker.stop()
    await store.close()




