"""
HookRelay Chaos & Failure Recovery Live Demonstration
=====================================================
Demonstrates why HookRelay exists in under 10 seconds:

1. Scenario 1 — Downstream Outage & Rate-Limit Recovery:
   GitHub -> HookRelay -> SQL Queue -> Worker -> Discord
   - Attempt 1: Discord DOWN (HTTP 503 Service Unavailable) -> Queued for Retry 1
   - Attempt 2: Discord RATE LIMITED (HTTP 429 Retry-After) -> Queued for Retry 2
   - Attempt 3: Discord UP (HTTP 204 No Content)            -> Message Delivered!

2. Scenario 2 — Worker Crash Mid-Flight & Split-Brain Fencing Token Protection:
   - Worker A claims lease (lease_generation = 1) and stalls/crashes mid-flight
   - Lease expires -> Worker B reclaims job (lease_generation = 2) & delivers to Discord
   - Zombie Worker A wakes up and tries to overwrite state -> Blocked by StaleWorkerLeaseError!

Usage:
    python -m scripts.demo_chaos_recovery
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
from typing import Any
from unittest.mock import patch

import httpx

from app.dispatcher import ResilientDispatcher
from app.routing import RouteDestination
from app.security import calculate_signature
from app.sender import RetryPolicy
from app.store import DeliveryStore, StaleWorkerLeaseError


def _banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(f"  {title}")
    print("=" * 78)


async def run_chaos_demo() -> None:
    db_fd, db_path = tempfile.mkstemp(suffix=".sqlite3", prefix="hookrelay_chaos_")
    os.close(db_fd)
    db_url = f"sqlite+aiosqlite:///{db_path}"

    store = DeliveryStore(database_url=db_url)
    await store.init_db()

    try:
        _banner("SCENARIO 1: DOWNSTREAM OUTAGE (503) -> RATE LIMIT (429) -> RECOVERY (204)")
        payload = {
            "ref": "refs/heads/main",
            "repository": {
                "full_name": "Abhishek-Gali/HookRelay",
                "html_url": "https://github.com/Abhishek-Gali/HookRelay",
            },
            "pusher": {"name": "Abhishek-Gali"},
            "commits": [{"id": "4915e85", "message": "feat: add distributed worker pool & fencing tokens"}],
        }
        raw_body = json.dumps(payload).encode("utf-8")
        sig = calculate_signature("demo_webhook_secret", raw_body)
        delivery_id = "deliv-outage-recovery-001"
        destinations = [{"provider": "discord", "url": "https://discord.com/api/webhooks/123456/demo_token", "status": "pending"}]

        print(f"  [1] GitHub sends signed 'push' webhook ({len(raw_body)} bytes, sig={sig[:22]}...)")
        claimed = await store.claim_delivery(
            delivery_id=delivery_id,
            event_type="push",
            repo="Abhishek-Gali/HookRelay",
            payload=payload,
            destinations=destinations,
            raw_body=raw_body,
        )
        print(f"  [2] HookRelay verified HMAC & persisted job in SQL: delivery_id='{delivery_id}' (claimed={claimed})")

        # Simulate Discord being DOWN on Attempt 1 (503), Rate-Limited on Attempt 2 (429), and UP on Attempt 3 (204)
        call_counter = 0

        async def simulated_discord_transport(self: Any, url: str, **kwargs: Any) -> httpx.Response:
            nonlocal call_counter
            call_counter += 1
            req = httpx.Request("POST", url)
            if call_counter == 1:
                print("      ├──► Attempt 1 -> Discord DOWN ❌ (HTTP 503 Service Unavailable)")
                return httpx.Response(503, request=req, text="Service Unavailable: upstream outage")
            if call_counter == 2:
                print("      ├──► Attempt 2 -> Discord RATE LIMITED ⚠️ (HTTP 429 Retry-After: 0.1s)")
                return httpx.Response(429, request=req, headers={"Retry-After": "0.1"}, text="Too Many Requests")
            print("      └──► Attempt 3 -> Discord UP ✅ (HTTP 204 No Content - Rich Embed Delivered!)")
            return httpx.Response(204, request=req)

        dispatcher = ResilientDispatcher(
            store=store,
            max_retries=4,
            policy=RetryPolicy(max_attempts=4, initial_backoff=0.05, max_backoff=0.2),
        )
        route_targets = [RouteDestination(provider="discord", url="https://discord.com/api/webhooks/123456/demo_token")]
        leased_primary = await store.acquire_lease(delivery_id, worker_id="worker-primary-01", lease_seconds=30)
        assert leased_primary is not None
        gen_primary = leased_primary.lease_generation

        with (
            patch("app.routing.resolve_and_pin_destination", side_effect=lambda u, **kw: (u, "162.159.135.232", "discord.com")),
            patch.object(httpx.AsyncClient, "post", new=simulated_discord_transport),
        ):
            print(f"  [3] Worker 'worker-primary-01' claims lease (gen={gen_primary}) & starts delivery with automatic retries:")
            t0 = time.perf_counter()
            success = await dispatcher.dispatch_job(
                delivery_id=delivery_id,
                event_type="push",
                payload=payload,
                destinations=route_targets,
                worker_id="worker-primary-01",
                lease_generation=gen_primary,
                trigger_type="initial",
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000.0

        record = await store.get_delivery(delivery_id, include_attempts=True)
        assert record is not None
        print(f"  [4] Final SQL Delivery Status: status='{record.status}' | attempts={record.attempts} | total_time={elapsed_ms:.1f}ms")
        for att in record.attempt_history:
            outcome = "sent" if att.http_status and 200 <= att.http_status < 300 else "failed"
            print(
                f"      • Attempt #{att.attempt_number}: outcome='{outcome}' | "
                f"http_status={att.http_status} | latency={(att.response_time_ms or 0.0):.2f}ms | error={att.error_message or 'None'}"
            )
        assert success is True and record.status == "sent" and record.attempts == 3

        _banner("SCENARIO 2: WORKER CRASH MID-FLIGHT & FENCING TOKEN SPLIT-BRAIN PROTECTION")
        crash_id = "deliv-worker-crash-002"
        await store.claim_delivery(
            delivery_id=crash_id,
            event_type="release",
            repo="Abhishek-Gali/HookRelay",
            payload=payload,
            destinations=destinations,
            raw_body=raw_body + b" ",
        )

        # Worker A acquires lease with a 0-second TTL and stalls
        leased_a = await store.acquire_lease(crash_id, worker_id="worker-A-stalled", lease_seconds=0)
        assert leased_a is not None
        gen_a = leased_a.lease_generation
        print(f"  [1] Worker A ('worker-A-stalled') claims '{crash_id}' -> lease_generation={gen_a}")
        print("  [2] Worker A suffers a network hang / GC pause 💥 (Lease expires!)")

        await asyncio.sleep(0.05)

        # Worker B (Reconciler / Peer) reclaims the expired lease; generation increments to 2!
        leased_b = await store.acquire_lease(crash_id, worker_id="worker-B-rescuer", lease_seconds=30)
        assert leased_b is not None
        gen_b = leased_b.lease_generation
        print(f"  [3] Worker B ('worker-B-rescuer') reclaims expired job -> lease_generation={gen_b} (incremented!)")

        await store.mark_sent(
            crash_id,
            attempts=1,
            worker_id="worker-B-rescuer",
            lease_generation=gen_b,
        )
        print("  [4] Worker B delivers webhook & commits status='sent' with fencing token (lease_generation=2) ✅")

        # Now Zombie Worker A wakes up and tries to mark the job failed with its stale generation=1
        print("  [5] Zombie Worker A wakes up and attempts to overwrite SQL state with stale lease_generation=1...")
        try:
            await store.mark_failed_or_dlq(
                crash_id,
                attempts=1,
                error="Stale worker timeout",
                worker_id="worker-A-stalled",
                lease_generation=gen_a,
            )
        except StaleWorkerLeaseError as exc:
            print(f"      └──► BLOCKED BY DATABASE FENCING GUARD 🛡️: {exc}")

        final_record = await store.get_delivery(crash_id)
        assert final_record is not None and final_record.status == "sent"
        print(f"  [6] Verified Final SQL State remains intact: status='{final_record.status}', lease_generation={final_record.lease_generation}")
        print("\n" + "=" * 78)
        print("  ALL CHAOS & RECOVERY CHECKS PASSED (0 lost webhooks, 0 duplicate deliveries)")
        print("=" * 78 + "\n")

    finally:
        await store.close()
        for suffix in ("", "-wal", "-shm"):
            p = db_path + suffix
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


if __name__ == "__main__":
    asyncio.run(run_chaos_demo())
