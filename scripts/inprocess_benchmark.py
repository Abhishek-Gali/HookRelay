import asyncio
import json
import time
import httpx
from app.config import settings
from app.security import calculate_signature
from app.store import DeliveryStore
import app.main as main_module
from app.main import app


async def run_inprocess_benchmark():
    # Use memory database
    bench_store = DeliveryStore("sqlite+aiosqlite:///:memory:")
    await bench_store.init_db()
    main_module.store = bench_store

    # Mock Discord responses as 204 No Content so it runs at pure memory speed
    mock_discord_calls = 0

    def mock_discord_handler(request: httpx.Request):
        nonlocal mock_discord_calls
        mock_discord_calls += 1
        return httpx.Response(204)

    mock_transport = httpx.MockTransport(mock_discord_handler)
    mock_discord_client = httpx.AsyncClient(transport=mock_transport)
    main_module.http_client = mock_discord_client

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        res = await client.get("/healthz")
        assert res.status_code == 200

        total_deliveries = 100
        unique_count = 90
        duplicate_count = 10

        delivery_ids = [f"bench-del-{i:03d}" for i in range(unique_count)]
        duplicate_ids = [f"bench-del-{i:03d}" for i in range(duplicate_count)]
        test_sequence = delivery_ids + duplicate_ids

        latencies = []
        accepted_count = 0
        duplicate_detected_count = 0
        rejected_count = 0

        secret = settings.github_webhook_secret

        start_bench = time.perf_counter()
        for d_id in test_sequence:
            payload_data = {
                "action": "opened",
                "issue": {
                    "title": f"Benchmark issue delivery {d_id}",
                    "html_url": f"https://github.com/acme/repo/issues/{d_id}"
                },
                "repository": {"full_name": "acme/benchmark-repo"},
                "sender": {"login": "perf-tester"}
            }
            body_bytes = json.dumps(payload_data).encode("utf-8")
            sig = calculate_signature(secret, body_bytes)
            headers = {
                "X-Hub-Signature-256": sig,
                "X-GitHub-Event": "issues",
                "X-GitHub-Delivery": d_id,
                "Content-Type": "application/json"
            }

            t0 = time.perf_counter()
            r = await client.post("/webhook/github", content=body_bytes, headers=headers)
            dt = time.perf_counter() - t0
            latencies.append(dt)

            if r.status_code == 200:
                data = r.json()
                if data.get("accepted"):
                    accepted_count += 1
                elif data.get("duplicate"):
                    duplicate_detected_count += 1
            else:
                rejected_count += 1

        total_time = time.perf_counter() - start_bench

        # Give background tasks a brief moment to finish
        await asyncio.sleep(0.2)

        avg_latency = (sum(latencies) / len(latencies)) * 1000
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] * 1000

        print("\n" + "=" * 60)
        print("  HOOKRELAY IN-PROCESS BENCHMARK RESULTS")
        print("=" * 60)
        print(f"Total Requests Dispatched  : {total_deliveries}")
        print(f"Accepted Deliveries (New)  : {accepted_count} (Target: {unique_count})")
        print(f"Duplicates Suppressed      : {duplicate_detected_count} (Target: {duplicate_count})")
        print(f"Unexpected Rejections      : {rejected_count} (Target: 0)")
        print(f"Duplicate Suppression Rate : {(duplicate_detected_count / duplicate_count) * 100:.1f}%")
        print(f"Total Ingestion Time       : {total_time:.3f} s")
        print(f"Average Response Latency   : {avg_latency:.2f} ms")
        print(f"P95 Response Latency       : {p95_latency:.2f} ms")
        print(f"Discord Mock Deliveries    : {mock_discord_calls}")
        print("=" * 60)

    await mock_discord_client.aclose()
    await bench_store.close()


if __name__ == "__main__":
    asyncio.run(run_inprocess_benchmark())
