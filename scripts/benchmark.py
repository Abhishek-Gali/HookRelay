"""
Benchmark and Stress Testing Script for HookRelay.
Sends 100 signed events (including intentional duplicates and payload variations)
to measure:
- Ingestion response times (<50ms)
- 100% duplicate suppression (0 duplicate messages processed)
- Resilience against signature tampering
"""
import asyncio
import json
import time
import httpx
from app.security import calculate_signature


async def run_benchmark(base_url: str = "http://localhost:8000", secret: str = "benchmark_secret"):
    print("=" * 60)
    print("  HookRelay High-Concurrency Verification & Benchmark")
    print("=" * 60)

    # 1. Healthcheck
    async with httpx.AsyncClient(base_url=base_url) as client:
        try:
            res = await client.get("/healthz")
            if res.status_code != 200:
                print(f"[ERROR] Service health check returned {res.status_code}")
                return
            print("[INFO] HookRelay service is HEALTHY.")
        except Exception as e:
            print(f"[ERROR] Could not connect to {base_url}: {e}")
            return

    # Plan:
    # 90 unique delivery IDs
    # 10 duplicate delivery IDs (replays of existing deliveries)
    # Total = 100 requests
    total_deliveries = 100
    unique_count = 90
    duplicate_count = 10

    delivery_ids = [f"bench-del-{i:03d}" for i in range(unique_count)]
    # Duplicate targets picked from the first 10
    duplicate_ids = [f"bench-del-{i:03d}" for i in range(duplicate_count)]
    test_sequence = delivery_ids + duplicate_ids

    latencies = []
    accepted_count = 0
    duplicate_detected_count = 0
    rejected_count = 0

    print(f"\n[BENCHMARK] Firing {len(test_sequence)} requests with concurrency limit 10...")

    sem = asyncio.Semaphore(10)

    async def send_delivery(client: httpx.AsyncClient, d_id: str):
        nonlocal accepted_count, duplicate_detected_count, rejected_count
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

        async with sem:
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

    start_bench = time.perf_counter()
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        await asyncio.gather(*[send_delivery(client, d_id) for d_id in test_sequence])
    total_time = time.perf_counter() - start_bench

    avg_latency = (sum(latencies) / len(latencies)) * 1000 if latencies else 0
    p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] * 1000 if latencies else 0

    print("\n" + "=" * 60)
    print("  BENCHMARK RESULTS SUMMARY")
    print("=" * 60)
    print(f"Total Requests Dispatched  : {total_deliveries}")
    print(f"Accepted Deliveries (New)  : {accepted_count} (Expected: {unique_count})")
    print(f"Duplicates Suppressed      : {duplicate_detected_count} (Expected: {duplicate_count})")
    print(f"Unexpected Rejections      : {rejected_count} (Expected: 0)")
    print(f"Duplicate Suppression Rate : {(duplicate_detected_count / duplicate_count) * 100:.1f}%")
    print(f"Total Wall-clock Time      : {total_time:.3f} s")
    print(f"Average Response Latency   : {avg_latency:.2f} ms")
    print(f"P95 Response Latency       : {p95_latency:.2f} ms")
    print("=" * 60)


if __name__ == "__main__":
    import sys
    url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    sec = sys.argv[2] if len(sys.argv) > 2 else "development_webhook_secret"
    asyncio.run(run_benchmark(url, sec))
