"""
Standalone Distributed Worker Entrypoint for HookRelay.

Allows horizontal scaling of queue consumers independently from FastAPI HTTP ingestion replicas:
    python -m app.worker --concurrency 4
"""
import argparse
import asyncio
import logging
import os
import socket
import uuid
from typing import List

import httpx

from app.config import settings
from app.dispatcher import ResilientDispatcher
from app.queue_broker import DatabaseQueueBroker
from app.reconciliation import start_reconciliation_worker
from app.routing import parse_persisted_destinations
from app.store import DeliveryStore, store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("hookrelay.worker")


async def run_worker_loop(
    worker_id: str,
    broker: DatabaseQueueBroker,
    dispatcher: ResilientDispatcher,
    http_client: httpx.AsyncClient,
    stop_event: asyncio.Event,
) -> None:
    """Continuously claims and dispatches runnable jobs using atomic SQL leases + fencing tokens."""
    logger.info(f"Distributed worker '{worker_id}' started.")
    while not stop_event.is_set():
        try:
            job = await broker.dequeue(worker_id=worker_id)
            if not job:
                await asyncio.sleep(0.2)
                continue

            dest_objs = parse_persisted_destinations(
                job.get("destinations"),
                settings.discord_webhook_url,
                only_unsent=True,
            )
            await dispatcher.dispatch_job(
                delivery_id=job["delivery_id"],
                event_type=job["event_type"],
                payload=job["payload"],
                destinations=dest_objs,
                client=http_client,
                worker_id=worker_id,
                lease_generation=job.get("lease_generation"),
                trigger_type="worker",
            )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error(f"Worker '{worker_id}' loop error: {exc}")
            await asyncio.sleep(1.0)


async def run_distributed_worker_pool(
    concurrency: int = 4,
    run_reconciliation: bool = True,
    target_store: DeliveryStore = store,
) -> None:
    """Starts a pool of concurrent worker coroutines sharing an async HTTP connection pool."""
    await target_store.init_db()
    broker = DatabaseQueueBroker(
        delivery_store=target_store,
        lease_seconds=settings.worker_lease_seconds,
    )
    await broker.start()

    dispatcher = ResilientDispatcher(
        store=target_store,
        max_retries=settings.max_retries,
        timeout=settings.request_timeout_seconds,
    )

    host_prefix = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
    stop_event = asyncio.Event()

    async with httpx.AsyncClient(
        timeout=settings.request_timeout_seconds,
        follow_redirects=False,
    ) as http_client:
        tasks: List[asyncio.Task] = []
        for idx in range(concurrency):
            wid = f"hr-dist-{host_prefix}-w{idx}"
            tasks.append(
                asyncio.create_task(
                    run_worker_loop(wid, broker, dispatcher, http_client, stop_event)
                )
            )

        if run_reconciliation and settings.enable_reconciliation:
            tasks.append(asyncio.create_task(start_reconciliation_worker(http_client)))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            stop_event.set()
            await broker.stop()
            await target_store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="HookRelay Distributed Worker Pool")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Number of concurrent worker coroutines in this process (default: 4)",
    )
    parser.add_argument(
        "--no-reconciliation",
        action="store_true",
        help="Disable background reconciliation loop on this worker node",
    )
    args = parser.parse_args()
    asyncio.run(
        run_distributed_worker_pool(
            concurrency=args.concurrency,
            run_reconciliation=not args.no_reconciliation,
        )
    )


if __name__ == "__main__":
    main()
