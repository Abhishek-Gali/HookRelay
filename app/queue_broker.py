from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import asyncio
import json
import logging
import uuid

from app.config import settings
from app.routing import parse_persisted_destinations
from app.store import DeliveryStore, store as default_store

logger = logging.getLogger("hookrelay.queue")


class BaseQueueBroker(ABC):
    """Abstract interface for durable queue brokers."""

    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass

    @abstractmethod
    async def enqueue(self, job: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def dequeue(self, worker_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_depth(self) -> int:
        pass


class DatabaseQueueBroker(BaseQueueBroker):
    """
    Production-grade SQL-backed durable job queue with atomic worker leases.
    - Jobs are persisted in the 'deliveries' table before acknowledgment.
    - Survives process restarts and crashes without losing queued work.
    - Uses an in-process asyncio.Event wakeup signal for sub-millisecond latency
      combined with SQL polling and atomic lease acquisition (worker_id + locked_until).
    """
    def __init__(self, delivery_store: Optional[DeliveryStore] = None, lease_seconds: Optional[int] = None):
        self.store = delivery_store or default_store
        self.lease_seconds = lease_seconds or settings.worker_lease_seconds
        self._wakeup = asyncio.Event()
        self._running = False
        self.default_worker_id = f"worker-{uuid.uuid4().hex[:8]}"

    async def start(self) -> None:
        self._running = True
        self._wakeup.set()

    async def stop(self) -> None:
        self._running = False
        self._wakeup.set()

    async def enqueue(self, job: Dict[str, Any]) -> None:
        """
        Ensures the delivery row is persisted in SQL (if not already claimed by the route)
        and wakes up any waiting worker immediately.
        """
        delivery_id = job["delivery_id"]
        existing = await self.store.get_delivery(delivery_id, include_attempts=False)
        if existing is None:
            await self.store.claim_delivery(
                delivery_id=delivery_id,
                event_type=job.get("event_type", "unknown"),
                repo=job.get("payload", {}).get("repository", {}).get("full_name"),
                payload=job.get("payload", {}),
                destinations=job.get("destinations")
            )
        self._wakeup.set()

    async def dequeue(self, worker_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Atomically claims and leases the next runnable job from the SQL store.
        Two workers calling dequeue() concurrently can NEVER receive the same job.
        """
        wid = worker_id or self.default_worker_id
        leased = await self.store.claim_next_runnable_job(
            worker_id=wid,
            lease_seconds=self.lease_seconds
        )
        if leased is not None:
            payload = json.loads(leased.payload) if leased.payload else {}
            destinations = parse_persisted_destinations(
                leased.destinations,
                default_url=settings.discord_webhook_url,
                only_unsent=True
            )
            return {
                "delivery_id": leased.delivery_id,
                "event_type": leased.event_type,
                "payload": payload,
                "destinations": [d.model_dump() for d in destinations],
                "worker_id": wid,
                "lease_generation": leased.lease_generation,
                "trigger_type": "redrive" if (leased.attempts and leased.attempts > 0) else "initial",
            }

        # Wait briefly for a new enqueue wakeup or poll interval
        try:
            self._wakeup.clear()
            await asyncio.wait_for(
                self._wakeup.wait(),
                timeout=settings.worker_poll_interval_seconds
            )
        except asyncio.TimeoutError:
            pass
        return None

    async def get_depth(self) -> int:
        return await self.store.count_pending_jobs()


def get_queue_broker(
    delivery_store: Optional[DeliveryStore] = None,
    lease_seconds: Optional[int] = None
) -> BaseQueueBroker:
    """Returns a SQL-backed durable queue broker."""
    return DatabaseQueueBroker(delivery_store=delivery_store, lease_seconds=lease_seconds)
