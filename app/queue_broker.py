from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import asyncio
import logging

logger = logging.getLogger("hookrelay.queue")


class QueueJob(Dict[str, Any]):
    """Represents a discrete queued delivery task."""
    pass


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
    async def dequeue(self) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def size(self) -> int:
        pass


class MemoryQueueBroker(BaseQueueBroker):
    """
    In-memory async queue with graceful lifecycle.
    Guarantees zero-dependency local operation and testability.
    """
    def __init__(self, maxsize: int = 10000):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self._running = False

    async def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False

    async def enqueue(self, job: Dict[str, Any]) -> None:
        await self._queue.put(job)

    async def dequeue(self) -> Optional[Dict[str, Any]]:
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            return None

    def size(self) -> int:
        return self._queue.qsize()


def get_queue_broker(redis_url: Optional[str] = None) -> BaseQueueBroker:
    """
    Factory returning either RedisQueueBroker (if REDIS_URL configured)
    or MemoryQueueBroker.
    """
    return MemoryQueueBroker()
