import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Union
from sqlalchemy import select, update, text, or_, and_
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.config import settings
from app.models import (
    Base,
    DeliveryModel,
    DeliveryAttemptModel,
    AuditLogModel,
    validate_state_transition,
)


class IllegalStateTransitionError(ValueError):
    """Raised when attempting an invalid delivery state transition."""
    pass


class DeliveryStore:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.is_sqlite = "sqlite" in database_url
        self._db_lock = asyncio.Lock()
        self.engine = create_async_engine(
            self.database_url,
            echo=False,
            future=True
        )
        self.session_factory = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

    async def init_db(self) -> None:
        """Create tables and indexes if they do not exist."""
        async with self._db_lock:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        """Dispose the engine connection pool."""
        async with self._db_lock:
            await self.engine.dispose()

    async def check_health(self) -> bool:
        """Actively verifies database connectivity with SELECT 1."""
        try:
            async with self._db_lock:
                async with self.session_factory() as session:
                    res = await session.execute(select(text("1")))
                    return res.scalar() == 1
        except Exception:
            return False

    @staticmethod
    def serialize_destinations(destinations: Optional[Union[str, List[Any]]]) -> Optional[str]:
        """Serializes destinations as structured JSON list of dicts."""
        if destinations is None:
            return None
        if isinstance(destinations, str):
            return destinations
        serialized = []
        for d in destinations:
            if hasattr(d, "model_dump"):
                serialized.append(d.model_dump())
            elif isinstance(d, dict):
                serialized.append(d)
        return json.dumps(serialized)

    async def claim_delivery(
        self,
        delivery_id: str,
        event_type: str,
        repo: Optional[str] = None,
        payload: Optional[dict] = None,
        destinations: Optional[Union[str, List[Any]]] = None
    ) -> bool:
        """
        Atomically claims a delivery ID using INSERT ... ON CONFLICT DO NOTHING RETURNING.
        Persists both the payload and structured destinations JSON for durable queueing.
        """
        payload_str = json.dumps(payload) if payload is not None else None
        dest_str = self.serialize_destinations(destinations)
        now = datetime.now(timezone.utc)

        values = dict(
            delivery_id=delivery_id,
            event_type=event_type,
            repo=repo,
            status="received",
            attempts=0,
            payload=payload_str,
            destinations=dest_str,
            worker_id=None,
            locked_until=None,
            next_run_at=now,
            created_at=now,
            updated_at=now,
        )

        async with self._db_lock:
            async with self.session_factory() as session:
                insert_fn = sqlite_insert if self.is_sqlite else pg_insert
                stmt = (
                    insert_fn(DeliveryModel)
                    .values(**values)
                    .on_conflict_do_nothing(index_elements=["delivery_id"])
                    .returning(DeliveryModel.delivery_id)
                )
                result = await session.execute(stmt)
                claimed_row = result.scalar_one_or_none()
                await session.commit()
                return claimed_row is not None

    async def acquire_lease(
        self,
        delivery_id: str,
        worker_id: str,
        lease_seconds: int = 120,
        require_stale_before: Optional[datetime] = None
    ) -> Optional[DeliveryModel]:
        """
        Atomically acquires an exclusive processing lease on a delivery row.
        Guarantees that two workers (or reconciliation sweeps) can NEVER process
        the same delivery concurrently.
        Returns the leased DeliveryModel if won, or None if another worker claimed it.
        """
        now = datetime.now(timezone.utc)
        lease_until = now + timedelta(seconds=lease_seconds)

        lease_expired_cond = or_(
            DeliveryModel.locked_until.is_(None),
            DeliveryModel.locked_until <= now
        )

        if require_stale_before is not None:
            eligibility_cond = and_(
                or_(
                    and_(
                        DeliveryModel.status.in_(["received", "queued", "retry_wait"]),
                        DeliveryModel.updated_at <= require_stale_before,
                        lease_expired_cond,
                    ),
                    and_(
                        DeliveryModel.status == "processing",
                        DeliveryModel.locked_until <= now,
                    )
                )
            )
        else:
            eligibility_cond = or_(
                and_(
                    DeliveryModel.status.in_(["received", "queued", "retry_wait"]),
                    lease_expired_cond,
                    or_(
                        DeliveryModel.next_run_at.is_(None),
                        DeliveryModel.next_run_at <= now
                    )
                ),
                and_(
                    DeliveryModel.status == "processing",
                    DeliveryModel.locked_until.is_not(None),
                    DeliveryModel.locked_until <= now
                )
            )

        async with self._db_lock:
            async with self.session_factory() as session:
                stmt = (
                    update(DeliveryModel)
                    .where(
                        DeliveryModel.delivery_id == delivery_id,
                        eligibility_cond
                    )
                    .values(
                        status="processing",
                        worker_id=worker_id,
                        locked_until=lease_until,
                        updated_at=now
                    )
                    .returning(DeliveryModel.delivery_id)
                )
                res = await session.execute(stmt)
                won_id = res.scalar_one_or_none()
                await session.commit()
                if not won_id:
                    return None

                row_res = await session.execute(
                    select(DeliveryModel).where(DeliveryModel.delivery_id == delivery_id)
                )
                return row_res.scalar_one_or_none()

    async def renew_lease(
        self,
        delivery_id: str,
        worker_id: str,
        lease_seconds: int = 120
    ) -> bool:
        """
        Extends an active worker lease (heartbeat) so long-running deliveries or
        rate-limit Retry-After waits never expire while the worker is still alive.
        """
        now = datetime.now(timezone.utc)
        lease_until = now + timedelta(seconds=lease_seconds)
        async with self._db_lock:
            async with self.session_factory() as session:
                stmt = (
                    update(DeliveryModel)
                    .where(
                        DeliveryModel.delivery_id == delivery_id,
                        DeliveryModel.worker_id == worker_id,
                        DeliveryModel.status == "processing"
                    )
                    .values(
                        locked_until=lease_until,
                        updated_at=now
                    )
                    .returning(DeliveryModel.delivery_id)
                )
                res = await session.execute(stmt)
                renewed_id = res.scalar_one_or_none()
                await session.commit()
                return renewed_id is not None

    async def claim_next_runnable_job(
        self,
        worker_id: str,
        lease_seconds: int = 120
    ) -> Optional[DeliveryModel]:
        """
        Polls the durable SQL table for the next runnable job and atomically leases it.
        Uses FOR UPDATE SKIP LOCKED on PostgreSQL to eliminate row lock contention across replicas.
        Survives process restarts without losing queued jobs.
        """
        now = datetime.now(timezone.utc)
        eligibility_cond = or_(
            and_(
                DeliveryModel.status.in_(["received", "queued", "retry_wait"]),
                or_(
                    DeliveryModel.locked_until.is_(None),
                    DeliveryModel.locked_until <= now
                ),
                or_(
                    DeliveryModel.next_run_at.is_(None),
                    DeliveryModel.next_run_at <= now
                )
            ),
            and_(
                DeliveryModel.status == "processing",
                DeliveryModel.locked_until.is_not(None),
                DeliveryModel.locked_until <= now
            )
        )

        if not self.is_sqlite:
            # High-concurrency PostgreSQL path using native FOR UPDATE SKIP LOCKED
            lease_until = now + timedelta(seconds=lease_seconds)
            async with self._db_lock:
                async with self.session_factory() as session:
                    stmt = (
                        select(DeliveryModel)
                        .where(eligibility_cond)
                        .order_by(DeliveryModel.created_at.asc())
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                    res = await session.execute(stmt)
                    row = res.scalar_one_or_none()
                    if row is None:
                        return None
                    row.status = "processing"
                    row.worker_id = worker_id
                    row.locked_until = lease_until
                    row.updated_at = now
                    await session.commit()
                    return row

        async with self._db_lock:
            async with self.session_factory() as session:
                stmt = (
                    select(DeliveryModel.delivery_id)
                    .where(eligibility_cond)
                    .order_by(DeliveryModel.created_at.asc())
                    .limit(5)
                )
                res = await session.execute(stmt)
                candidate_ids = list(res.scalars().all())

        for cid in candidate_ids:
            leased = await self.acquire_lease(cid, worker_id=worker_id, lease_seconds=lease_seconds)
            if leased is not None:
                return leased
        return None

    async def record_attempt(
        self,
        delivery_id: str,
        destination: str,
        attempt_number: int,
        http_status: Optional[int],
        response_time_ms: float,
        error_message: Optional[str] = None,
        response_headers: Optional[str] = None,
        trigger_type: str = "initial"
    ) -> None:
        """Records a granular delivery attempt for auditability and observability."""
        from app.providers import sanitize_error_message
        now = datetime.now(timezone.utc)
        bounded_err = sanitize_error_message(error_message) if error_message else None
        async with self._db_lock:
            async with self.session_factory() as session:
                attempt = DeliveryAttemptModel(
                    delivery_id=delivery_id,
                    destination=destination,
                    attempt_number=attempt_number,
                    trigger_type=trigger_type,
                    http_status=http_status,
                    response_time_ms=response_time_ms,
                    error_message=bounded_err,
                    response_headers=response_headers[:512] if response_headers else None,
                    created_at=now
                )
                session.add(attempt)
                await session.commit()

    async def _transition_state(
        self,
        delivery_id: str,
        target_status: str,
        extra_values: Dict[str, Any]
    ) -> None:
        """Validates and executes a state machine transition on a delivery record."""
        now = datetime.now(timezone.utc)
        async with self._db_lock:
            async with self.session_factory() as session:
                row_res = await session.execute(
                    select(DeliveryModel).where(DeliveryModel.delivery_id == delivery_id)
                )
                row = row_res.scalar_one_or_none()
                if not row:
                    raise ValueError(f"Delivery '{delivery_id}' not found.")

                if not validate_state_transition(row.status, target_status):
                    raise IllegalStateTransitionError(
                        f"Illegal delivery state transition for '{delivery_id}': '{row.status}' -> '{target_status}'"
                    )

                values = {
                    "status": target_status,
                    "updated_at": now,
                    **extra_values
                }
                stmt = (
                    update(DeliveryModel)
                    .where(DeliveryModel.delivery_id == delivery_id)
                    .values(**values)
                )
                await session.execute(stmt)
                await session.commit()

    async def mark_sent(self, delivery_id: str, attempts: int) -> None:
        """Marks a delivery as successfully dispatched (terminal state)."""
        await self._transition_state(
            delivery_id=delivery_id,
            target_status="sent",
            extra_values={
                "attempts": attempts,
                "last_error": None,
                "locked_until": None,
                "worker_id": None,
                "next_run_at": None,
            }
        )

    async def mark_retry_wait(
        self,
        delivery_id: str,
        attempts: int,
        error: str,
        backoff_seconds: float
    ) -> None:
        """Marks a delivery as waiting for next retry backoff window."""
        from app.providers import sanitize_error_message
        now = datetime.now(timezone.utc)
        await self._transition_state(
            delivery_id=delivery_id,
            target_status="retry_wait",
            extra_values={
                "attempts": attempts,
                "last_error": sanitize_error_message(error) or "Retry scheduled",
                "locked_until": None,
                "worker_id": None,
                "next_run_at": now + timedelta(seconds=backoff_seconds),
            }
        )

    async def mark_failed_or_dlq(self, delivery_id: str, attempts: int, error: str) -> None:
        """Moves delivery to dead_letter (DLQ) after retries are exhausted."""
        from app.providers import sanitize_error_message
        await self._transition_state(
            delivery_id=delivery_id,
            target_status="dead_letter",
            extra_values={
                "attempts": attempts,
                "last_error": sanitize_error_message(error) or "Exhausted retries",
                "locked_until": None,
                "worker_id": None,
                "next_run_at": None,
            }
        )

    async def mark_failed(self, delivery_id: str, attempts: int, error: str) -> None:
        """Alias for mark_failed_or_dlq."""
        await self.mark_failed_or_dlq(delivery_id, attempts, error)

    async def prepare_for_redrive(
        self,
        delivery_id: str,
        new_destinations: Optional[Union[str, List[Any]]] = None
    ) -> DeliveryModel:
        """
        Transitions a dead_letter (or received/queued) delivery back to 'queued' for replay.
        Preserves original persisted destinations unless new_destinations is explicitly supplied.
        """
        now = datetime.now(timezone.utc)
        async with self._db_lock:
            async with self.session_factory() as session:
                row_res = await session.execute(
                    select(DeliveryModel).where(DeliveryModel.delivery_id == delivery_id)
                )
                row = row_res.scalar_one_or_none()
                if not row:
                    raise ValueError(f"Delivery '{delivery_id}' not found.")
                if row.status in ("sent", "discarded"):
                    raise IllegalStateTransitionError(
                        f"Cannot redrive delivery '{delivery_id}' from terminal status '{row.status}'."
                    )

                values: Dict[str, Any] = {
                    "status": "queued",
                    "locked_until": None,
                    "worker_id": None,
                    "next_run_at": now,
                    "updated_at": now,
                }
                if new_destinations is not None:
                    values["destinations"] = self.serialize_destinations(new_destinations)

                await session.execute(
                    update(DeliveryModel)
                    .where(DeliveryModel.delivery_id == delivery_id)
                    .values(**values)
                )
                await session.commit()

                refreshed = await session.execute(
                    select(DeliveryModel).where(DeliveryModel.delivery_id == delivery_id)
                )
                return refreshed.scalar_one()

    async def get_delivery(self, delivery_id: str, include_attempts: bool = True) -> Optional[DeliveryModel]:
        """Fetch delivery by ID, optionally loading its attempt history."""
        async with self._db_lock:
            async with self.session_factory() as session:
                stmt = select(DeliveryModel).where(DeliveryModel.delivery_id == delivery_id)
                if include_attempts:
                    stmt = stmt.options(selectinload(DeliveryModel.attempt_history))
                result = await session.execute(stmt)
                return result.scalar_one_or_none()

    async def list_deliveries(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[DeliveryModel]:
        """List deliveries with status filtering and pagination."""
        async with self._db_lock:
            async with self.session_factory() as session:
                stmt = (
                    select(DeliveryModel)
                    .options(selectinload(DeliveryModel.attempt_history))
                    .order_by(DeliveryModel.created_at.desc())
                    .limit(limit)
                    .offset(offset)
                )
                if status:
                    stmt = stmt.where(DeliveryModel.status == status)
                result = await session.execute(stmt)
                return list(result.scalars().all())

    async def fetch_dlq(self, limit: int = 50) -> List[DeliveryModel]:
        """Fetch deliveries in dead_letter status."""
        return await self.list_deliveries(status="dead_letter", limit=limit)

    async def discard_dlq(self, delivery_id: str) -> bool:
        """Discards an item from the DLQ by transitioning dead_letter -> discarded."""
        now = datetime.now(timezone.utc)
        async with self._db_lock:
            async with self.session_factory() as session:
                stmt = (
                    update(DeliveryModel)
                    .where(
                        DeliveryModel.delivery_id == delivery_id,
                        DeliveryModel.status == "dead_letter"
                    )
                    .values(
                        status="discarded",
                        locked_until=None,
                        worker_id=None,
                        updated_at=now
                    )
                )
                result = await session.execute(stmt)
                rowcount = result.rowcount or 0
                await session.commit()
                return rowcount > 0

    async def fetch_stale_deliveries(
        self,
        older_than_seconds: int = 120,
        batch_size: int = 100
    ) -> List[DeliveryModel]:
        """
        Finds deliveries stuck in 'received'/'queued'/'retry_wait' older than cutoff,
        OR stuck in 'processing' with an expired worker lease.
        """
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=older_than_seconds)
        async with self._db_lock:
            async with self.session_factory() as session:
                stmt = (
                    select(DeliveryModel)
                    .where(
                        or_(
                            and_(
                                DeliveryModel.status.in_(["received", "queued", "retry_wait"]),
                                DeliveryModel.updated_at <= cutoff,
                                or_(
                                    DeliveryModel.locked_until.is_(None),
                                    DeliveryModel.locked_until <= now
                                )
                            ),
                            and_(
                                DeliveryModel.status == "processing",
                                DeliveryModel.locked_until.is_not(None),
                                DeliveryModel.locked_until <= now
                            )
                        )
                    )
                    .order_by(DeliveryModel.updated_at.asc())
                    .limit(batch_size)
                )
                result = await session.execute(stmt)
                return list(result.scalars().all())

    async def count_pending_jobs(self) -> int:
        """Returns count of deliveries currently queued/received/retry_wait."""
        async with self._db_lock:
            async with self.session_factory() as session:
                res = await session.execute(
                    select(text("count(*) from deliveries where status in ('received', 'queued', 'retry_wait', 'processing')"))
                )
                return res.scalar() or 0

    async def record_audit_log(
        self,
        actor_role: str,
        action: str,
        target_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        request_id: Optional[str] = None,
        status: str = "success",
        details: Optional[str] = None
    ) -> None:
        """Appends a security or administrative event to the immutable audit log."""
        now = datetime.now(timezone.utc)
        async with self._db_lock:
            async with self.session_factory() as session:
                log_entry = AuditLogModel(
                    actor_role=actor_role,
                    action=action,
                    target_id=target_id,
                    ip_address=ip_address,
                    user_agent=user_agent[:512] if user_agent else None,
                    request_id=request_id,
                    status=status,
                    details=details[:1024] if details else None,
                    created_at=now
                )
                session.add(log_entry)
                await session.commit()

    async def list_audit_logs(self, limit: int = 50) -> List[AuditLogModel]:
        """Fetch recent security audit logs."""
        async with self._db_lock:
            async with self.session_factory() as session:
                stmt = select(AuditLogModel).order_by(AuditLogModel.created_at.desc()).limit(limit)
                result = await session.execute(stmt)
                return list(result.scalars().all())

    async def get_stats(self) -> Dict[str, Any]:
        """Returns aggregated operational counts for dashboard."""
        async with self._db_lock:
            async with self.session_factory() as session:
                res_total = await session.execute(select(text("count(*) from deliveries")))
                total = res_total.scalar() or 0

                res_sent = await session.execute(select(text("count(*) from deliveries where status='sent'")))
                sent = res_sent.scalar() or 0

                res_dlq = await session.execute(select(text("count(*) from deliveries where status='dead_letter'")))
                dlq = res_dlq.scalar() or 0

                res_attempts = await session.execute(select(text("count(*) from delivery_attempts")))
                attempts = res_attempts.scalar() or 0

                return {
                    "total_deliveries": total,
                    "sent_deliveries": sent,
                    "dlq_deliveries": dlq,
                    "total_attempts": attempts,
                    "success_rate": round((sent / total * 100), 1) if total > 0 else 100.0
                }


# Default global store instance
store = DeliveryStore(settings.database_url)
