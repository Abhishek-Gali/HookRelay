import asyncio
import hashlib
import json
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Union
from sqlalchemy import select, update, delete, text, or_, and_
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


class StaleWorkerLeaseError(RuntimeError):
    """
    Raised when a worker attempts to mutate a delivery after its lease expired
    or was superseded by a newer fencing token (lease_generation).
    """
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
        """Serializes destinations as structured JSON list of dicts with per-destination status."""
        if destinations is None:
            return None
        if isinstance(destinations, str):
            return destinations
        serialized = []
        for d in destinations:
            if hasattr(d, "model_dump"):
                item = d.model_dump()
            elif isinstance(d, dict):
                item = dict(d)
            else:
                continue
            item.setdefault("status", "pending")
            serialized.append(item)
        return json.dumps(serialized)

    async def claim_delivery(
        self,
        delivery_id: str,
        event_type: str,
        repo: Optional[str] = None,
        payload: Optional[dict] = None,
        destinations: Optional[Union[str, List[Any]]] = None,
        raw_body: Optional[bytes] = None,
        replay_window_seconds: Optional[int] = None
    ) -> bool:
        """
        Atomically claims a delivery ID using INSERT ... ON CONFLICT DO NOTHING RETURNING.
        Also enforces bounded payload replay protection (HR-04): if an identical signed body
        (payload_hash) for the same event_type was already ingested within replay_window_seconds
        under a different X-GitHub-Delivery header, rejects it as a replay.
        """
        payload_str = json.dumps(payload, sort_keys=True) if payload is not None else None
        if raw_body is not None:
            payload_hash = hashlib.sha256(raw_body).hexdigest() if raw_body else None
        elif payload_str is not None:
            payload_hash = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
        else:
            payload_hash = None

        dest_str = self.serialize_destinations(destinations)
        now = datetime.now(timezone.utc)

        values = dict(
            delivery_id=delivery_id,
            event_type=event_type,
            repo=repo,
            status="received",
            attempts=0,
            payload=payload_str,
            payload_hash=payload_hash,
            destinations=dest_str,
            worker_id=None,
            lease_generation=0,
            locked_until=None,
            next_run_at=now,
            created_at=now,
            updated_at=now,
        )

        async with self._db_lock:
            async with self.session_factory() as session:
                # HR-04: Bounded replay check across distinct X-GitHub-Delivery IDs
                if replay_window_seconds and replay_window_seconds > 0 and payload_hash:
                    replay_cutoff = now - timedelta(seconds=replay_window_seconds)
                    replay_stmt = (
                        select(DeliveryModel.delivery_id)
                        .where(
                            DeliveryModel.event_type == event_type,
                            DeliveryModel.payload_hash == payload_hash,
                            DeliveryModel.created_at >= replay_cutoff
                        )
                        .limit(1)
                    )
                    replay_res = await session.execute(replay_stmt)
                    if replay_res.scalar_one_or_none() is not None:
                        return False

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
        Atomically acquires an exclusive processing lease on a delivery row and increments
        its monotonic fencing token (lease_generation = lease_generation + 1).
        Guarantees that two workers (or reconciliation sweeps) can NEVER process
        the same delivery concurrently, and stale workers are fenced out.
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
                        lease_generation=DeliveryModel.lease_generation + 1,
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
        lease_seconds: int = 120,
        lease_generation: Optional[int] = None
    ) -> bool:
        """
        Extends an active worker lease (heartbeat) conditional on worker_id and
        lease_generation fencing token. Returns False if another worker reclaimed the job.
        """
        now = datetime.now(timezone.utc)
        lease_until = now + timedelta(seconds=lease_seconds)
        conditions = [
            DeliveryModel.delivery_id == delivery_id,
            DeliveryModel.worker_id == worker_id,
            DeliveryModel.status == "processing",
        ]
        if lease_generation is not None:
            conditions.append(DeliveryModel.lease_generation == lease_generation)

        async with self._db_lock:
            async with self.session_factory() as session:
                stmt = (
                    update(DeliveryModel)
                    .where(*conditions)
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
        Polls the durable SQL table for the next runnable job and atomically leases it
        with an incremented fencing token (lease_generation).
        Uses FOR UPDATE SKIP LOCKED on PostgreSQL to eliminate row lock contention across replicas.
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
                    row.lease_generation = (row.lease_generation or 0) + 1
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

    async def update_destination_status(
        self,
        delivery_id: str,
        provider: str,
        url: str,
        dest_status: str,
        worker_id: Optional[str] = None,
        lease_generation: Optional[int] = None
    ) -> bool:
        """
        Records per-destination delivery state ('sent' or 'failed') inside deliveries.destinations
        conditional on active worker lease ownership (HR-06).
        Prevents duplicate sends to already-successful destinations during partial-failure redrives.
        """
        now = datetime.now(timezone.utc)
        async with self._db_lock:
            async with self.session_factory() as session:
                row_res = await session.execute(
                    select(DeliveryModel).where(DeliveryModel.delivery_id == delivery_id)
                )
                row = row_res.scalar_one_or_none()
                if not row:
                    return False

                if worker_id is not None and row.worker_id != worker_id:
                    return False
                if lease_generation is not None and row.lease_generation != lease_generation:
                    return False

                dest_list = []
                if row.destinations:
                    try:
                        parsed = json.loads(row.destinations)
                        if isinstance(parsed, list):
                            dest_list = parsed
                    except Exception:
                        dest_list = []

                updated = False
                for item in dest_list:
                    if isinstance(item, dict) and item.get("provider") == provider and item.get("url") == url:
                        item["status"] = dest_status
                        updated = True

                if not updated:
                    dest_list.append({"provider": provider, "url": url, "status": dest_status})

                stmt = (
                    update(DeliveryModel)
                    .where(DeliveryModel.delivery_id == delivery_id)
                    .values(destinations=json.dumps(dest_list), updated_at=now)
                )
                await session.execute(stmt)
                await session.commit()
                return True

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
        extra_values: Dict[str, Any],
        worker_id: Optional[str] = None,
        lease_generation: Optional[int] = None
    ) -> None:
        """
        Validates and executes a state machine transition on a delivery record.
        When worker_id or lease_generation are supplied, enforces ownership and fencing
        token checks in SQL (HR-02, HR-03) and raises StaleWorkerLeaseError if superseded.
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

                if worker_id is not None or lease_generation is not None:
                    if (
                        row.status != "processing"
                        or (worker_id is not None and row.worker_id != worker_id)
                        or (lease_generation is not None and row.lease_generation != lease_generation)
                    ):
                        raise StaleWorkerLeaseError(
                            f"Fenced out: worker '{worker_id}' (gen={lease_generation}) no longer owns "
                            f"delivery '{delivery_id}' (owner='{row.worker_id}', gen={row.lease_generation}, status='{row.status}')."
                        )

                if not validate_state_transition(row.status, target_status):
                    raise IllegalStateTransitionError(
                        f"Illegal delivery state transition for '{delivery_id}': '{row.status}' -> '{target_status}'"
                    )

                conditions = [DeliveryModel.delivery_id == delivery_id]
                if worker_id is not None:
                    conditions.append(DeliveryModel.worker_id == worker_id)
                if lease_generation is not None:
                    conditions.append(DeliveryModel.lease_generation == lease_generation)

                values = {
                    "status": target_status,
                    "updated_at": now,
                    **extra_values
                }
                stmt = (
                    update(DeliveryModel)
                    .where(*conditions)
                    .values(**values)
                    .returning(DeliveryModel.delivery_id)
                )
                res = await session.execute(stmt)
                updated_id = res.scalar_one_or_none()
                await session.commit()
                if updated_id is None:
                    raise StaleWorkerLeaseError(
                        f"Concurrent fencing failure for '{delivery_id}' (worker='{worker_id}', gen={lease_generation})."
                    )

    async def mark_sent(
        self,
        delivery_id: str,
        attempts: int,
        worker_id: Optional[str] = None,
        lease_generation: Optional[int] = None
    ) -> None:
        """Marks a delivery as successfully dispatched (terminal state), conditional on lease ownership."""
        await self._transition_state(
            delivery_id=delivery_id,
            target_status="sent",
            extra_values={
                "attempts": attempts,
                "last_error": None,
                "locked_until": None,
                "worker_id": None,
                "next_run_at": None,
            },
            worker_id=worker_id,
            lease_generation=lease_generation
        )

    async def mark_retry_wait(
        self,
        delivery_id: str,
        attempts: int,
        error: str,
        backoff_seconds: float,
        worker_id: Optional[str] = None,
        lease_generation: Optional[int] = None
    ) -> None:
        """Marks a delivery as waiting for next retry backoff window, conditional on lease ownership."""
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
            },
            worker_id=worker_id,
            lease_generation=lease_generation
        )

    async def mark_failed_or_dlq(
        self,
        delivery_id: str,
        attempts: int,
        error: str,
        worker_id: Optional[str] = None,
        lease_generation: Optional[int] = None
    ) -> None:
        """Moves delivery to dead_letter (DLQ) after retries are exhausted, conditional on lease ownership."""
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
            },
            worker_id=worker_id,
            lease_generation=lease_generation
        )

    async def mark_failed(
        self,
        delivery_id: str,
        attempts: int,
        error: str,
        worker_id: Optional[str] = None,
        lease_generation: Optional[int] = None
    ) -> None:
        """Alias for mark_failed_or_dlq."""
        await self.mark_failed_or_dlq(
            delivery_id,
            attempts,
            error,
            worker_id=worker_id,
            lease_generation=lease_generation
        )

    async def prepare_for_redrive(
        self,
        delivery_id: str,
        new_destinations: Optional[Union[str, List[Any]]] = None
    ) -> DeliveryModel:
        """
        Transitions a dead_letter (or received/queued) delivery back to 'queued' for replay.
        Preserves original persisted destinations (including per-destination 'sent' status)
        unless new_destinations is explicitly supplied.
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

    async def enforce_retention_policy(
        self,
        payload_days: Optional[int] = None,
        attempt_days: Optional[int] = None,
        audit_days: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Enforces configurable data retention policies (HR-09):
        1. Scrubs raw webhook payloads (SET payload = NULL) on terminal 'sent'/'discarded'
           deliveries older than payload_days (retaining delivery_id for idempotency).
        2. Deletes delivery_attempts older than attempt_days.
        3. Deletes audit_logs older than audit_days.
        """
        now = datetime.now(timezone.utc)
        eff_payload_days = payload_days if payload_days is not None else settings.payload_retention_days
        eff_attempt_days = attempt_days if attempt_days is not None else settings.attempt_retention_days
        eff_audit_days = audit_days if audit_days is not None else settings.audit_log_retention_days

        payload_cutoff = now - timedelta(days=eff_payload_days)
        attempt_cutoff = now - timedelta(days=eff_attempt_days)
        audit_cutoff = now - timedelta(days=eff_audit_days)

        async with self._db_lock:
            async with self.session_factory() as session:
                res_payloads = await session.execute(
                    update(DeliveryModel)
                    .where(
                        DeliveryModel.status.in_(["sent", "discarded"]),
                        DeliveryModel.payload.is_not(None),
                        DeliveryModel.updated_at <= payload_cutoff
                    )
                    .values(payload=None)
                )
                res_attempts = await session.execute(
                    delete(DeliveryAttemptModel).where(DeliveryAttemptModel.created_at <= attempt_cutoff)
                )
                res_audits = await session.execute(
                    delete(AuditLogModel).where(AuditLogModel.created_at <= audit_cutoff)
                )
                await session.commit()
                return {
                    "scrubbed_payloads": res_payloads.rowcount or 0,
                    "deleted_attempts": res_attempts.rowcount or 0,
                    "deleted_audit_logs": res_audits.rowcount or 0,
                }

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

                res_pending = await session.execute(
                    select(text("count(*) from deliveries where status in ('received', 'queued', 'retry_wait', 'processing')"))
                )
                pending = res_pending.scalar() or 0

                return {
                    "total_deliveries": total,
                    "sent_deliveries": sent,
                    "dead_letter_queue_count": dlq,
                    "queue_depth": pending,
                    "total_attempts": attempts,
                    "by_status": {
                        "sent": sent,
                        "dead_letter": dlq,
                    },
                    "success_rate_percent": round((sent / total * 100), 2) if total > 0 else 100.0
                }


store = DeliveryStore(settings.database_url)
