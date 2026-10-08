import json
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from sqlalchemy import select, update, text, delete
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.config import settings
from app.models import Base, DeliveryModel, DeliveryAttemptModel, AuditLogModel


class DeliveryStore:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.is_sqlite = "sqlite" in database_url
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
        """Create tables if they do not exist."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        """Dispose the engine connection pool."""
        await self.engine.dispose()

    async def claim_delivery(
        self,
        delivery_id: str,
        event_type: str,
        repo: Optional[str] = None,
        payload: Optional[dict] = None,
        destinations: Optional[str] = None
    ) -> bool:
        """
        Atomically claims a delivery ID using INSERT ... ON CONFLICT DO NOTHING RETURNING.
        """
        payload_str = json.dumps(payload) if payload else None
        now = datetime.now(timezone.utc)

        async with self.session_factory() as session:
            if self.is_sqlite:
                stmt = (
                    sqlite_insert(DeliveryModel)
                    .values(
                        delivery_id=delivery_id,
                        event_type=event_type,
                        repo=repo,
                        status="received",
                        attempts=0,
                        payload=payload_str,
                        destinations=destinations,
                        created_at=now,
                        updated_at=now
                    )
                    .on_conflict_do_nothing(index_elements=["delivery_id"])
                    .returning(DeliveryModel.delivery_id)
                )
            else:
                stmt = (
                    pg_insert(DeliveryModel)
                    .values(
                        delivery_id=delivery_id,
                        event_type=event_type,
                        repo=repo,
                        status="received",
                        attempts=0,
                        payload=payload_str,
                        destinations=destinations,
                        created_at=now,
                        updated_at=now
                    )
                    .on_conflict_do_nothing(index_elements=["delivery_id"])
                    .returning(DeliveryModel.delivery_id)
                )

            result = await session.execute(stmt)
            await session.commit()
            claimed_row = result.scalar_one_or_none()
            return claimed_row is not None

    async def record_attempt(
        self,
        delivery_id: str,
        destination: str,
        attempt_number: int,
        http_status: Optional[int],
        response_time_ms: float,
        error_message: Optional[str] = None,
        response_headers: Optional[str] = None
    ) -> None:
        """Records a granular delivery attempt for auditability and observability."""
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            attempt = DeliveryAttemptModel(
                delivery_id=delivery_id,
                destination=destination,
                attempt_number=attempt_number,
                http_status=http_status,
                response_time_ms=response_time_ms,
                error_message=error_message[:2000] if error_message else None,
                response_headers=response_headers,
                created_at=now
            )
            session.add(attempt)
            await session.commit()

    async def mark_sent(self, delivery_id: str, attempts: int) -> None:
        """Marks a delivery as successfully dispatched."""
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            stmt = (
                update(DeliveryModel)
                .where(DeliveryModel.delivery_id == delivery_id)
                .values(
                    status="sent",
                    attempts=attempts,
                    last_error=None,
                    updated_at=now
                )
            )
            await session.execute(stmt)
            await session.commit()

    async def mark_failed_or_dlq(self, delivery_id: str, attempts: int, error: str) -> None:
        """Moves delivery to dead_letter (DLQ) after retries are exhausted."""
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            stmt = (
                update(DeliveryModel)
                .where(DeliveryModel.delivery_id == delivery_id)
                .values(
                    status="dead_letter",
                    attempts=attempts,
                    last_error=error[:2000],
                    updated_at=now
                )
            )
            await session.execute(stmt)
            await session.commit()

    async def mark_failed(self, delivery_id: str, attempts: int, error: str) -> None:
        """Alias for mark_failed_or_dlq for test compatibility."""
        await self.mark_failed_or_dlq(delivery_id, attempts, error)

    async def get_delivery(self, delivery_id: str, include_attempts: bool = True) -> Optional[DeliveryModel]:
        """Fetch delivery by ID, optionally loading its attempt history."""
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
        """Discards an item from the DLQ by setting status to 'discarded'."""
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            stmt = (
                update(DeliveryModel)
                .where(DeliveryModel.delivery_id == delivery_id, DeliveryModel.status == "dead_letter")
                .values(status="discarded", updated_at=now)
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0

    async def fetch_stale_deliveries(self, older_than_seconds: int = 120) -> List[DeliveryModel]:
        """Finds deliveries stuck in 'received' status for crash reconciliation."""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)
        async with self.session_factory() as session:
            stmt = (
                select(DeliveryModel)
                .where(
                    DeliveryModel.status == "received",
                    DeliveryModel.updated_at < cutoff
                )
                .limit(20)
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def record_audit_log(
        self,
        actor_role: str,
        action: str,
        target_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        status: str = "success",
        details: Optional[str] = None
    ) -> None:
        """Appends an administrative operation to the audit log."""
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            log_entry = AuditLogModel(
                actor_role=actor_role,
                action=action,
                target_id=target_id,
                ip_address=ip_address,
                status=status,
                details=details,
                created_at=now
            )
            session.add(log_entry)
            await session.commit()

    async def list_audit_logs(self, limit: int = 50) -> List[AuditLogModel]:
        """Fetch recent security audit logs."""
        async with self.session_factory() as session:
            stmt = select(AuditLogModel).order_by(AuditLogModel.created_at.desc()).limit(limit)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_stats(self) -> Dict[str, Any]:
        """Returns aggregated operational counts for dashboard."""
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
