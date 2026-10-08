import json
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from sqlalchemy import select, update, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.config import settings
from app.models import Base, DeliveryModel, DeliveryDTO


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
        payload: Optional[dict] = None
    ) -> bool:
        """
        Atomically claims a delivery ID.
        Uses INSERT ... ON CONFLICT (delivery_id) DO NOTHING RETURNING delivery_id.
        If a row is returned, this worker claimed the delivery (True).
        If no row is returned, the delivery was already seen (False: duplicate).
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

    async def mark_sent(self, delivery_id: str, attempts: int) -> None:
        """Marks a delivery as successfully sent to Discord."""
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

    async def mark_failed(self, delivery_id: str, attempts: int, error: str) -> None:
        """Marks a delivery as permanently failed after exhausting retries."""
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            stmt = (
                update(DeliveryModel)
                .where(DeliveryModel.delivery_id == delivery_id)
                .values(
                    status="failed",
                    attempts=attempts,
                    last_error=error[:2000],  # safeguard max error text
                    updated_at=now
                )
            )
            await session.execute(stmt)
            await session.commit()

    async def get_delivery(self, delivery_id: str) -> Optional[DeliveryModel]:
        """Fetch delivery by its delivery_id."""
        async with self.session_factory() as session:
            stmt = select(DeliveryModel).where(DeliveryModel.delivery_id == delivery_id)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def list_deliveries(
        self,
        status: Optional[str] = None,
        limit: int = 50
    ) -> List[DeliveryModel]:
        """List recent deliveries with optional status filter."""
        async with self.session_factory() as session:
            stmt = select(DeliveryModel).order_by(DeliveryModel.created_at.desc()).limit(limit)
            if status:
                stmt = stmt.where(DeliveryModel.status == status)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def fetch_stale_deliveries(self, older_than_seconds: int = 120) -> List[DeliveryModel]:
        """
        Finds deliveries stuck in 'received' status older than the specified threshold.
        This provides the data source for crash recovery reconciliation.
        """
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


# Global default store instance
store = DeliveryStore(settings.database_url)
