from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Column, String, Integer, Text, DateTime
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DeliveryModel(Base):
    """
    SQLAlchemy Table Definition for Deliveries.
    Primary key on delivery_id guarantees database-level idempotency.
    """
    __tablename__ = "deliveries"

    delivery_id = Column(String, primary_key=True, index=True)
    event_type = Column(String, nullable=False)
    repo = Column(String, nullable=True)
    status = Column(String, nullable=False, default="received")  # 'received' | 'sent' | 'failed'
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    payload = Column(Text, nullable=True)  # JSON payload string for reconciliation & redrive
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc)
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )


class DeliveryDTO(BaseModel):
    """Pydantic model representing a delivery record for API responses."""
    model_config = ConfigDict(from_attributes=True)

    delivery_id: str
    event_type: str
    repo: Optional[str] = None
    status: str
    attempts: int
    last_error: Optional[str] = None
    created_at: datetime
    updated_at: datetime
