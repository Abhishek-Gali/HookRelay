from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Column, String, Integer, Text, DateTime, ForeignKey, Float
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class DeliveryModel(Base):
    """
    SQLAlchemy Table Definition for Deliveries.
    Primary key on delivery_id guarantees database-level idempotency.
    """
    __tablename__ = "deliveries"

    delivery_id = Column(String, primary_key=True, index=True)
    event_type = Column(String, nullable=False, index=True)
    repo = Column(String, nullable=True, index=True)
    status = Column(String, nullable=False, default="received", index=True)  # 'received' | 'sent' | 'failed' | 'dead_letter'
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    payload = Column(Text, nullable=True)  # JSON payload string
    destinations = Column(Text, nullable=True)  # Comma-separated destinations e.g. "discord,slack"
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

    # Relationships
    attempt_history = relationship(
        "DeliveryAttemptModel",
        back_populates="delivery",
        cascade="all, delete-orphan",
        order_by="DeliveryAttemptModel.attempt_number"
    )


class DeliveryAttemptModel(Base):
    """
    Granular attempt records detailing each downstream delivery attempt.
    Enables auditability: Attempt 1 -> 429, Attempt 2 -> 503, Attempt 3 -> 204.
    """
    __tablename__ = "delivery_attempts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    delivery_id = Column(String, ForeignKey("deliveries.delivery_id", ondelete="CASCADE"), nullable=False, index=True)
    destination = Column(String, nullable=False, default="discord")
    attempt_number = Column(Integer, nullable=False)
    http_status = Column(Integer, nullable=True)
    response_time_ms = Column(Float, nullable=True)
    error_message = Column(Text, nullable=True)
    response_headers = Column(Text, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc)
    )

    delivery = relationship("DeliveryModel", back_populates="attempt_history")


class AuditLogModel(Base):
    """
    Security Audit Log recording all administrative operations on /api/* and DLQ.
    """
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    actor_role = Column(String, nullable=False)  # ADMIN, OPERATOR
    action = Column(String, nullable=False)      # redrive_delivery, replay_dlq, discard_dlq
    target_id = Column(String, nullable=True)
    ip_address = Column(String, nullable=True)
    status = Column(String, nullable=False, default="success")
    details = Column(Text, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc)
    )


# Pydantic DTOs for Clean API Responses
class DeliveryAttemptDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt_number: int
    destination: str
    http_status: Optional[int] = None
    response_time_ms: Optional[float] = None
    error_message: Optional[str] = None
    created_at: datetime


class DeliveryDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    delivery_id: str
    event_type: str
    repo: Optional[str] = None
    status: str
    attempts: int
    last_error: Optional[str] = None
    destinations: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    attempt_history: Optional[List[DeliveryAttemptDTO]] = None


class AuditLogDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    actor_role: str
    action: str
    target_id: Optional[str] = None
    ip_address: Optional[str] = None
    status: str
    details: Optional[str] = None
    created_at: datetime
