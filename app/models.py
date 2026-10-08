from datetime import datetime, timezone
from typing import Optional, List, Dict, Set
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Column, String, Integer, Text, DateTime, ForeignKey, Float, Index
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

# Formal Delivery State Machine
VALID_STATE_TRANSITIONS: Dict[str, Set[str]] = {
    "received": {"queued", "processing", "sent", "retry_wait", "dead_letter"},
    "queued": {"processing", "sent", "retry_wait", "dead_letter"},
    "processing": {"sent", "retry_wait", "dead_letter", "processing"},  # processing->processing allowed only when expired lease is reclaimed
    "retry_wait": {"processing", "sent", "dead_letter"},
    "dead_letter": {"queued", "processing", "discarded"},
    "sent": set(),       # Terminal state: sent deliveries cannot transition to dead_letter or discarded
    "discarded": set(),  # Terminal state: discarded DLQ items cannot be marked sent without explicit replay first
}


def validate_state_transition(current_status: str, next_status: str) -> bool:
    """Enforces legal delivery state machine transitions."""
    allowed = VALID_STATE_TRANSITIONS.get(current_status, set())
    return next_status in allowed


class DeliveryModel(Base):
    """
    SQLAlchemy Table Definition for Deliveries & Durable Job Queue State.
    Primary key on delivery_id guarantees database-level idempotency.
    Includes atomic lease columns (worker_id, locked_until, next_run_at) so
    multiple workers and reconciliation sweeps never double-deliver.
    """
    __tablename__ = "deliveries"
    __table_args__ = (
        Index("idx_deliveries_reconciliation", "status", "locked_until", "updated_at"),
        Index("idx_deliveries_queue_poll", "status", "next_run_at", "locked_until"),
        Index("idx_deliveries_created", "created_at"),
    )

    delivery_id = Column(String, primary_key=True, index=True)
    event_type = Column(String, nullable=False, index=True)
    repo = Column(String, nullable=True, index=True)
    status = Column(
        String,
        nullable=False,
        default="received",
        index=True
    )  # 'received' | 'queued' | 'processing' | 'retry_wait' | 'sent' | 'dead_letter' | 'discarded'
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    payload = Column(Text, nullable=True)       # JSON payload string
    destinations = Column(Text, nullable=True)  # Structured JSON list: [{"provider": "discord", "url": "..."}]

    # Durable Queue & Atomic Worker Lease columns
    worker_id = Column(String, nullable=True)
    locked_until = Column(DateTime(timezone=True), nullable=True)
    next_run_at = Column(DateTime(timezone=True), nullable=True)

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
    Security Audit Log recording authentication failures, authorization denials,
    and administrative operations on /api/* and DLQ.
    """
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    actor_role = Column(String, nullable=False)  # ADMIN, OPERATOR, VIEWER, UNAUTHENTICATED
    action = Column(String, nullable=False)      # auth_failed, authz_denied, redrive_delivery, discard_dlq, etc.
    target_id = Column(String, nullable=True)
    ip_address = Column(String, nullable=True)
    user_agent = Column(String, nullable=True)
    request_id = Column(String, nullable=True)
    status = Column(String, nullable=False, default="success")
    details = Column(Text, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc)
    )


# Pydantic DTOs for API Responses
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
    worker_id: Optional[str] = None
    locked_until: Optional[datetime] = None
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
    user_agent: Optional[str] = None
    request_id: Optional[str] = None
    status: str
    details: Optional[str] = None
    created_at: datetime
