"""Модели хранения платежей и событий outbox."""

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Enum, Numeric, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from payments_processing.db.base import Base


class Currency(StrEnum):
    """Поддерживаемые валюты платежа."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    """Статусы обработки платежа."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Payment(Base):
    """Платёж и результат его обработки."""

    __tablename__ = "payments"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(Enum(Currency, name="payment_currency"))
    description: Mapped[str] = mapped_column(Text)
    payment_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, default=dict
    )
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(
            PaymentStatus,
            name="payment_status",
            values_callable=lambda enum: [item.value for item in enum],
        ),
        default=PaymentStatus.PENDING,
        server_default=PaymentStatus.PENDING.value,
    )
    idempotency_key: Mapped[str] = mapped_column(Text, unique=True)
    webhook_url: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    processing_attempts: Mapped[int] = mapped_column(default=0, server_default="0")


class Outbox(Base):
    """Событие для публикации в брокер и отметка о его отправке."""

    __tablename__ = "outbox"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    event_type: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    headers: Mapped[dict[str, Any]] = mapped_column(
        JSONB, default=dict, server_default="{}"
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
