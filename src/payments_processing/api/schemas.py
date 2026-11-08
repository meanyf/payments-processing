"""Схемы запросов и ответов API платежей."""

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl

from payments_processing.db.models import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    """Данные для создания платежа."""

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentCreated(BaseModel):
    """Подтверждение принятия платежа в обработку."""

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetails(PaymentCreated):
    """Подробная информация о платеже и результате его обработки."""

    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: HttpUrl
    idempotency_key: str
    processed_at: datetime | None
