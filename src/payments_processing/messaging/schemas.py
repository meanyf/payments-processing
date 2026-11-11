"""Схемы сообщений RabbitMQ."""

from uuid import UUID

from pydantic import BaseModel


class PaymentEvent(BaseModel):
    """Событие с идентификатором платежа для обработки."""

    payment_id: UUID
