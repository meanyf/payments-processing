"""Получение событий о новых платежах."""

import logging
from uuid import UUID

import httpx
from faststream import AckPolicy
from pydantic import BaseModel

from payments_processing.db.session import session_factory
from payments_processing.messaging.broker import broker, payments_queue
from payments_processing.services.payments import process_payment
from payments_processing.services.webhook import send_webhook

logger = logging.getLogger(__name__)


class PaymentEvent(BaseModel):
    """Событие с идентификатором платежа для обработки."""

    payment_id: UUID


@broker.subscriber(payments_queue, ack_policy=AckPolicy.MANUAL)
async def handle_payment(event: PaymentEvent) -> None:
    """Принимает событие платежа без автоматического подтверждения.

    Args:
        event: Событие с проверенным идентификатором платежа.
    """
    logger.info("Получено сообщение о платеже %s", event.payment_id)
    async with session_factory() as session:
        payment = await process_payment(session, event.payment_id)
    async with httpx.AsyncClient() as client:
        await send_webhook(client, payment)
