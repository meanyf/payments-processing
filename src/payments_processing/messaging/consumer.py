"""Получение событий о новых платежах."""

import logging

import httpx
from faststream import AckPolicy
from faststream.rabbit.annotations import RabbitMessage

from payments_processing.config import MAX_ATTEMPTS
from payments_processing.db.session import session_factory
from payments_processing.messaging.broker import broker, payments_queue
from payments_processing.messaging.schemas import PaymentEvent
from payments_processing.services.payments import process_payment
from payments_processing.services.retries import (
    begin_attempt,
    publish_failure,
)
from payments_processing.services.webhook import send_webhook

logger = logging.getLogger(__name__)


@broker.subscriber(payments_queue, ack_policy=AckPolicy.MANUAL)
async def handle_payment(event: PaymentEvent, message: RabbitMessage) -> None:
    """Обрабатывает платёж и webhook с общим лимитом попыток.

    Args:
        event: Событие с проверенным идентификатором платежа.
        message: Сообщение RabbitMQ для управления подтверждением доставки.
    """
    logger.info("Получено сообщение о платеже %s", event.payment_id)
    previous_attempts = int(message.headers.get("x-attempts", 0))
    attempt = min(previous_attempts + 1, MAX_ATTEMPTS)
    try:
        async with session_factory() as session:
            started = await begin_attempt(session, event.payment_id, previous_attempts)
            if started is None:
                attempt = MAX_ATTEMPTS
            else:
                attempt = started
                payment = await process_payment(session, event.payment_id)
        if started is not None:
            async with httpx.AsyncClient() as client:
                await send_webhook(client, payment)
    except Exception:
        logger.exception(
            "Ошибка обработки платежа %s, попытка %s", event.payment_id, attempt
        )
    else:
        if started is not None:
            await message.ack()
            return

    try:
        await publish_failure(event.model_dump(mode="json"), attempt)
    except Exception:
        logger.exception("Не удалось передать сообщение на повтор или в DLQ")
        await message.nack(requeue=True)
        return
    await message.ack()
