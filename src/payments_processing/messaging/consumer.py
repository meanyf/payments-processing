"""Получение событий о новых платежах."""

import logging

import httpx
from faststream import AckPolicy
from faststream.rabbit.annotations import RabbitMessage
from pamqp.commands import Basic
from pydantic import ValidationError

from payments_processing.config import MAX_ATTEMPTS
from payments_processing.db.session import session_factory
from payments_processing.messaging import broker as broker_module
from payments_processing.messaging.broker import broker, payments_queue
from payments_processing.messaging.schemas import PaymentEvent
from payments_processing.services.exceptions import AttemptMismatch, PaymentNotFound
from payments_processing.services.payment_lock import lock_payment
from payments_processing.services.payments import process_payment
from payments_processing.services.retries import (
    InterruptedAttempt,
    begin_attempt,
    publish_failure,
)
from payments_processing.services.webhook import mark_webhook_delivered, send_webhook

logger = logging.getLogger(__name__)


async def handle_payment(event: PaymentEvent, message: RabbitMessage) -> None:
    """Допускает только одну активную обработку платежа между consumer-процессами."""
    try:
        async with lock_payment(event.payment_id) as acquired:
            if not acquired:
                await message.nack(requeue=True)
                return
            await _handle_payment(event, message)
    except Exception:
        logger.exception("Ошибка блокировки платежа %s", event.payment_id)
        await message.nack(requeue=True)


async def _handle_payment(event: PaymentEvent, message: RabbitMessage) -> None:
    """Обрабатывает платёж и webhook с общим лимитом попыток.

    Args:
        event: Событие с проверенным идентификатором платежа.
        message: Сообщение RabbitMQ для управления подтверждением доставки.
    """
    logger.info("Получено сообщение о платеже %s", event.payment_id)
    previous_attempts = int(message.headers.get("x-attempts", 0))
    try:
        async with session_factory() as session:
            started = await begin_attempt(session, event.payment_id, previous_attempts)
    except InterruptedAttempt as error:
        started = None
        attempt = error.attempt
    except (PaymentNotFound, AttemptMismatch) as error:
        await reject_invalid(message, str(error))
        return
    except Exception:
        logger.exception("Не удалось начать попытку платежа %s", event.payment_id)
        await message.nack(requeue=True)
        return
    else:
        attempt = MAX_ATTEMPTS if started is None else started
    try:
        async with session_factory() as session:
            if started == 0:
                await message.ack()
                return
            if started is not None:
                attempt = started
                payment = await process_payment(session, event.payment_id)
        if started is not None:
            async with httpx.AsyncClient() as client:
                await send_webhook(client, payment)
            async with session_factory() as session:
                await mark_webhook_delivered(session, event.payment_id)
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


async def raw_body(message: RabbitMessage) -> bytes:
    """Возвращает исходные байты для проверки JSON внутри обработчика."""
    return message.body


async def publish_invalid(body: bytes, headers: dict, reason: str) -> None:
    """Передаёт исходное сообщение и причину ошибки в DLQ.

    Args:
        body: Исходное тело сообщения.
        headers: Исходные заголовки.
        reason: Причина отказа в обработке.
    """
    confirmation = await broker.publish(
        body,
        queue=broker_module.dead_letter_queue,
        exchange=broker_module.dead_letter_exchange,
        headers={**headers, "x-validation-error": reason},
        persist=True,
        mandatory=True,
        timeout=10,
    )
    if not isinstance(confirmation, Basic.Ack):
        raise RuntimeError("RabbitMQ не подтвердил передачу в DLQ")  # noqa: TRY004


async def reject_invalid(message: RabbitMessage, reason: str) -> None:
    """Подтверждает постоянную ошибку только после передачи оригинала в DLQ."""
    try:
        await publish_invalid(message.body, message.headers, reason)
    except Exception:
        logger.exception("Не удалось передать некорректное сообщение в DLQ")
        await message.nack(requeue=True)
        return
    await message.ack()


@broker.subscriber(payments_queue, ack_policy=AckPolicy.MANUAL, decoder=raw_body)
async def receive_payment(body: bytes, message: RabbitMessage) -> None:
    """Проверяет сообщение и направляет некорректные данные в DLQ.

    Args:
        body: Исходное тело сообщения.
        message: Сообщение RabbitMQ для подтверждения доставки.
    """
    try:
        event = PaymentEvent.model_validate_json(body)
        attempts = message.headers.get("x-attempts", 0)
        if type(attempts) is not int or not 0 <= attempts <= MAX_ATTEMPTS:
            raise ValueError("x-attempts должен быть целым числом от 0 до 3")
    except (ValidationError, ValueError) as error:
        await reject_invalid(message, str(error))
        return
    await handle_payment(event, message)
