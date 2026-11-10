"""Учёт попыток и передача сообщений на повтор или в DLQ."""

from uuid import UUID

from pamqp.commands import Basic
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from payments_processing.db.models import Payment
from payments_processing.messaging.broker import (
    broker,
    dead_letter_exchange,
    dead_letter_queue,
    retry_queues,
)
from payments_processing.services.payments import PaymentNotFound

MAX_ATTEMPTS = 3


async def begin_attempt(
    session: AsyncSession, payment_id: UUID, previous_attempts: int = 0
) -> int | None:
    """Фиксирует начало попытки отдельно от транзакции обработки платежа.

    Args:
        session: Сессия БД без открытой транзакции.
        payment_id: Идентификатор платежа.
        previous_attempts: Число попыток из заголовка сообщения на случай сбоев БД.

    Returns:
        Номер начатой попытки или None, если лимит уже исчерпан.

    Raises:
        PaymentNotFound: Если платёж отсутствует в БД.
    """
    async with session.begin():
        attempt = await session.scalar(
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.processing_attempts < MAX_ATTEMPTS,
                previous_attempts < MAX_ATTEMPTS,
            )
            .values(
                processing_attempts=func.greatest(
                    Payment.processing_attempts, previous_attempts
                )
                + 1
            )
            .returning(Payment.processing_attempts)
            .execution_options(synchronize_session=False)
        )
        if attempt is None:
            exists = await session.scalar(
                select(Payment.id).where(Payment.id == payment_id)
            )
            if exists is None:
                raise PaymentNotFound(str(payment_id))
    return attempt


async def publish_failure(payload: dict[str, str], attempt: int) -> None:
    """Надёжно публикует сообщение в очередь ожидания или DLQ.

    Args:
        payload: Исходное событие платежа.
        attempt: Количество использованных попыток.

    Raises:
        RuntimeError: Если брокер не подтвердил публикацию.
    """
    queue = retry_queues[attempt - 1] if attempt < MAX_ATTEMPTS else dead_letter_queue
    confirmation = await broker.publish(
        payload,
        queue=queue,
        exchange=dead_letter_exchange if attempt >= MAX_ATTEMPTS else None,
        headers={"x-attempts": attempt},
        persist=True,
        mandatory=True,
        timeout=10,
    )
    if not isinstance(confirmation, Basic.Ack):
        raise RuntimeError("RabbitMQ не подтвердил передачу сообщения")  # noqa: TRY004
