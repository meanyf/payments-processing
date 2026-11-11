"""Учёт попыток и передача сообщений на повтор или в DLQ."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from pamqp.commands import Basic
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from payments_processing.config import MAX_ATTEMPTS, RETRY_DELAYS
from payments_processing.db.models import Outbox, Payment
from payments_processing.db.session import session_factory
from payments_processing.messaging.broker import (
    broker,
    dead_letter_exchange,
    dead_letter_queue,
)
from payments_processing.services.exceptions import PaymentNotFound


async def begin_attempt(
    session: AsyncSession, payment_id: UUID, previous_attempts: int = 0
) -> int | None:
    """Фиксирует начало попытки отдельно от транзакции обработки платежа.

    Args:
        session: Сессия БД без открытой транзакции.
        payment_id: Идентификатор платежа.
        previous_attempts: Число попыток из заголовка сообщения на случай сбоев БД.

    Returns:
        Номер попытки, 0 для доставленного уведомления или None при исчерпании лимита.

    Raises:
        PaymentNotFound: Если платёж отсутствует в БД.
    """
    async with session.begin():
        attempt = await session.scalar(
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.processing_attempts < MAX_ATTEMPTS,
                Payment.webhook_delivered_at.is_(None),
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
                select(Payment)
                .where(Payment.id == payment_id)
                .execution_options(populate_existing=True)
            )
            if exists is None:
                raise PaymentNotFound(str(payment_id))
            if exists.webhook_delivered_at is not None:
                return 0
    return attempt


async def publish_failure(payload: dict[str, str], attempt: int) -> None:
    """Сохраняет повтор в outbox либо публикует сообщение в DLQ.

    Args:
        payload: Исходное событие платежа.
        attempt: Количество использованных попыток.

    Raises:
        RuntimeError: Если брокер не подтвердил публикацию.
    """
    if attempt < MAX_ATTEMPTS:
        async with session_factory() as session:
            await schedule_retry(session, payload, attempt)
        return
    queue = dead_letter_queue
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


async def schedule_retry(
    session: AsyncSession, payload: dict[str, str], attempt: int
) -> None:
    """Сохраняет отложенный повтор до ACK исходного сообщения.

    Args:
        session: Сессия без активной транзакции.
        payload: Исходное событие платежа.
        attempt: Число завершённых попыток.
    """
    async with session.begin():
        session.add(
            Outbox(
                event_type="payments.new",
                payload=payload,
                headers={"x-attempts": attempt},
                available_at=datetime.now(UTC)
                + timedelta(seconds=RETRY_DELAYS[attempt - 1]),
            )
        )
