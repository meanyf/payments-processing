"""Учёт попыток и передача сообщений на повтор или в DLQ."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from pamqp.commands import Basic
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payments_processing.config import MAX_ATTEMPTS, RETRY_DELAYS
from payments_processing.db.models import AttemptState, Outbox, Payment
from payments_processing.db.session import session_factory
from payments_processing.messaging.broker import (
    broker,
    dead_letter_exchange,
    dead_letter_queue,
)
from payments_processing.services.exceptions import AttemptMismatch, PaymentNotFound


class InterruptedAttempt(Exception):
    """Сигнализирует о необходимости завершить прерванную попытку."""

    def __init__(self, attempt: int):
        self.attempt = attempt
        super().__init__(f"Прервана попытка {attempt}")


class RetryNotDue(Exception):
    """Повтор доставлен раньше разрешённого времени."""


async def begin_attempt(
    session: AsyncSession, payment_id: UUID, previous_attempts: int = 0
) -> int | None:
    """Начинает попытку под внешней advisory-блокировкой платежа.

    Args:
        session: Сессия без активной транзакции.
        payment_id: Идентификатор платежа.
        previous_attempts: Номер завершённой попытки из сообщения.

    Returns:
        Номер новой попытки, 0 для дубля или None для исчерпанного лимита.

    Raises:
        InterruptedAttempt: Предыдущая попытка требует восстановления.
        RetryNotDue: Срок повтора ещё не наступил.
        PaymentNotFound: Платёж отсутствует.
        AttemptMismatch: Номер повтора несовместим с состоянием платежа.
    """
    async with session.begin():
        payment = await session.scalar(
            select(Payment)
            .where(Payment.id == payment_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if payment is None:
            raise PaymentNotFound(str(payment_id))
        if payment.webhook_delivered_at is not None:
            return 0
        if payment.attempt_state == AttemptState.PROCESSING:
            raise InterruptedAttempt(payment.processing_attempts)
        if payment.attempt_state == AttemptState.WAITING_RETRY:
            if previous_attempts < payment.processing_attempts:
                return 0
            if previous_attempts != payment.processing_attempts:
                raise AttemptMismatch(
                    "Номер повтора не соответствует состоянию платежа"
                )
            if payment.next_attempt_at > datetime.now(UTC):
                raise RetryNotDue
        if payment.processing_attempts >= MAX_ATTEMPTS:
            return None
        payment.processing_attempts = max(
            payment.processing_attempts, previous_attempts
        )
        if payment.processing_attempts >= MAX_ATTEMPTS:
            return None
        payment.processing_attempts += 1
        payment.attempt_state = AttemptState.PROCESSING
        payment.next_attempt_at = None
        return payment.processing_attempts


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
        payment = await session.scalar(
            select(Payment)
            .where(Payment.id == UUID(payload["payment_id"]))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if payment is None:
            raise PaymentNotFound(payload["payment_id"])
        if (
            payment.webhook_delivered_at is not None
            or payment.attempt_state == AttemptState.WAITING_RETRY
        ):
            return
        available_at = datetime.now(UTC) + timedelta(seconds=RETRY_DELAYS[attempt - 1])
        payment.attempt_state = AttemptState.WAITING_RETRY
        payment.next_attempt_at = available_at
        session.add(
            Outbox(
                event_type="payments.new",
                payload=payload,
                headers={"x-attempts": attempt},
                available_at=available_at,
            )
        )
