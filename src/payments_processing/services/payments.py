"""Операции с платежами и событиями outbox."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payments_processing.db.models import Currency, Outbox, Payment, PaymentStatus
from payments_processing.services.exceptions import IdempotencyConflict, PaymentNotFound
from payments_processing.services.gateway import simulate_payment


async def create_payment(
    session: AsyncSession,
    *,
    amount: Decimal,
    currency: Currency,
    description: str,
    metadata: dict[str, Any],
    webhook_url: str,
    idempotency_key: str,
) -> Payment:
    """Сохраняет платёж и событие outbox в одной транзакции.

    Args:
        session: Сессия БД с expire_on_commit=False.
        amount: Сумма платежа.
        currency: Валюта платежа.
        description: Описание платежа.
        metadata: Дополнительные данные платежа.
        webhook_url: Адрес уведомления о результате.
        idempotency_key: Ключ идемпотентности запроса.

    Returns:
        Новый или существующий платёж с теми же данными.

    Raises:
        IdempotencyConflict: Если ключ уже использован с другими данными.
    """
    async with session.begin():
        payment = await session.scalar(
            insert(Payment)
            .values(
                amount=amount,
                currency=currency,
                description=description,
                payment_metadata=metadata,
                webhook_url=webhook_url,
                idempotency_key=idempotency_key,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        if payment is None:
            payment = await session.scalar(
                select(Payment).where(
                    Payment.idempotency_key == idempotency_key,
                    Payment.amount == amount,
                    Payment.currency == currency,
                    Payment.description == description,
                    Payment.payment_metadata == metadata,
                    Payment.webhook_url == webhook_url,
                )
            )
            if payment is None:
                raise IdempotencyConflict
        else:
            session.add(
                Outbox(event_type="payments.new", payload={"payment_id": str(payment.id)})
            )

    return payment


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    """Находит платёж по идентификатору.

    Args:
        session: Сессия БД для чтения платежа.
        payment_id: Идентификатор платежа.

    Returns:
        Найденный платёж или None, если он отсутствует.
    """
    return await session.get(Payment, payment_id)


async def process_payment(session: AsyncSession, payment_id: UUID) -> Payment:
    """Обрабатывает ожидающий платёж или возвращает сохранённый результат.

    Блокировка строки удерживается до сохранения результата и завершения
    транзакции. Параллельный обработчик дождётся её и прочитает итоговый статус.

    Args:
        session: Сессия БД с expire_on_commit=False без открытой транзакции.
        payment_id: Идентификатор платежа.

    Returns:
        Платёж с сохранённым результатом обработки.

    Raises:
        PaymentNotFound: Если платёж отсутствует в БД.
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
        if payment.status == PaymentStatus.PENDING:
            payment.status = await simulate_payment()
            payment.processed_at = datetime.now(UTC)

    return payment
