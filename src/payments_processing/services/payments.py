"""Операции с платежами и событиями outbox."""

from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from payments_processing.db.models import Currency, Outbox, Payment


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
        Сохранённый платёж.
    """
    async with session.begin():
        payment = Payment(
            amount=amount,
            currency=currency,
            description=description,
            payment_metadata=metadata,
            webhook_url=webhook_url,
            idempotency_key=idempotency_key,
        )
        session.add(payment)
        await session.flush()
        session.add(
            Outbox(event_type="payments.new", payload={"payment_id": str(payment.id)})
        )

    return payment
