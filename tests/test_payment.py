"""Интеграционные проверки хранения платежей в PostgreSQL."""

from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from payments_processing.db.models import Currency, Payment, PaymentStatus


@pytest.fixture
def payment_data():
    """Возвращает данные нового платежа."""
    return {
        "amount": Decimal("1234567890123456.78"),
        "currency": Currency.RUB,
        "description": "Оплата заказа",
        "idempotency_key": str(uuid4()),
        "webhook_url": "https://example.com/webhook",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("currency", list(Currency))
async def test_payment_preserves_data(session, payment_data, currency):
    """Платёж сохраняет сумму без потери точности, валюту и JSON-метаданные."""
    payment_data["currency"] = currency
    metadata = {"order_id": "123", "items": [1, 2], "details": {"paid": False}}
    payment = Payment(**payment_data, payment_metadata=metadata)
    session.add(payment)
    await session.flush()
    await session.refresh(payment)

    assert isinstance(payment.id, UUID)
    assert payment.amount == payment_data["amount"]
    assert payment.currency == currency
    assert payment.description == payment_data["description"]
    assert payment.payment_metadata == metadata
    assert payment.idempotency_key == payment_data["idempotency_key"]
    assert payment.webhook_url == payment_data["webhook_url"]


@pytest.mark.asyncio
async def test_new_payment_defaults(session, payment_data):
    """Новый платёж ожидает обработки и получает дату создания."""
    payment = Payment(**payment_data)
    session.add(payment)
    await session.flush()
    await session.refresh(payment)

    assert payment.status == PaymentStatus.PENDING
    assert payment.payment_metadata == {}
    assert payment.created_at.tzinfo is not None
    assert payment.processed_at is None


@pytest.mark.asyncio
async def test_duplicate_idempotency_key_is_rejected(session, payment_data):
    """БД запрещает создание двух платежей с одним ключом идемпотентности."""
    session.add(Payment(**payment_data))
    await session.flush()

    with pytest.raises(IntegrityError) as error:
        async with session.begin_nested():
            session.add(Payment(**payment_data))
            await session.flush()

    assert error.value.orig.sqlstate == "23505"
