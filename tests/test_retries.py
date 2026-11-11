"""Проверки общего лимита попыток и передачи сообщений на повтор."""

from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pamqp.commands import Basic

from payments_processing.db.models import Currency, Payment
from payments_processing.services import retries


@pytest.mark.asyncio
async def test_attempt_survives_processing_rollback_and_stops_at_three(session):
    """Начало попытки фиксируется отдельно от обработки; четвёртой попытки нет."""
    payment = Payment(
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Заказ",
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhook",
    )
    async with session.begin():
        session.add(payment)
    payment_id = payment.id

    for expected in range(1, 4):
        assert await retries.begin_attempt(session, payment_id) == expected
        assert not session.in_transaction()
        await session.rollback()

    assert await retries.begin_attempt(session, payment_id) is None


@pytest.mark.asyncio
async def test_attempt_accounts_for_db_failures_in_message_header(session):
    """После восстановления БД учитываются попытки из заголовка сообщения."""
    payment = Payment(
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Заказ",
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhook",
    )
    async with session.begin():
        session.add(payment)

    assert await retries.begin_attempt(session, payment.id, previous_attempts=2) == 3
    assert await retries.begin_attempt(session, payment.id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attempt,queue",
    [(1, "payments.retry.1"), (2, "payments.retry.2"), (3, "payments.dlq")],
)
async def test_retry_routes_by_attempt(monkeypatch, attempt, queue):
    """Первые две неудачи направляются на ожидание, третья — в DLQ."""
    publish = AsyncMock(return_value=Basic.Ack())
    monkeypatch.setattr(retries.broker, "publish", publish)
    payload = {"payment_id": str(uuid4())}

    await retries.publish_failure(payload, attempt)

    assert publish.await_args.kwargs["queue"].name == queue
    exchange = publish.await_args.kwargs.get("exchange")
    if attempt == 3:
        assert exchange is not None
        assert exchange.name == "payments.dlx"
    else:
        assert exchange is None
    assert publish.await_args.kwargs["headers"] == {"x-attempts": attempt}
    assert publish.await_args.kwargs["persist"] is True
    assert publish.await_args.kwargs["mandatory"] is True


@pytest.mark.asyncio
async def test_retry_requires_broker_confirmation(monkeypatch):
    """Отсутствие publisher confirm не считается передачей на повтор."""
    monkeypatch.setattr(retries.broker, "publish", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="подтвердил"):
        await retries.publish_failure({"payment_id": str(uuid4())}, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("attempts", [1, 3])
async def test_delivered_webhook_does_not_consume_attempt(session, attempts):
    """Дубли завершённого уведомления не расходуют попытки даже после лимита."""
    from datetime import UTC, datetime

    payment = Payment(
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Заказ",
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhook",
        processing_attempts=attempts,
        webhook_delivered_at=datetime.now(UTC),
    )
    async with session.begin():
        session.add(payment)
    assert await retries.begin_attempt(session, payment.id, 3) == 0
    await session.refresh(payment)
    assert payment.processing_attempts == attempts


@pytest.mark.asyncio
async def test_delivery_marker_is_persisted_before_duplicate(session):
    """Сохранённая доставка предотвращает новую попытку в БД."""
    from payments_processing.services.webhook import mark_webhook_delivered

    payment = Payment(
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Заказ",
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhook",
    )
    async with session.begin():
        session.add(payment)
    assert await retries.begin_attempt(session, payment.id) == 1
    await mark_webhook_delivered(session, payment.id)
    assert await retries.begin_attempt(session, payment.id) == 0
    await session.refresh(payment)
    assert payment.webhook_delivered_at is not None
    assert payment.processing_attempts == 1
