"""Проверки общего лимита попыток и передачи сообщений на повтор."""

from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from payments_processing.db.models import (
    AttemptState,
    Currency,
    Outbox,
    Payment,
    PaymentStatus,
)
from payments_processing.messaging.broker import broker
from payments_processing.services import retries
from payments_processing.services.exceptions import AttemptMismatch


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
        assert (
            await retries.begin_attempt(session, payment_id, expected - 1) == expected
        )
        assert not session.in_transaction()
        await session.rollback()
        if expected < 3:
            await retries.schedule_retry(
                session, {"payment_id": str(payment_id)}, expected
            )
            from datetime import UTC, datetime, timedelta

            async with session.begin():
                payment.next_attempt_at = datetime.now(UTC) - timedelta(seconds=1)

    with pytest.raises(retries.InterruptedAttempt) as error:
        await retries.begin_attempt(session, payment_id)
    assert error.value.attempt == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("previous_attempts", [1, 2, 3])
async def test_ready_payment_rejects_nonzero_attempts_without_changing_state(
    session, previous_attempts
):
    """Заголовок не расходует попытки нового платежа и не мешает верному событию."""
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

    with pytest.raises(AttemptMismatch):
        await retries.begin_attempt(session, payment_id, previous_attempts)

    assert not session.in_transaction()
    async with session.begin():
        payment = await session.get(Payment, payment_id, populate_existing=True)
        assert payment.attempt_state == AttemptState.READY
        assert payment.processing_attempts == 0
        assert payment.status == PaymentStatus.PENDING
        assert payment.processed_at is None
        assert payment.next_attempt_at is None
        assert payment.webhook_delivered_at is None
        assert list(await session.scalars(select(Outbox))) == []

    assert await retries.begin_attempt(session, payment_id) == 1


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


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", [1, 2, 3])
async def test_failure_schedules_retry_without_contacting_broker(monkeypatch, attempt):
    """Повтор и окончательная ошибка сохраняются в БД без обращения к RabbitMQ."""
    from unittest.mock import Mock

    context = AsyncMock()
    monkeypatch.setattr(retries, "session_factory", Mock(return_value=context))
    schedule = AsyncMock()
    publish = AsyncMock(side_effect=TimeoutError)
    monkeypatch.setattr(
        retries, "schedule_retry" if attempt < 3 else "exhaust_attempts", schedule
    )
    monkeypatch.setattr(broker, "publish", publish)
    payload = {"payment_id": str(uuid4())}
    await retries.publish_failure(payload, attempt)
    schedule.assert_awaited_once_with(context.__aenter__.return_value, payload, attempt)
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_interrupted_attempt_recovers_without_consuming_retry(session):
    """Прерывание требует отложенного повтора, старый дубль не начинает попытку."""
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import select

    from payments_processing.db.models import Outbox

    payment = Payment(
        amount=Decimal(1),
        currency=Currency.RUB,
        description="test",
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com",
    )
    async with session.begin():
        session.add(payment)
    payment_id = payment.id
    assert await retries.begin_attempt(session, payment_id) == 1
    with pytest.raises(retries.InterruptedAttempt) as error:
        await retries.begin_attempt(session, payment_id)
    assert error.value.attempt == 1
    payload = {"payment_id": str(payment_id)}
    await retries.schedule_retry(session, payload, 1)
    await retries.schedule_retry(session, payload, 1)
    assert await retries.begin_attempt(session, payment_id) == 0
    with pytest.raises(retries.RetryNotDue):
        await retries.begin_attempt(session, payment_id, 1)
    async with session.begin():
        rows = list((await session.scalars(select(Outbox))).all())
        assert len(rows) == 1
        await session.refresh(payment)
        assert payment.processing_attempts == 1
        payment.next_attempt_at = datetime.now(UTC) - timedelta(seconds=1)
    assert await retries.begin_attempt(session, payment_id, 1) == 2
