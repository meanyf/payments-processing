"""Проверки завершения попыток и атомарного планирования передачи в DLQ."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from pamqp.commands import Basic
from sqlalchemy import event, select

from payments_processing.db.models import (
    AttemptState,
    Currency,
    Outbox,
    Payment,
    PaymentStatus,
)
from payments_processing.messaging.broker import broker
from payments_processing.services import outbox, retries


@pytest_asyncio.fixture
async def final_attempt(session, monkeypatch):
    """Предоставляет платёж на третьей попытке и изолирует отправку в брокер."""
    payment = Payment(
        amount=1,
        currency=Currency.RUB,
        description="Платёж с недоставленным webhook",
        idempotency_key=uuid4().hex,
        webhook_url="https://example.com/webhook",
        status=PaymentStatus.SUCCEEDED,
        processing_attempts=3,
        attempt_state=AttemptState.PROCESSING,
    )
    async with session.begin():
        session.add(payment)

    @asynccontextmanager
    async def test_session():
        yield session

    publish = AsyncMock(return_value=Basic.Ack())
    monkeypatch.setattr(retries, "session_factory", test_session)
    monkeypatch.setattr(broker, "publish", publish)
    return payment.id, publish


@pytest.mark.asyncio
async def test_third_failure_persists_exhausted_state_and_dlq_event(
    session, final_attempt
):
    """Третья ошибка сохраняет конечное состояние и outbox до обращения к брокеру."""
    payment_id, publish = final_attempt
    payload = {"payment_id": str(payment_id)}

    await retries.publish_failure(payload, 3)

    assert not session.in_transaction()
    payment = await session.get(Payment, payment_id, populate_existing=True)
    assert payment.attempt_state == "exhausted"
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.processing_attempts == 3
    assert payment.next_attempt_at is None
    assert payment.webhook_delivered_at is None
    saved = (await session.scalars(select(Outbox))).one()
    assert saved.event_type == "payments.dlq"
    assert saved.payload == payload
    assert saved.headers == {"x-attempts": 3}
    assert saved.published_at is None
    publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [TimeoutError("RabbitMQ недоступен"), None])
async def test_dlq_event_survives_publish_failure(session, final_attempt, failure):
    """Сбой публикации оставляет DLQ в outbox, не возобновляя обработку платежа."""
    payment_id, publish = final_attempt
    payload = {"payment_id": str(payment_id)}
    await retries.publish_failure(payload, 3)
    publish.side_effect = [failure, Basic.Ack()]

    with pytest.raises(TimeoutError if failure is not None else RuntimeError):
        await outbox.publish_pending_events(session)

    async with session.begin():
        payment = await session.get(Payment, payment_id, populate_existing=True)
        assert payment.attempt_state == "exhausted"
        saved = (await session.scalars(select(Outbox))).one()
        assert saved.published_at is None
    assert await retries.begin_attempt(session, payment_id) == 0
    assert await outbox.publish_pending_events(session) == 1
    publish.assert_awaited_with(
        payload,
        queue=outbox.dead_letter_queue,
        exchange=outbox.dead_letter_exchange,
        headers={"x-attempts": 3},
        persist=True,
        mandatory=True,
        timeout=10,
    )
    saved = (await session.scalars(select(Outbox))).one()
    assert saved.published_at is not None


@pytest.mark.asyncio
async def test_duplicates_after_exhaustion_do_not_schedule_another_dlq_event(
    session, final_attempt
):
    """Повтор завершения и старые сообщения не начинают попытку и не множат DLQ."""
    payment_id, publish = final_attempt
    payload = {"payment_id": str(payment_id)}
    await retries.publish_failure(payload, 3)
    await retries.publish_failure(payload, 3)

    for previous_attempts in (0, 1, 2, 3):
        assert await retries.begin_attempt(session, payment_id, previous_attempts) == 0

    events = list(await session.scalars(select(Outbox)))
    assert len(events) == 1
    assert events[0].event_type == "payments.dlq"
    payment = await session.get(Payment, payment_id, populate_existing=True)
    assert payment.processing_attempts == 3
    assert payment.attempt_state == "exhausted"
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_outbox_insert_failure_rolls_back_exhaustion(session, final_attempt):
    """Ошибка outbox оставляет попытку незавершённой для безопасной redelivery."""
    payment_id, publish = final_attempt

    def fail_insert(mapper, connection, target):
        raise RuntimeError("Ошибка записи DLQ в outbox")

    event.listen(Outbox, "before_insert", fail_insert)
    try:
        with pytest.raises(RuntimeError, match="Ошибка записи DLQ в outbox"):
            await retries.publish_failure({"payment_id": str(payment_id)}, 3)
    finally:
        event.remove(Outbox, "before_insert", fail_insert)

    assert not session.in_transaction()
    payment = await session.get(Payment, payment_id, populate_existing=True)
    assert payment.attempt_state == AttemptState.PROCESSING
    assert payment.processing_attempts == 3
    assert payment.status == PaymentStatus.SUCCEEDED
    assert list(await session.scalars(select(Outbox))) == []
    publish.assert_not_awaited()
