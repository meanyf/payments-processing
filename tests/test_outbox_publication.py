"""Проверки публикации outbox и сохранения отметок в PostgreSQL."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pamqp.commands import Basic
from sqlalchemy import select

from payments_processing.db.models import Outbox
from payments_processing.services import outbox
from payments_processing.services.outbox import publish_pending_events


@pytest.mark.asyncio
async def test_marks_event_only_after_broker_confirmation(session, monkeypatch):
    """Отметка появляется после подтверждения; отправленное событие пропускается."""
    event = Outbox(event_type="payments.new", payload={"payment_id": str(uuid4())})
    session.add(event)
    await session.commit()
    payload = event.payload.copy()
    event_id = event.id

    async def confirm(message):
        assert message == payload
        assert event.published_at is None
        return Basic.Ack()

    publish = AsyncMock(side_effect=confirm)
    monkeypatch.setattr(outbox.payments_publisher, "publish", publish)

    assert await publish_pending_events(session) == 1
    saved = await session.get(Outbox, event_id)
    assert saved.published_at is not None
    assert saved.published_at.tzinfo is not None
    await session.commit()
    assert await publish_pending_events(session) == 0
    publish.assert_awaited_once_with(payload)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "error"),
    [(TimeoutError("Broker timeout"), TimeoutError), (None, RuntimeError)],
)
async def test_failed_batch_is_rolled_back_and_retried(
    session, monkeypatch, failure, error
):
    """Сбой второй отправки откатывает отметки; следующая попытка отправляет порцию."""
    session.add_all(
        Outbox(event_type="payments.new", payload={"payment_id": str(uuid4())})
        for _ in range(2)
    )
    await session.commit()
    publish = AsyncMock(side_effect=[Basic.Ack(), failure])
    monkeypatch.setattr(outbox.payments_publisher, "publish", publish)

    with pytest.raises(error):
        await publish_pending_events(session)

    events = list(await session.scalars(select(Outbox)))
    assert len(events) == 2
    assert all(event.published_at is None for event in events)
    await session.commit()

    publish.side_effect = None
    publish.return_value = Basic.Ack()
    assert await publish_pending_events(session) == 2
    assert all(event.published_at is not None for event in events)
    assert publish.await_count == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt,delay", [(1, 2), (2, 4)])
async def test_scheduled_retry_survives_broker_failure(
    session, monkeypatch, attempt, delay
):
    """Повтор ждёт срока и сохраняется при сбое RabbitMQ вместе со счётчиком."""
    from datetime import UTC, datetime, timedelta

    from payments_processing.db.models import AttemptState, Currency, Payment
    from payments_processing.services.retries import schedule_retry

    payment = Payment(
        amount=1,
        currency=Currency.RUB,
        description="test",
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com",
        processing_attempts=attempt,
        attempt_state=AttemptState.PROCESSING,
    )
    async with session.begin():
        session.add(payment)
    payload = {"payment_id": str(payment.id)}
    before = datetime.now(UTC)
    await schedule_retry(session, payload, attempt)
    event = await session.scalar(select(Outbox))
    assert event.available_at >= before + timedelta(seconds=delay)
    await session.commit()
    publish = AsyncMock(side_effect=TimeoutError)
    monkeypatch.setattr(outbox.payments_publisher, "publish", publish)
    assert await publish_pending_events(session) == 0
    publish.assert_not_awaited()
    event.available_at = before - timedelta(seconds=1)
    await session.commit()
    with pytest.raises(TimeoutError):
        await publish_pending_events(session)
    event = await session.scalar(select(Outbox))
    assert event.published_at is None
    assert event.headers == {"x-attempts": attempt}
    await session.commit()
    publish.side_effect = None
    publish.return_value = Basic.Ack()
    assert await publish_pending_events(session) == 1
    publish.assert_awaited_with(payload, headers={"x-attempts": attempt})
