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
