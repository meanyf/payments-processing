"""Интеграционные проверки хранения событий outbox в PostgreSQL."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select

from payments_processing.db.models import Outbox


@pytest.mark.asyncio
async def test_new_event_is_pending_and_preserves_payload(session):
    """Новое событие сохраняет JSON и ожидает публикации."""
    payload = {"payment_id": str(uuid4())}
    event = Outbox(event_type="payments.new", payload=payload)
    session.add(event)
    await session.flush()
    await session.refresh(event)

    assert isinstance(event.id, UUID)
    assert event.event_type == "payments.new"
    assert event.payload == payload
    assert event.created_at.tzinfo is not None
    assert event.published_at is None


@pytest.mark.asyncio
async def test_published_event_is_excluded_from_pending_events(session):
    """Отметка публикации исключает событие из ожидающих отправки."""
    published = Outbox(event_type="payments.new", payload={"payment_id": str(uuid4())})
    pending = Outbox(event_type="payments.new", payload={"payment_id": str(uuid4())})
    session.add_all([published, pending])
    await session.flush()

    published.published_at = datetime.now(UTC)
    await session.flush()
    await session.refresh(published)
    events = (
        await session.scalars(select(Outbox).where(Outbox.published_at.is_(None)))
    ).all()

    assert published.published_at.tzinfo is not None
    assert [event.id for event in events] == [pending.id]
