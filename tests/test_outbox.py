"""Интеграционные проверки хранения событий outbox в PostgreSQL."""

from uuid import UUID, uuid4

import pytest

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
