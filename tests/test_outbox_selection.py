"""Проверки выборки outbox и блокировок PostgreSQL."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from alembic import command
from payments_processing.config import Settings
from payments_processing.db.models import Outbox
from payments_processing.services.outbox import get_pending_events


@pytest.mark.asyncio
async def test_selects_oldest_unpublished_events_with_limit(session):
    """Выборка пропускает опубликованные события и ограничивает размер порции."""
    now = datetime.now(UTC)
    events = [
        Outbox(
            event_type="payments.new",
            payload={"payment_id": str(uuid4())},
            created_at=now + timedelta(seconds=index),
            published_at=now if index == 0 else None,
        )
        for index in range(4)
    ]
    session.add_all(events)
    await session.flush()

    selected = await get_pending_events(session, batch_size=2)

    assert [event.id for event in selected] == [events[1].id, events[2].id]


@pytest_asyncio.fixture
async def outbox_sessions(alembic_config):
    """Создаёт независимые соединения с общей изолированной схемой."""
    schema = "test_outbox_" + uuid4().hex
    url = str(Settings().database_url)
    admin_engine = create_async_engine(url)
    engine = create_async_engine(
        url,
        connect_args={"server_settings": {"search_path": schema + ",pg_catalog"}},
    )

    def upgrade(connection):
        alembic_config.attributes["connection"] = connection
        command.upgrade(alembic_config, "head")

    try:
        async with admin_engine.begin() as connection:
            await connection.exec_driver_sql("CREATE SCHEMA " + schema)
        async with engine.begin() as connection:
            await connection.run_sync(upgrade)
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.exec_driver_sql("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_skips_locked_events_and_releases_them_after_rollback(outbox_sessions):
    """Второй publisher берёт другие записи, а откат освобождает первую порцию."""
    factory = outbox_sessions
    async with factory.begin() as session:
        session.add_all(
            Outbox(event_type="payments.new", payload={"payment_id": str(uuid4())})
            for _ in range(3)
        )

    async with factory() as first, factory() as second:
        async with first.begin():
            first_batch = await get_pending_events(first, batch_size=2)
            first_ids = {event.id for event in first_batch}
            async with second.begin():
                async with asyncio.timeout(5):
                    second_batch = await get_pending_events(second, batch_size=2)
                assert len(first_batch) == 2
                assert len(second_batch) == 1
                assert first_ids.isdisjoint(event.id for event in second_batch)
            # Моделируем откат транзакции упавшего publisher.
            await first.rollback()

        async with second.begin():
            available = await get_pending_events(second, batch_size=3)
            assert len(available) == 3
            assert first_ids.issubset(event.id for event in available)
