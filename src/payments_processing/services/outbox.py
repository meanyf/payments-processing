"""Выборка и публикация событий outbox."""

from datetime import UTC, datetime

from pamqp.commands import Basic
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payments_processing.db.models import Outbox
from payments_processing.messaging.broker import payments_publisher


async def get_pending_events(
    session: AsyncSession, *, batch_size: int = 100
) -> list[Outbox]:
    """Выбирает и блокирует порцию неопубликованных событий.

    Вызывающий код сохраняет транзакцию открытой до завершения публикации
    и записи published_at. Блокировки снимаются при коммите или откате.

    Args:
        session: Сессия БД в транзакции, которой управляет вызывающий код.
        batch_size: Максимальное количество событий в порции.

    Returns:
        События по дате создания, пропуская заблокированные другим процессом.
    """
    events = await session.scalars(
        select(Outbox)
        .where(Outbox.published_at.is_(None))
        .order_by(Outbox.created_at, Outbox.id)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    return list(events)


async def publish_pending_events(
    session: AsyncSession, *, batch_size: int = 100
) -> int:
    """Публикует порцию событий и сохраняет подтверждённые отправки.

    Транзакция удерживает блокировки до сохранения published_at. При ошибке
    откатывается вся порция; уже отправленные сообщения могут прийти повторно.

    Args:
        session: Сессия БД без активной транзакции.
        batch_size: Максимальное количество событий в порции.

    Returns:
        Количество опубликованных событий.

    Raises:
        RuntimeError: Если брокер не подтвердил публикацию.
    """
    async with session.begin():
        events = await get_pending_events(session, batch_size=batch_size)
        for event in events:
            confirmation = await payments_publisher.publish(event.payload)
            if not isinstance(confirmation, Basic.Ack):
                raise RuntimeError("RabbitMQ did not confirm publication")  # noqa: TRY004
            event.published_at = datetime.now(UTC)
    return len(events)
