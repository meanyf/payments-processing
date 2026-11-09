"""Отдельный процесс публикации событий outbox в RabbitMQ."""

import asyncio
import logging
import signal

from payments_processing.db.session import engine, session_factory
from payments_processing.messaging.broker import connected_broker
from payments_processing.services.outbox import publish_pending_events

logger = logging.getLogger(__name__)


async def run_publisher(*, poll_interval: float = 1.0) -> None:
    """Периодически публикует outbox до отмены задачи.

    Ошибки публикации логируются, после паузы выполняется новая попытка.
    При остановке закрываются сессия, соединение с брокером и пул БД.

    Args:
        poll_interval: Пауза между попытками публикации в секундах.
    """
    try:
        async with connected_broker():
            logger.info("Publisher запущен")
            while True:
                try:
                    async with session_factory() as session:
                        await publish_pending_events(session)
                except Exception:
                    logger.exception("Ошибка публикации outbox; повтор после паузы")
                await asyncio.sleep(poll_interval)
    finally:
        await engine.dispose()


async def main() -> None:
    """Запускает publisher с остановкой по SIGTERM и Ctrl+C."""
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    assert task is not None
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        await run_publisher()
    except asyncio.CancelledError:
        logger.info("Publisher остановлен")
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
