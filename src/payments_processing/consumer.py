"""Отдельный процесс обработки платежей из RabbitMQ."""

import asyncio
import logging
import signal

from payments_processing.db.session import engine
from payments_processing.messaging import consumer as payment_consumer  # noqa: F401
from payments_processing.messaging.broker import connected_broker

logger = logging.getLogger(__name__)


async def run_consumer() -> None:
    """Запускает подписку и ожидает остановки процесса.

    Брокер завершает обработчики и закрывает соединение до освобождения пула БД.
    Сессии БД и HTTP-клиенты закрываются контекстными менеджерами обработчика.
    """
    try:
        async with connected_broker() as broker:
            await broker.start()
            logger.info("Consumer запущен")
            await asyncio.Event().wait()
    finally:
        await engine.dispose()


async def main() -> None:
    """Запускает consumer с остановкой по SIGTERM и Ctrl+C."""
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    assert task is not None
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        await run_consumer()
    except asyncio.CancelledError:
        logger.info("Consumer остановлен")
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
