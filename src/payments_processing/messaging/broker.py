"""Подключение к RabbitMQ и публикация событий новых платежей."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from faststream.rabbit import Channel, RabbitBroker, RabbitQueue

from payments_processing.config import get_settings

broker = RabbitBroker(
    str(get_settings().rabbitmq_url),
    default_channel=Channel(publisher_confirms=True, on_return_raises=True),
)
payments_queue = RabbitQueue("payments.new", durable=True)
payments_publisher = broker.publisher(
    payments_queue,
    persist=True,
    mandatory=True,
    timeout=10,
)


@asynccontextmanager
async def connected_broker() -> AsyncGenerator[RabbitBroker]:
    """Подключается к брокеру, объявляет очередь и закрывает соединение.

    Yields:
        Брокер с готовой очередью платежей.
    """
    async with broker:
        await broker.declare_queue(payments_queue)
        yield broker
