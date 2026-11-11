"""Подключение к RabbitMQ и публикация событий новых платежей."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from faststream.rabbit import Channel, RabbitBroker, RabbitExchange, RabbitQueue

from payments_processing.config import get_settings

broker = RabbitBroker(
    str(get_settings().rabbitmq_url),
    default_channel=Channel(
        prefetch_count=get_settings().consumer_prefetch_count,
        publisher_confirms=True,
        on_return_raises=True,
    ),
)
payments_queue = RabbitQueue("payments.new", durable=True)
dead_letter_queue = RabbitQueue("payments.dlq", durable=True)
dead_letter_exchange = RabbitExchange("payments.dlx", durable=True)
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
        exchange = await broker.declare_exchange(dead_letter_exchange)
        dlq = await broker.declare_queue(dead_letter_queue)
        await dlq.bind(exchange, routing_key=dead_letter_queue.name)
        yield broker
