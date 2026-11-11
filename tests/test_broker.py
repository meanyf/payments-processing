"""Проверки доставки через настоящий RabbitMQ."""

import json
from uuid import uuid4

import pytest
from aio_pika import DeliveryMode
from aio_pika.exceptions import DeliveryError
from faststream.rabbit import RabbitQueue
from pamqp.commands import Basic

from payments_processing.messaging import broker as broker_module
from payments_processing.messaging.broker import (
    connected_broker,
    payments_publisher,
)
from payments_processing.services import retries


@pytest.mark.asyncio
async def test_exhausted_message_reaches_dlq_through_named_exchange(monkeypatch):
    """Окончательная ошибка доставляется в DLQ через отдельный обменник."""
    target = RabbitQueue(f"test.dlq.{uuid4().hex}", exclusive=True)
    monkeypatch.setattr(broker_module, "dead_letter_queue", target)
    monkeypatch.setattr(retries, "dead_letter_queue", target)
    async with connected_broker() as broker:
        queue = await broker.declare_queue(target)
        payload = {"payment_id": str(uuid4())}
        await retries.publish_failure(payload, 3)

        message = await queue.get(timeout=5)
        assert message is not None
        async with message.process():
            assert message.exchange == "payments.dlx"
            assert json.loads(message.body) == payload
            assert message.headers["x-attempts"] == 3


@pytest.mark.asyncio
async def test_publish_persistent_message_with_confirmation():
    """Брокер подтверждает публикацию сообщения с сохранением на диск."""
    async with connected_broker() as broker:
        queue = await broker.declare_queue(
            RabbitQueue(f"test.payments.{uuid4().hex}", exclusive=True)
        )
        payload = {"payment_id": str(uuid4())}

        confirmation = await payments_publisher.publish(payload, queue=queue.name)

        assert isinstance(confirmation, Basic.Ack)
        message = await queue.get(timeout=5)
        assert message is not None
        async with message.process():
            assert json.loads(message.body) == payload
            assert message.delivery_mode == DeliveryMode.PERSISTENT


@pytest.mark.asyncio
async def test_publish_to_missing_queue_raises():
    """Недоставленное в очередь сообщение не считается опубликованным."""
    async with connected_broker():
        with pytest.raises(DeliveryError):
            await payments_publisher.publish(
                {"payment_id": str(uuid4())},
                queue=f"missing.payments.{uuid4().hex}",
            )
