"""Проверки доставки через настоящий RabbitMQ."""

import asyncio
import json
from uuid import uuid4

import pytest
from aio_pika import DeliveryMode
from aio_pika.exceptions import DeliveryError
from faststream.rabbit import RabbitQueue
from pamqp.commands import Basic
from sqlalchemy import select

from payments_processing.config import get_settings
from payments_processing.db.models import AttemptState, Currency, Outbox, Payment
from payments_processing.messaging import broker as broker_module
from payments_processing.messaging.broker import (
    connected_broker,
    payments_publisher,
)
from payments_processing.services import outbox, retries


@pytest.mark.asyncio
async def test_consumer_receives_no_more_than_configured_prefetch():
    """При достижении лимита брокер ждёт ACK перед следующей доставкой."""
    limit = get_settings().consumer_prefetch_count
    async with connected_broker() as broker:
        queue = await broker.declare_queue(
            RabbitQueue(f"test.prefetch.{uuid4().hex}", exclusive=True)
        )
        received = asyncio.Queue()
        tag = await queue.consume(received.put)
        try:
            for _ in range(limit + 1):
                await broker.publish(b"payment", queue=queue.name)
            pending = [
                await asyncio.wait_for(received.get(), timeout=5) for _ in range(limit)
            ]
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(received.get(), timeout=0.2)
            await pending.pop().ack()
            next_message = await asyncio.wait_for(received.get(), timeout=5)
            await next_message.ack()
            for message in pending:
                await message.ack()
        finally:
            await queue.cancel(tag)


@pytest.mark.asyncio
async def test_exhausted_message_reaches_dlq_through_named_exchange(
    monkeypatch, session
):
    """Окончательная ошибка доставляется в DLQ через отдельный обменник."""
    target = RabbitQueue(f"test.dlq.{uuid4().hex}", exclusive=True)
    monkeypatch.setattr(broker_module, "dead_letter_queue", target)
    monkeypatch.setattr(outbox, "dead_letter_queue", target)
    async with connected_broker() as broker:
        queue = await broker.declare_queue(target)
        payment = Payment(
            amount=1,
            currency=Currency.RUB,
            description="test",
            idempotency_key=uuid4().hex,
            webhook_url="https://example.com",
            processing_attempts=3,
            attempt_state=AttemptState.PROCESSING,
        )
        async with session.begin():
            session.add(payment)
        payload = {"payment_id": str(payment.id)}
        await retries.exhaust_attempts(session, payload, 3)
        assert await outbox.publish_pending_events(session) == 1

        message = await queue.get(timeout=5)
        assert message is not None
        async with message.process():
            assert message.exchange == "payments.dlx"
            assert json.loads(message.body) == payload
            assert message.headers["x-attempts"] == 3
            assert message.delivery_mode == DeliveryMode.PERSISTENT
        saved = (await session.scalars(select(Outbox))).one()
        assert saved.published_at is not None


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
