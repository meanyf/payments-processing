"""Проверки изоляции некорректных сообщений."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from payments_processing.messaging import consumer


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body,headers",
    [
        (b"{", {}),
        (b"{}", {}),
        (b'{"payment_id":"abc"}', {}),
        (b'{"payment_id":"00000000-0000-0000-0000-000000000001"}', {"x-attempts": -1}),
    ],
)
async def test_invalid_message_ack_after_transfer(monkeypatch, body, headers):
    """Некорректное сообщение подтверждается только после передачи в DLQ."""
    message = AsyncMock(headers=headers)

    async def transfer(*args):
        message.ack.assert_not_awaited()

    publish = AsyncMock(side_effect=transfer)
    monkeypatch.setattr(consumer, "publish_invalid", publish, raising=False)
    process = AsyncMock()
    monkeypatch.setattr(consumer, "handle_payment", process)
    await consumer.receive_payment(body, message)
    publish.assert_awaited_once()
    message.ack.assert_awaited_once()
    process.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_transfer_failure_keeps_original(monkeypatch):
    """Сбой DLQ возвращает исходное сообщение без ACK."""
    monkeypatch.setattr(
        consumer, "publish_invalid", AsyncMock(side_effect=TimeoutError)
    )
    message = AsyncMock(headers={})
    await consumer.receive_payment(b"{", message)
    message.ack.assert_not_awaited()
    message.nack.assert_awaited_once_with(requeue=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body,headers",
    [
        (b"{", {}),
        (b"{}", {}),
        (b'{"payment_id":"abc"}', {}),
        (
            b'{"payment_id":"00000000-0000-0000-0000-000000000001"}',
            {"x-attempts": "abc"},
        ),
        (b'{"payment_id":"00000000-0000-0000-0000-000000000001"}', {"x-attempts": -1}),
    ],
)
async def test_real_broker_moves_invalid_body_to_dlq(monkeypatch, body, headers):
    """Настоящий брокер доставляет исходные байты в DLQ и получает ACK."""
    await assert_real_dlq_transfer(monkeypatch, body, headers)


@pytest.mark.asyncio
@pytest.mark.parametrize("exists", [False, True])
async def test_real_broker_moves_permanent_failure_to_dlq(monkeypatch, session, exists):
    """Отсутствующий платёж и несовместимый повтор не остаются в рабочей очереди."""
    import json
    from datetime import UTC, datetime, timedelta
    from uuid import uuid4

    from payments_processing.db.models import AttemptState, Currency, Payment

    payment_id = uuid4()
    if exists:
        async with session.begin():
            session.add(
                Payment(
                    id=payment_id,
                    amount=1,
                    currency=Currency.RUB,
                    description="test",
                    idempotency_key=str(uuid4()),
                    webhook_url="https://example.com/webhook",
                    attempt_state=AttemptState.WAITING_RETRY,
                    processing_attempts=1,
                    next_attempt_at=datetime.now(UTC) + timedelta(seconds=4),
                )
            )

    @asynccontextmanager
    async def test_session():
        yield session

    @asynccontextmanager
    async def lock(payment_id):
        yield True

    monkeypatch.setattr(consumer, "session_factory", test_session)
    monkeypatch.setattr(consumer, "lock_payment", lock)
    await assert_real_dlq_transfer(
        monkeypatch,
        json.dumps({"payment_id": str(payment_id)}).encode(),
        {"x-attempts": 2},
    )


async def assert_real_dlq_transfer(monkeypatch, body, headers):
    """Проверяет исходное тело в DLQ, ACK и отсутствие сообщения в источнике."""
    import asyncio
    from uuid import uuid4

    from faststream import AckPolicy
    from faststream.rabbit import RabbitBroker, RabbitQueue
    from faststream.rabbit.annotations import RabbitMessage

    from payments_processing.config import get_settings
    from payments_processing.messaging import broker as topology

    source = RabbitQueue(f"test.invalid.{uuid4().hex}", exclusive=True)
    target = RabbitQueue(f"test.invalid.dlq.{uuid4().hex}", exclusive=True)
    monkeypatch.setattr(topology, "dead_letter_queue", target)
    done = asyncio.Event()
    received = []
    local = RabbitBroker(str(get_settings().rabbitmq_url))

    async def handler(body: bytes, message: RabbitMessage):
        await consumer.receive_payment(body, message)
        received.append(message.raw_message.processed)
        done.set()

    local.subscriber(source, decoder=consumer.raw_body, ack_policy=AckPolicy.MANUAL)(
        handler
    )
    async with topology.connected_broker() as publisher:
        queue = await publisher.declare_queue(target)
        async with local:
            await local.start()
            await publisher.publish(
                body, queue=source, headers=headers, content_type="application/json"
            )
            await asyncio.wait_for(done.wait(), timeout=5)
            result = await queue.get(timeout=5)
            assert result is not None
            async with result.process():
                assert result.body == body
                assert result.exchange == "payments.dlx"
                assert result.headers["x-validation-error"]
                for key, value in headers.items():
                    assert result.headers[key] == value
            assert received == [True]
            source_queue = await local.declare_queue(source)
            assert await source_queue.get(fail=False) is None
