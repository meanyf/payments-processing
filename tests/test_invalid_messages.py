"""Проверки изоляции некорректных сообщений."""

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
