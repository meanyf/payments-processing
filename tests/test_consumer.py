"""Проверки получения и разбора событий платежей."""

from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from faststream.rabbit import TestRabbitBroker

from payments_processing.messaging import consumer
from payments_processing.messaging.schemas import PaymentEvent


@pytest.fixture(autouse=True)
def mock_delivery_marker(monkeypatch):
    """Изолирует запись доставки в модульных тестах обработчика."""
    monkeypatch.setattr(consumer, "mark_webhook_delivered", AsyncMock())


@pytest.mark.asyncio
async def test_consumer_parses_payment_id(monkeypatch):
    """Сообщение из payments.new поступает обработчику с UUID платежа."""
    logger = Mock()
    monkeypatch.setattr(consumer, "logger", logger)
    payment_id = uuid4()
    session = object()
    context = AsyncMock()
    context.__aenter__.return_value = session
    monkeypatch.setattr(consumer, "session_factory", Mock(return_value=context))
    process = AsyncMock()
    monkeypatch.setattr(consumer, "process_payment", process)
    monkeypatch.setattr(consumer, "begin_attempt", AsyncMock(return_value=1))
    http_context = AsyncMock()
    monkeypatch.setattr(consumer.httpx, "AsyncClient", Mock(return_value=http_context))

    async def send(client, payment):
        context.__aexit__.assert_awaited_once()
        assert client is http_context.__aenter__.return_value
        assert payment is process.return_value

    webhook = AsyncMock(side_effect=send)
    monkeypatch.setattr(consumer, "send_webhook", webhook)

    async with TestRabbitBroker(consumer.broker) as broker:
        await broker.publish({"payment_id": str(payment_id)}, queue="payments.new")

    logger.info.assert_called_once_with("Получено сообщение о платеже %s", payment_id)
    process.assert_awaited_once_with(session, payment_id)
    assert context.__aexit__.await_count == 2
    webhook.assert_awaited_once()
    http_context.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [{}, {"payment_id": "invalid"}])
async def test_consumer_rejects_invalid_payment_id(monkeypatch, payload):
    """Без корректного payment_id обработка платежа не начинается."""
    logger = Mock()
    monkeypatch.setattr(consumer, "logger", logger)

    monkeypatch.setattr(consumer, "publish_invalid", AsyncMock())
    async with TestRabbitBroker(consumer.broker) as broker:
        await broker.publish(payload, queue="payments.new")

    logger.info.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_at", ["process", "webhook", None])
@pytest.mark.parametrize("attempt", [1, 2, 3])
async def test_consumer_acknowledges_only_after_work_or_transfer(
    monkeypatch, failure_at, attempt
):
    """ACK следует за webhook либо подтверждённой передачей на повтор/DLQ."""
    event = PaymentEvent(payment_id=uuid4())
    message = AsyncMock(headers={"x-attempts": attempt - 1})
    monkeypatch.setattr(consumer, "session_factory", Mock(return_value=AsyncMock()))
    monkeypatch.setattr(consumer.httpx, "AsyncClient", Mock(return_value=AsyncMock()))
    monkeypatch.setattr(consumer, "begin_attempt", AsyncMock(return_value=attempt))
    process = AsyncMock(
        side_effect=RuntimeError("DB error") if failure_at == "process" else None
    )
    monkeypatch.setattr(consumer, "process_payment", process)

    async def send(*args):
        message.ack.assert_not_awaited()
        if failure_at == "webhook":
            raise TimeoutError("Webhook timeout")

    webhook = AsyncMock(side_effect=send)
    monkeypatch.setattr(consumer, "send_webhook", webhook)

    async def transfer(*args):
        message.ack.assert_not_awaited()

    publish = AsyncMock(side_effect=transfer)
    monkeypatch.setattr(consumer, "publish_failure", publish)

    await consumer.handle_payment(event, message)

    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    if failure_at:
        publish.assert_awaited_once_with(event.model_dump(mode="json"), attempt)
    else:
        publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_exhausted_after_crash_goes_to_dlq_without_processing(monkeypatch):
    """После трёх ранее начатых попыток обработка не запускается заново."""
    event = PaymentEvent(payment_id=uuid4())
    message = AsyncMock(headers={})
    monkeypatch.setattr(consumer, "session_factory", Mock(return_value=AsyncMock()))
    monkeypatch.setattr(consumer, "begin_attempt", AsyncMock(return_value=None))
    process = AsyncMock()
    monkeypatch.setattr(consumer, "process_payment", process)
    publish = AsyncMock()
    monkeypatch.setattr(consumer, "publish_failure", publish)

    await consumer.handle_payment(event, message)

    process.assert_not_awaited()
    publish.assert_awaited_once_with(event.model_dump(mode="json"), 3)
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_transfer_error_requeues_original(monkeypatch):
    """Сбой передачи в DLQ не удаляет исходное сообщение."""
    event = PaymentEvent(payment_id=uuid4())
    message = AsyncMock(headers={})
    monkeypatch.setattr(consumer, "session_factory", Mock(return_value=AsyncMock()))
    monkeypatch.setattr(consumer, "begin_attempt", AsyncMock(return_value=None))
    monkeypatch.setattr(
        consumer, "publish_failure", AsyncMock(side_effect=TimeoutError)
    )

    await consumer.handle_payment(event, message)

    message.ack.assert_not_awaited()
    message.nack.assert_awaited_once_with(requeue=True)


@pytest.mark.asyncio
async def test_delivered_duplicate_is_acknowledged_without_work(monkeypatch):
    """Завершённый дубль подтверждается без обработки, webhook и DLQ."""
    monkeypatch.setattr(consumer, "session_factory", Mock(return_value=AsyncMock()))
    monkeypatch.setattr(consumer, "begin_attempt", AsyncMock(return_value=0))
    process = AsyncMock()
    webhook = AsyncMock()
    transfer = AsyncMock()
    monkeypatch.setattr(consumer, "process_payment", process)
    monkeypatch.setattr(consumer, "send_webhook", webhook)
    monkeypatch.setattr(consumer, "publish_failure", transfer)
    message = AsyncMock(headers={})
    await consumer.handle_payment(PaymentEvent(payment_id=uuid4()), message)
    message.ack.assert_awaited_once()
    process.assert_not_awaited()
    webhook.assert_not_awaited()
    transfer.assert_not_awaited()
