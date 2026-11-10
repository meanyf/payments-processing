"""Проверки получения и разбора событий платежей."""

from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from faststream.rabbit import TestRabbitBroker
from pydantic import ValidationError

from payments_processing.messaging import consumer


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
    context.__aexit__.assert_awaited_once()
    webhook.assert_awaited_once()
    http_context.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [{}, {"payment_id": "invalid"}])
async def test_consumer_rejects_invalid_payment_id(monkeypatch, payload):
    """Без корректного payment_id обработка платежа не начинается."""
    logger = Mock()
    monkeypatch.setattr(consumer, "logger", logger)

    async with TestRabbitBroker(consumer.broker) as broker:
        with pytest.raises(ValidationError):
            await broker.publish(payload, queue="payments.new")

    logger.info.assert_not_called()
