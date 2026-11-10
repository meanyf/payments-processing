"""Проверки HTTP-уведомлений о результате платежа."""

import json
from uuid import uuid4

import httpx
import pytest

from payments_processing.db.models import Payment, PaymentStatus
from payments_processing.services.webhook import send_webhook


@pytest.fixture
def payment():
    """Возвращает платёж с готовым результатом и адресом уведомления."""
    return Payment(
        id=uuid4(),
        status=PaymentStatus.SUCCEEDED,
        webhook_url="https://example.com/webhook",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [200, 204, 299, 302, 400, 500])
async def test_webhook_sends_result_and_requires_2xx(payment, status_code):
    """Отправляется POST с результатом; только 2xx означает доставку."""
    def respond(request):
        assert request.method == "POST"
        assert str(request.url) == payment.webhook_url
        assert json.loads(request.content) == {
            "payment_id": str(payment.id),
            "status": "succeeded",
        }
        assert all(value == 10 for value in request.extensions["timeout"].values())
        return httpx.Response(status_code)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        if 200 <= status_code < 300:
            await send_webhook(client, payment)
        else:
            with pytest.raises(httpx.HTTPStatusError):
                await send_webhook(client, payment)


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [httpx.ReadTimeout, httpx.ConnectError])
async def test_webhook_propagates_delivery_error(payment, error):
    """Таймауты и ошибки соединения передаются вызывающему обработчику."""
    def respond(request):
        raise error("Endpoint unavailable", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(error):
            await send_webhook(client, payment)
