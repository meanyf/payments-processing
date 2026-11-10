"""Отправка клиенту результата платежа по HTTP."""

import httpx

from payments_processing.db.models import Payment


async def send_webhook(client: httpx.AsyncClient, payment: Payment) -> None:
    """Отправляет результат платежа на указанный клиентом URL.

    Args:
        client: HTTP-клиент для отправки уведомления.
        payment: Платёж с сохранённым результатом обработки.

    Raises:
        httpx.HTTPStatusError: Если получен ответ вне диапазона 2xx.
        httpx.RequestError: При таймауте или другой ошибке HTTP-запроса.
    """
    response = await client.post(
        payment.webhook_url,
        json={"payment_id": str(payment.id), "status": payment.status.value},
        timeout=10,
        follow_redirects=False,
    )
    response.raise_for_status()
