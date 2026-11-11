"""Отправка клиенту результата платежа по HTTP."""

from uuid import UUID

import httpx
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

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


async def mark_webhook_delivered(session: AsyncSession, payment_id: UUID) -> None:
    """Сохраняет факт доставки уведомления перед подтверждением сообщения.

    Args:
        session: Сессия БД без активной транзакции.
        payment_id: Идентификатор уведомлённого платежа.
    """
    async with session.begin():
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(webhook_delivered_at=func.now())
        )
