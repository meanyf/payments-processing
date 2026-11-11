"""Межпроцессная блокировка обработки одного платежа."""

from contextlib import asynccontextmanager
from uuid import UUID

from sqlalchemy import text

from payments_processing.db.session import session_factory


@asynccontextmanager
async def lock_payment(payment_id: UUID):
    """Удерживает блокировку до завершения обработки, включая webhook.

    Args:
        payment_id: Идентификатор платежа.

    Yields:
        Получена ли блокировка. Занятый платёж не начинает новую попытку.
    """
    key = int.from_bytes(payment_id.bytes[:8], signed=True)
    async with session_factory() as session, session.begin():
        yield await session.scalar(
            text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": key}
        )
