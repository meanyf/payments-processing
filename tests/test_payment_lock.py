"""Проверки блокировки параллельных доставок в PostgreSQL."""

from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from payments_processing.config import Settings
from payments_processing.services import payment_lock


@pytest.mark.asyncio
async def test_lock_excludes_duplicates_and_releases_after_error(monkeypatch):
    """Дубль не получает блокировку; исключение освобождает её."""
    engine = create_async_engine(str(Settings().database_url))
    monkeypatch.setattr(payment_lock, "session_factory", async_sessionmaker(engine))
    payment_id = uuid4()
    try:
        with pytest.raises(RuntimeError):
            async with payment_lock.lock_payment(payment_id) as acquired:
                assert acquired
                async with payment_lock.lock_payment(payment_id) as duplicate:
                    assert not duplicate
                async with payment_lock.lock_payment(uuid4()) as independent:
                    assert independent
                raise RuntimeError("consumer failed")
        async with payment_lock.lock_payment(payment_id) as acquired:
            assert acquired
    finally:
        await engine.dispose()
