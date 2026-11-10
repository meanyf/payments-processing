"""Проверки запуска consumer и освобождения ресурсов при остановке."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from payments_processing import consumer


@pytest.mark.asyncio
@pytest.mark.parametrize("startup_fails", [False, True])
async def test_consumer_closes_broker_before_database_pool(monkeypatch, startup_fails):
    """При отмене и ошибке запуска брокер закрывается раньше пула БД."""
    started = asyncio.Event()
    closed = []

    async def start():
        if startup_fails:
            raise RuntimeError("Cannot start consumer")
        started.set()

    @asynccontextmanager
    async def connected_broker():
        try:
            yield SimpleNamespace(start=start)
        finally:
            closed.append("broker")

    async def dispose():
        closed.append("database")

    monkeypatch.setattr(consumer, "connected_broker", connected_broker)
    monkeypatch.setattr(
        consumer, "engine", SimpleNamespace(dispose=AsyncMock(side_effect=dispose))
    )

    if startup_fails:
        with pytest.raises(RuntimeError, match="Cannot start consumer"):
            await consumer.run_consumer()
    else:
        task = asyncio.create_task(consumer.run_consumer())
        try:
            await asyncio.wait_for(started.wait(), timeout=5)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    assert closed == ["broker", "database"]
