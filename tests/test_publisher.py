"""Проверки продолжения работы publisher и остановки процесса."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from payments_processing import publisher


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_first", [False, True])
async def test_publisher_retries_and_closes_connections_on_cancel(monkeypatch, fail_first):
    """Ошибка не останавливает опрос; отмена закрывает сессии, брокер и пул БД."""
    closed_sessions = []
    broker_closed = asyncio.Event()
    publishing = asyncio.Event()
    calls = 0

    @asynccontextmanager
    async def connected_broker():
        try:
            yield
        finally:
            broker_closed.set()

    @asynccontextmanager
    async def session_factory():
        session = object()
        try:
            yield session
        finally:
            closed_sessions.append(session)

    async def publish(session):
        nonlocal calls
        calls += 1
        if fail_first and calls == 1:
            raise TimeoutError("Broker unavailable")
        publishing.set()
        await asyncio.Event().wait()

    dispose = AsyncMock()
    monkeypatch.setattr(publisher, "connected_broker", connected_broker)
    monkeypatch.setattr(publisher, "session_factory", session_factory)
    monkeypatch.setattr(publisher, "publish_pending_events", publish)
    monkeypatch.setattr(publisher, "engine", SimpleNamespace(dispose=dispose))

    task = asyncio.create_task(publisher.run_publisher(poll_interval=0))
    try:
        await asyncio.wait_for(publishing.wait(), timeout=5)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert calls == (2 if fail_first else 1)
    assert len(closed_sessions) == calls
    assert broker_closed.is_set()
    dispose.assert_awaited_once()
