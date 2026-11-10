"""Проверки сохранения результата обработки платежа."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from alembic import command
from payments_processing.config import Settings
from payments_processing.db.models import Currency, Payment, PaymentStatus
from payments_processing.services import payments


@pytest_asyncio.fixture
async def processing_sessions(alembic_config):
    """Предоставляет независимые сессии в отдельной схеме PostgreSQL."""
    schema = "test_processing_" + uuid4().hex
    url = str(Settings().database_url)
    admin_engine = create_async_engine(url)
    engine = create_async_engine(
        url,
        connect_args={"server_settings": {"search_path": schema + ",pg_catalog"}},
    )

    def upgrade(connection):
        alembic_config.attributes["connection"] = connection
        command.upgrade(alembic_config, "head")

    try:
        async with admin_engine.begin() as connection:
            await connection.exec_driver_sql("CREATE SCHEMA " + schema)
        async with engine.begin() as connection:
            await connection.run_sync(upgrade)
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.exec_driver_sql("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
        await admin_engine.dispose()


@pytest.fixture
def new_payment():
    """Возвращает новый платёж для обработки."""
    return Payment(
        amount=Decimal("100.50"),
        currency=Currency.RUB,
        description="Оплата заказа",
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhook",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_processes_pending_payment_once(session, new_payment, monkeypatch, status):
    """Результат и дата сохраняются, повтор не вызывает шлюз заново."""
    async with session.begin():
        session.add(new_payment)
    payment_id = new_payment.id
    gateway = AsyncMock(return_value=status)
    monkeypatch.setattr(payments, "simulate_payment", gateway)
    started_at = datetime.now(UTC)

    result = await payments.process_payment(session, payment_id)
    processed_at = result.processed_at
    assert not session.in_transaction()
    await session.refresh(result)
    assert result.status == status
    assert started_at <= result.processed_at <= datetime.now(UTC)
    await session.rollback()

    repeated = await payments.process_payment(session, payment_id)

    assert repeated.status == status
    assert repeated.processed_at == processed_at
    gateway.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_gateway_error_leaves_payment_pending(session, new_payment, monkeypatch):
    """Ошибка шлюза не сохраняет незавершённый результат."""
    async with session.begin():
        session.add(new_payment)
    payment_id = new_payment.id
    monkeypatch.setattr(
        payments, "simulate_payment", AsyncMock(side_effect=RuntimeError("Gateway error"))
    )

    with pytest.raises(RuntimeError, match="Gateway error"):
        await payments.process_payment(session, payment_id)

    saved = await session.get(Payment, payment_id)
    assert saved.status == PaymentStatus.PENDING
    assert saved.processed_at is None


@pytest.mark.asyncio
async def test_parallel_deliveries_share_result(processing_sessions, new_payment, monkeypatch):
    """Два обработчика одного платежа вызывают эмуляцию только один раз."""
    factory = processing_sessions
    async with factory.begin() as session:
        session.add(new_payment)
    entered_gateway = asyncio.Event()
    release_gateway = asyncio.Event()
    second_started = asyncio.Event()

    async def simulate():
        entered_gateway.set()
        await release_gateway.wait()
        return PaymentStatus.SUCCEEDED

    gateway = AsyncMock(side_effect=simulate)
    monkeypatch.setattr(payments, "simulate_payment", gateway)

    async def process_second(session):
        second_started.set()
        return await payments.process_payment(session, new_payment.id)

    async with factory() as first, factory() as second:
        async with asyncio.timeout(5):
            async with asyncio.TaskGroup() as tasks:
                first_task = tasks.create_task(payments.process_payment(first, new_payment.id))
                await entered_gateway.wait()
                second_task = tasks.create_task(process_second(second))
                await second_started.wait()
                release_gateway.set()

    assert first_task.result().status == second_task.result().status == PaymentStatus.SUCCEEDED
    assert first_task.result().processed_at == second_task.result().processed_at
    gateway.assert_awaited_once_with()
