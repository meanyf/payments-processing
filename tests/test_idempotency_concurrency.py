"""Проверки идемпотентности при параллельных запросах к PostgreSQL."""

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from alembic import command
from payments_processing.config import Settings, get_settings
from payments_processing.db.models import Outbox, Payment
from payments_processing.db.session import get_session
from payments_processing.main import app


@pytest_asyncio.fixture
async def concurrent_api_client(alembic_config, monkeypatch):
    """Использует отдельную схему и независимую транзакцию для каждого запроса."""
    monkeypatch.setenv("API_KEY", "test-api-key")
    get_settings.cache_clear()
    schema = "test_concurrency_" + uuid4().hex
    admin_engine = create_async_engine(str(Settings().database_url))
    engine = create_async_engine(
        str(Settings().database_url),
        connect_args={"server_settings": {"search_path": schema + ",pg_catalog"}},
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def override_session():
        async with factory() as session:
            yield session

    def upgrade(connection):
        alembic_config.attributes["connection"] = connection
        command.upgrade(alembic_config, "head")

    try:
        async with admin_engine.begin() as connection:
            await connection.exec_driver_sql("CREATE SCHEMA " + schema)
        async with engine.begin() as connection:
            await connection.run_sync(upgrade)
        app.dependency_overrides[get_session] = override_session
        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://test",
            headers={"X-API-Key": "test-api-key", "Idempotency-Key": "same-key"},
        ) as client:
            yield client, factory
    finally:
        app.dependency_overrides.pop(get_session, None)
        get_settings.cache_clear()
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.exec_driver_sql("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
        await admin_engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("different_data", [False, True])
async def test_concurrent_requests_create_one_payment_and_event(
    concurrent_api_client, different_data
):
    """Конкурирующие запросы создают один платёж; разные данные дают конфликт."""
    client, factory = concurrent_api_client
    payload = {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Оплата заказа",
        "metadata": {},
        "webhook_url": "https://example.com/webhook",
    }
    second_payload = {**payload, "amount": "200.00"} if different_data else payload
    responses = await asyncio.gather(
        client.post("/api/v1/payments", json=payload),
        client.post("/api/v1/payments", json=second_payload),
    )

    assert sorted(response.status_code for response in responses) == (
        [202, 409] if different_data else [202, 202]
    )
    if not different_data:
        assert responses[0].json() == responses[1].json()
    async with factory() as session:
        payments = (await session.scalars(select(Payment))).all()
        events = (await session.scalars(select(Outbox))).all()
        assert len(payments) == len(events) == 1
        assert events[0].payload == {"payment_id": str(payments[0].id)}
