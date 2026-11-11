"""Сквозная проверка API, outbox, RabbitMQ и HTTP webhook."""

import asyncio
import json
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from faststream import AckPolicy
from faststream.rabbit import Channel, RabbitBroker, RabbitQueue
from faststream.rabbit.annotations import RabbitMessage
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from alembic import command
from payments_processing.config import Settings, get_settings
from payments_processing.db.models import AttemptState, Outbox, Payment
from payments_processing.db.session import get_session
from payments_processing.main import app
from payments_processing.messaging import consumer
from payments_processing.services import outbox, payment_lock, retries


@pytest_asyncio.fixture
async def e2e_database(alembic_config, monkeypatch):
    """Создаёт схему с коммитами, видимыми независимым сессиям всех участников."""
    schema = "test_e2e_" + uuid4().hex
    admin = create_async_engine(str(Settings().database_url))
    engine = create_async_engine(
        str(Settings().database_url),
        connect_args={"server_settings": {"search_path": schema + ",pg_catalog"}},
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)

    def upgrade(connection):
        alembic_config.attributes["connection"] = connection
        command.upgrade(alembic_config, "head")

    async def api_session():
        async with factory() as session:
            yield session

    try:
        async with admin.begin() as connection:
            await connection.exec_driver_sql("CREATE SCHEMA " + schema)
        async with engine.begin() as connection:
            await connection.run_sync(upgrade)
        for module in (consumer, payment_lock, retries):
            monkeypatch.setattr(module, "session_factory", factory)
        app.dependency_overrides[get_session] = api_session
        yield factory
    finally:
        app.dependency_overrides.pop(get_session, None)
        await engine.dispose()
        try:
            async with admin.begin() as connection:
                await connection.exec_driver_sql(
                    "DROP SCHEMA IF EXISTS " + schema + " CASCADE"
                )
        finally:
            await admin.dispose()


@pytest_asyncio.fixture
async def webhook_receiver():
    """Принимает настоящий HTTP POST на свободном локальном порту."""
    received = asyncio.get_running_loop().create_future()

    async def handle(reader, writer):
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            lines = request.decode("ascii").split("\r\n")
            headers = dict(
                line.lower().split(":", 1) for line in lines[1:] if ":" in line
            )
            body = await reader.readexactly(int(headers["content-length"]))
            result = (lines[0], headers, json.loads(body))
            writer.write(
                b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"
            )
            await writer.drain()
            received.set_result(result)
        except Exception as error:
            if not received.done():
                received.set_exception(error)
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    async with server:
        port = server.sockets[0].getsockname()[1]
        yield f"http://127.0.0.1:{port}/webhook", received


@pytest.mark.asyncio
async def test_payment_reaches_http_webhook_through_outbox_and_rabbitmq(
    e2e_database, webhook_receiver, monkeypatch
):
    """Созданный через API платёж проходит реальный брокер и завершается с ACK."""
    factory = e2e_database
    webhook_url, received = webhook_receiver
    queue = RabbitQueue(f"test.e2e.{uuid4().hex}", exclusive=True)
    broker = RabbitBroker(
        str(get_settings().rabbitmq_url),
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )
    publisher = broker.publisher(queue, persist=True, mandatory=True, timeout=10)
    monkeypatch.setattr(outbox, "payments_publisher", publisher)
    completed = asyncio.get_running_loop().create_future()

    async def handler(body: bytes, message: RabbitMessage):
        await consumer.receive_payment(body, message)
        if not completed.done():
            completed.set_result(message.raw_message.processed)

    broker.subscriber(queue, decoder=consumer.raw_body, ack_policy=AckPolicy.MANUAL)(
        handler
    )
    async with (
        asyncio.timeout(30),
        broker,
        AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={
                "X-API-Key": get_settings().api_key,
                "Idempotency-Key": uuid4().hex,
            },
        ) as client,
    ):
        await broker.start()
        response = await client.post(
            "/api/v1/payments",
            json={
                "amount": "100.50",
                "currency": "RUB",
                "description": "Сквозная проверка платежа",
                "metadata": {"order_id": "e2e"},
                "webhook_url": webhook_url,
            },
        )
        assert response.status_code == 202
        created = response.json()
        assert created["status"] == "pending"
        async with factory() as session:
            event = (await session.scalars(select(Outbox))).one()
            assert event.payload == {"payment_id": created["payment_id"]}
            assert event.published_at is None
        assert not received.done()

        async with factory() as session:
            assert await outbox.publish_pending_events(session) == 1

        request_line, headers, payload = await received
        assert request_line == "POST /webhook HTTP/1.1"
        assert headers["content-type"].strip() == "application/json"
        assert payload["payment_id"] == created["payment_id"]
        assert payload["status"] in {"succeeded", "failed"}
        assert await completed is True

        async with factory() as session:
            payment = await session.get(Payment, UUID(created["payment_id"]))
            assert payment.status.value == payload["status"]
            assert payment.processed_at is not None
            assert payment.webhook_delivered_at is not None
            assert payment.attempt_state == AttemptState.DELIVERED
            assert payment.processing_attempts == 1
            event = (await session.scalars(select(Outbox))).one()
            assert event.published_at is not None
        async with factory() as session:
            assert await outbox.publish_pending_events(session) == 0

        response = await client.get("/api/v1/payments/" + created["payment_id"])
        assert response.status_code == 200
        assert response.json()["status"] == payload["status"]
        assert response.json()["processed_at"] is not None
        source = await broker.declare_queue(queue)
        assert await source.get(fail=False) is None
