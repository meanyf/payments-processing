"""Интеграционные проверки создания платежей через API."""

from uuid import UUID

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event, select

from payments_processing.config import get_settings
from payments_processing.db.models import Outbox, Payment, PaymentStatus
from payments_processing.db.session import get_session
from payments_processing.main import app


@pytest.fixture
def payment_payload():
    """Возвращает корректный запрос создания платежа."""
    return {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Оплата заказа",
        "metadata": {"order_id": "123"},
        "webhook_url": "https://example.com/webhook",
    }


@pytest_asyncio.fixture
async def api_client(session, monkeypatch):
    """Предоставляет HTTP-клиент с изолированной БД и тестовым API-ключом."""
    monkeypatch.setenv("API_KEY", "test-api-key")
    get_settings.cache_clear()

    async def override_session():
        yield session

    app.dependency_overrides[get_session] = override_session
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://test",
            headers={"X-API-Key": "test-api-key", "Idempotency-Key": "order-123"},
        ) as client:
            yield client
    finally:
        app.dependency_overrides.pop(get_session)
        get_settings.cache_clear()


@pytest.mark.asyncio
async def test_create_payment_saves_payment_and_outbox(api_client, session, payment_payload):
    """API сохраняет платёж и событие, возвращая идентификатор и статус pending."""
    response = await api_client.post("/api/v1/payments", json=payment_payload)

    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert body["status"] == "pending"
    payment = await session.get(Payment, UUID(body["payment_id"]))
    assert str(payment.amount) == "100.50"
    assert payment.currency == "RUB"
    assert payment.description == payment_payload["description"]
    assert payment.payment_metadata == payment_payload["metadata"]
    assert payment.webhook_url == payment_payload["webhook_url"]
    assert payment.idempotency_key == "order-123"
    assert payment.created_at.isoformat() == body["created_at"].replace("Z", "+00:00")
    outbox = (await session.scalars(select(Outbox))).one()
    assert outbox.event_type == "payments.new"
    assert outbox.payload == {"payment_id": body["payment_id"]}
    assert outbox.published_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount",
    [
        "0",
        "-1",
        "1.001",
        "10000000000000000",
    ],
)
async def test_create_payment_rejects_invalid_amount(api_client, payment_payload, amount):
    """API отклоняет неположительные суммы и превышение точности БД."""
    payment_payload["amount"] = amount
    response = await api_client.post("/api/v1/payments", json=payment_payload)
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("key", [None, ""])
async def test_create_payment_requires_idempotency_key(api_client, payment_payload, key):
    """Создание платежа требует непустой Idempotency-Key."""
    api_client.headers.pop("Idempotency-Key")
    headers = {} if key is None else {"Idempotency-Key": key}
    response = await api_client.post(
        "/api/v1/payments", json=payment_payload, headers=headers
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_payment_requires_api_key(api_client, payment_payload):
    """Реальный маршрут создания защищён API-ключом."""
    api_client.headers.pop("X-API-Key")
    response = await api_client.post("/api/v1/payments", json=payment_payload)
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_outbox_failure_rolls_back_payment(api_client, session, payment_payload):
    """Ошибка сохранения outbox откатывает также вставку платежа."""

    def fail_outbox_insert(mapper, connection, target):
        raise RuntimeError("Ошибка записи outbox")

    event.listen(Outbox, "before_insert", fail_outbox_insert)
    try:
        response = await api_client.post("/api/v1/payments", json=payment_payload)
    finally:
        event.remove(Outbox, "before_insert", fail_outbox_insert)

    assert response.status_code == 500
    assert (await session.scalars(select(Payment))).all() == []
    assert (await session.scalars(select(Outbox))).all() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("payment_status", [PaymentStatus.PENDING, PaymentStatus.SUCCEEDED])
async def test_repeated_request_returns_existing_payment(
    api_client, session, payment_payload, payment_status
):
    """Повтор возвращает существующий платёж без новых записей в БД."""
    first = await api_client.post("/api/v1/payments", json=payment_payload)
    assert first.status_code == 202
    async with session.begin():
        payment = await session.get(Payment, UUID(first.json()["payment_id"]))
        payment.status = payment_status

    payment_payload["amount"] = "100.500"
    repeated = await api_client.post("/api/v1/payments", json=payment_payload)

    assert repeated.status_code == 202
    assert repeated.json() == {**first.json(), "status": payment_status.value}
    assert len((await session.scalars(select(Payment))).all()) == 1
    assert len((await session.scalars(select(Outbox))).all()) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "200.00"),
        ("currency", "USD"),
        ("description", "Другой заказ"),
        ("metadata", {"order_id": "456"}),
        ("webhook_url", "https://example.com/another-webhook"),
    ],
)
async def test_repeated_key_with_different_data_returns_conflict(
    api_client, session, payment_payload, field, value
):
    """Изменение данных при том же ключе даёт 409 без создания дублей."""
    first = await api_client.post("/api/v1/payments", json=payment_payload)
    assert first.status_code == 202
    payment_payload[field] = value

    response = await api_client.post("/api/v1/payments", json=payment_payload)

    assert response.status_code == 409
    assert len((await session.scalars(select(Payment))).all()) == 1
    assert len((await session.scalars(select(Outbox))).all()) == 1
