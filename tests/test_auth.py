"""Проверки аутентификации API статическим ключом."""

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from payments_processing.config import Settings, get_settings
from payments_processing.main import app


@pytest.fixture
def protected_app(monkeypatch):
    """Добавляет тестовый маршрут для проверки общей защиты приложения."""
    monkeypatch.setenv("API_KEY", "test-api-key")
    get_settings.cache_clear()
    original_routes = list(app.router.routes)

    async def endpoint():
        return {"ok": True}

    app.add_api_route("/_test/auth", endpoint, methods=["GET"])
    try:
        yield app
    finally:
        app.router.routes[:] = original_routes
        get_settings.cache_clear()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("key", "expected_status"),
    [("test-api-key", 200), ("wrong-key", 401), (None, 401)],
)
async def test_api_key_authentication(protected_app, key, expected_status):
    """Доступ разрешён только при передаче правильного X-API-Key."""
    headers = {} if key is None else {"X-API-Key": key}
    async with AsyncClient(
        transport=ASGITransport(app=protected_app), base_url="http://test"
    ) as client:
        response = await client.get("/_test/auth", headers=headers)

    assert response.status_code == expected_status


def test_settings_read_api_key(monkeypatch):
    """Ключ читается из переменных окружения."""
    monkeypatch.setenv("API_KEY", "configured-key")
    assert Settings().api_key == "configured-key"


@pytest.mark.parametrize("key", [None, ""])
def test_settings_require_nonempty_api_key(monkeypatch, key):
    """Настройки не допускают отсутствующий или пустой API-ключ."""
    monkeypatch.setenv(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost/payments"
    )
    if key is None:
        monkeypatch.delenv("API_KEY", raising=False)
    else:
        monkeypatch.setenv("API_KEY", key)

    with pytest.raises(ValidationError, match="api_key"):
        Settings(_env_file=None)
