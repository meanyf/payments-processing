"""Проверки ограничений настроек consumer."""

import pytest
from pydantic import ValidationError

from payments_processing.config import Settings


@pytest.mark.parametrize("value", ["0", "-1", "65536"])
def test_prefetch_rejects_unlimited_or_negative_value(monkeypatch, value):
    """Лимит положителен и укладывается в диапазон поля AMQP."""
    monkeypatch.setenv("CONSUMER_PREFETCH_COUNT", value)
    with pytest.raises(ValidationError, match="consumer_prefetch_count"):
        Settings(_env_file=None)


def test_prefetch_is_configurable(monkeypatch):
    """Лимит задаётся окружением; без него используется четыре сообщения."""
    monkeypatch.delenv("CONSUMER_PREFETCH_COUNT", raising=False)
    assert Settings(_env_file=None).consumer_prefetch_count == 4
    monkeypatch.setenv("CONSUMER_PREFETCH_COUNT", "2")
    assert Settings(_env_file=None).consumer_prefetch_count == 2
