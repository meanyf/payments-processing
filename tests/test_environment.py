"""Проверки изоляции тестов от настроек приложения."""

import os

import pytest
from environment import configure_test_environment


def test_missing_test_urls_refuses_application_connections(monkeypatch):
    """Рабочие URL не используются при отсутствии тестовых настроек."""
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "production-database")
    with pytest.raises(RuntimeError, match="compose.test.yaml"):
        configure_test_environment()
    assert os.environ["DATABASE_URL"] == "production-database"


def test_test_urls_override_application_settings(monkeypatch):
    """Даже явно заданные рабочие подключения заменяются тестовыми."""
    monkeypatch.setenv("TEST_DATABASE_URL", "test-database")
    monkeypatch.setenv("TEST_RABBITMQ_URL", "test-broker")
    monkeypatch.setenv("DATABASE_URL", "production-database")
    monkeypatch.setenv("RABBITMQ_URL", "production-broker")
    configure_test_environment()
    assert os.environ["DATABASE_URL"] == "test-database"
    assert os.environ["RABBITMQ_URL"] == "test-broker"
