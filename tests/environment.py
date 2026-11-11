"""Настройка подключений исключительно к тестовой инфраструктуре."""

import os


def configure_test_environment() -> None:
    """Устанавливает тестовые подключения до импорта модулей приложения.

    Raises:
        RuntimeError: Если подключения к тестовой инфраструктуре не заданы.
    """
    required = ("TEST_DATABASE_URL", "TEST_RABBITMQ_URL")
    if any(not os.environ.get(name) for name in required):
        raise RuntimeError(
            "Запустите тесты через compose.test.yaml: нужны TEST_DATABASE_URL и TEST_RABBITMQ_URL"
        )
    os.environ["DATABASE_URL"] = os.environ["TEST_DATABASE_URL"]
    os.environ["RABBITMQ_URL"] = os.environ["TEST_RABBITMQ_URL"]
    os.environ["API_KEY"] = "test-api-key"
