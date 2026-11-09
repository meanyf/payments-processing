"""Настройки приложения из переменных окружения и файла .env."""

from functools import lru_cache

from pydantic import AmqpDsn, Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки подключений к PostgreSQL, RabbitMQ и доступа к API."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: PostgresDsn
    rabbitmq_url: AmqpDsn = AmqpDsn("amqp://payments:payments@localhost:5672/")
    api_key: str = Field(min_length=1, repr=False)


@lru_cache
def get_settings() -> Settings:
    """Возвращает настройки, загруженные один раз на процесс."""
    return Settings()
