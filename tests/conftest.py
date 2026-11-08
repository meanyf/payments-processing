"""Общие фикстуры интеграционных тестов PostgreSQL."""

from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from alembic import command
from payments_processing.config import Settings


@pytest.fixture
def alembic_config():
    """Возвращает конфигурацию миграций проекта."""
    return Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))


@pytest_asyncio.fixture
async def connection():
    """Предоставляет соединение в отдельной схеме с откатом всех изменений."""
    engine = create_async_engine(str(Settings().database_url))
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                schema = "test_models_" + uuid4().hex
                await connection.exec_driver_sql("CREATE SCHEMA " + schema)
                await connection.exec_driver_sql(
                    "SET LOCAL search_path TO " + schema + ", pg_catalog"
                )
                yield connection
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def session(connection, alembic_config):
    """Предоставляет сессию с таблицами, созданными миграциями."""

    def upgrade(sync_connection):
        alembic_config.attributes["connection"] = sync_connection
        command.upgrade(alembic_config, "head")

    await connection.run_sync(upgrade)
    async with AsyncSession(bind=connection) as db_session:
        yield db_session
