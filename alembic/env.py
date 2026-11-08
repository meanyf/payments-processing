"""Окружение Alembic для асинхронного PostgreSQL."""

import asyncio

from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import context
from payments_processing.config import Settings
from payments_processing.db.models import Payment

config = context.config
target_metadata = Payment.metadata


def run_migrations(connection):
    """Выполняет миграции через синхронный интерфейс соединения."""
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations():
    """Создаёт асинхронное соединение для запуска миграций."""
    engine = create_async_engine(str(Settings().database_url), poolclass=pool.NullPool)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(run_migrations)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    context.configure(
        url=str(Settings().database_url),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()
elif (connection := config.attributes.get("connection")) is not None:
    run_migrations(connection)
else:
    asyncio.run(run_async_migrations())
