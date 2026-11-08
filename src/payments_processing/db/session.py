"""Асинхронное подключение к БД и фабрика сессий."""

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from payments_processing.config import Settings

settings = Settings()

engine = create_async_engine(str(settings.database_url), pool_pre_ping=True)
session_factory = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """Предоставляет сессию БД и закрывает её после завершения запроса.

    Yields:
        Асинхронная сессия БД.
    """
    async with session_factory() as session:
        yield session
