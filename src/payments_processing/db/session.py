"""Асинхронное подключение к БД и фабрика сессий."""

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from payments_processing.config import Settings

settings = Settings()

engine = create_async_engine(str(settings.database_url), pool_pre_ping=True)
session_factory = async_sessionmaker(engine, expire_on_commit=False)
