"""Базовый класс моделей SQLAlchemy."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Общая база моделей и метаданных таблиц."""
