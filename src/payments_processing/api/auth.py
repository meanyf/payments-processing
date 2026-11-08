"""Проверка статического API-ключа."""

from secrets import compare_digest
from typing import Annotated

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from payments_processing.config import get_settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(
    api_key: Annotated[str | None, Security(api_key_header)],
) -> None:
    """Проверяет ключ доступа из заголовка запроса.

    Args:
        api_key: Значение заголовка X-API-Key.

    Raises:
        HTTPException: Если ключ отсутствует или неверен.
    """
    if api_key is None or not compare_digest(
        api_key.encode(), get_settings().api_key.encode()
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
