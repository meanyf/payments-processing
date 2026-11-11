FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /usr/local/bin/uv

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY src ./src
RUN uv sync --locked --no-dev --no-editable

COPY alembic.ini ./
COPY alembic ./alembic

RUN useradd --create-home app
USER app

EXPOSE 8000

CMD ["uvicorn", "payments_processing.main:app", "--host", "0.0.0.0", "--port", "8000"]
