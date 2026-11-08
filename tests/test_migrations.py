"""Проверки применения и отката миграций PostgreSQL."""

import pytest
from sqlalchemy import inspect, text

from alembic import command


@pytest.mark.asyncio
async def test_upgrade_downgrade_and_upgrade_again(connection, alembic_config):
    """Миграция создаёт таблицы, полностью откатывается и применяется повторно."""

    def check_cycle(sync_connection):
        alembic_config.attributes["connection"] = sync_connection
        command.upgrade(alembic_config, "head")
        assert set(inspect(sync_connection).get_table_names()) == {
            "payments",
            "outbox",
            "alembic_version",
        }
        command.check(alembic_config)

        command.downgrade(alembic_config, "base")
        assert set(inspect(sync_connection).get_table_names()) == {"alembic_version"}
        enum_count = sync_connection.scalar(
            text(
                "SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace "
                "WHERE n.nspname = current_schema() AND t.typtype = 'e'"
            )
        )
        assert enum_count == 0

        command.upgrade(alembic_config, "head")
        command.check(alembic_config)
        assert set(inspect(sync_connection).get_table_names()) == {
            "payments",
            "outbox",
            "alembic_version",
        }

    await connection.run_sync(check_cycle)
