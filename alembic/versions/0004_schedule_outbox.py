"""Отложенная публикация повторов из outbox."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Добавляет срок отправки и заголовки сообщения."""
    op.add_column(
        "outbox",
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.add_column(
        "outbox",
        sa.Column("headers", postgresql.JSONB(), nullable=False, server_default="{}"),
    )


def downgrade() -> None:
    """Удаляет поля отложенной публикации."""
    op.drop_column("outbox", "headers")
    op.drop_column("outbox", "available_at")
