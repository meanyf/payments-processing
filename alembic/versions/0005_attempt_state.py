"""Состояние попытки и срок повторной обработки."""

import sqlalchemy as sa

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Добавляет состояние; незавершённые старые попытки требуют восстановления."""
    op.add_column(
        "payments",
        sa.Column("attempt_state", sa.Text(), nullable=False, server_default="ready"),
    )
    op.add_column("payments", sa.Column("next_attempt_at", sa.DateTime(timezone=True)))
    op.execute(
        "UPDATE payments SET attempt_state = CASE WHEN webhook_delivered_at IS NOT NULL THEN 'delivered' WHEN processing_attempts > 0 THEN 'processing' ELSE 'ready' END"
    )


def downgrade() -> None:
    """Удаляет состояние восстановления попыток."""
    op.drop_column("payments", "next_attempt_at")
    op.drop_column("payments", "attempt_state")
