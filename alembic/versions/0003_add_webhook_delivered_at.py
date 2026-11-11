"""Отметка успешной доставки webhook."""

import sqlalchemy as sa

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Добавляет дату доставки уведомления."""
    op.add_column(
        "payments",
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    """Удаляет дату доставки уведомления."""
    op.drop_column("payments", "webhook_delivered_at")
