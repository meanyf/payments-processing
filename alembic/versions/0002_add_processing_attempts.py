"""Счётчик попыток обработки платежа."""

import sqlalchemy as sa

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Добавляет сохраняемый счётчик попыток с начальным значением ноль."""
    op.add_column(
        "payments",
        sa.Column(
            "processing_attempts", sa.Integer(), server_default="0", nullable=False
        ),
    )


def downgrade() -> None:
    """Удаляет счётчик попыток обработки."""
    op.drop_column("payments", "processing_attempts")
