"""Приводит тип состояния к строковому enum модели."""

import sqlalchemy as sa

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Согласует длину столбца с допустимыми состояниями."""
    op.alter_column(
        "payments", "attempt_state", existing_type=sa.Text(), type_=sa.String(13)
    )


def downgrade() -> None:
    """Возвращает неограниченное текстовое поле."""
    op.alter_column(
        "payments", "attempt_state", existing_type=sa.String(13), type_=sa.Text()
    )
