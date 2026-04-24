"""add prediction_explanation column to matches

Revision ID: 9a4c7b3e1d22
Revises: 2b8c0c3d2c1a
Create Date: 2026-04-24 12:00:00.000000+00:00

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# Revision identifiers, used by Alembic
revision: str = "9a4c7b3e1d22"
down_revision: str | None = "2b8c0c3d2c1a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "matches",
        sa.Column(
            "prediction_explanation",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("matches", "prediction_explanation")
