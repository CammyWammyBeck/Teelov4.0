"""add composite index on matches status and dates

Revision ID: b916a7c0890a
Revises: e5f6a7b8c9d0
Create Date: 2026-03-07 23:21:56.299115+00:00

"""
from typing import Sequence, Union

from alembic import op

# Revision identifiers, used by Alembic
revision: str = 'b916a7c0890a'
down_revision: Union[str, None] = 'e5f6a7b8c9d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index('idx_matches_status_dates', 'matches', ['status', 'scheduled_date', 'match_date'], unique=False)


def downgrade() -> None:
    op.drop_index('idx_matches_status_dates', table_name='matches')
