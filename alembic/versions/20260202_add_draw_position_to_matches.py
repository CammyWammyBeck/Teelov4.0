"""Add draw_position column to matches

Revision ID: a1b2c3d4e5f6
Revises: db3fab1b850a
Create Date: 2026-02-02

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# Revision identifiers, used by Alembic
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = 'db3fab1b850a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add draw_position column (nullable - only set for draw-sourced matches)
    op.add_column('matches', sa.Column('draw_position', sa.Integer(), nullable=True))

    # Composite index for efficient draw lookups:
    # "find the match at position X in round Y of tournament Z"
    op.create_index(
        'idx_matches_draw',
        'matches',
        ['tournament_edition_id', 'round', 'draw_position'],
    )


def downgrade() -> None:
    op.drop_index('idx_matches_draw', table_name='matches')
    op.drop_column('matches', 'draw_position')
