"""Add gender column to players and backfill from tournament data

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-03-05 12:00:00.000000+00:00
"""

from typing import Sequence, Union
import sqlalchemy as sa
from alembic import op


revision: str = "e5f6a7b8c9d0"
down_revision: Union[str, None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("players", sa.Column("gender", sa.String(length=10), nullable=True))

    # Backfill using DISTINCT ON — picks one gendered tournament per player,
    # much faster than a full aggregation across all 612k matches.
    op.execute("""
        UPDATE players p
        SET gender = t.gender
        FROM (
            SELECT DISTINCT ON (player_id) player_id, t.gender
            FROM (
                SELECT player_a_id AS player_id, tournament_edition_id FROM matches
                UNION ALL
                SELECT player_b_id AS player_id, tournament_edition_id FROM matches
            ) m
            JOIN tournament_editions te ON te.id = m.tournament_edition_id
            JOIN tournaments t ON t.id = te.tournament_id
            WHERE t.gender IN ('men', 'women')
            ORDER BY player_id, t.gender
        ) t
        WHERE p.id = t.player_id
    """)


def downgrade() -> None:
    op.drop_column("players", "gender")
