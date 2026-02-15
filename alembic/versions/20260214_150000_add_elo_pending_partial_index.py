"""Add partial index for ELO pending match query

Adds idx_matches_elo_pending, a partial index on (temporal_order, id)
covering only terminal matches that still need ELO processing. This
avoids a full table scan on the matches table during incremental ELO
updates, which is critical in steady state where only a few matches
match the filter.

Revision ID: b8c9d0e1f2a3
Revises: 9b4c1f7d3e2a
Create Date: 2026-02-14 15:00:00.000000+00:00
"""

from typing import Sequence, Union

from alembic import op


# Revision identifiers, used by Alembic.
revision: str = "b8c9d0e1f2a3"
down_revision: Union[str, None] = "9b4c1f7d3e2a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE INDEX idx_matches_elo_pending
        ON matches (temporal_order, id)
        WHERE status IN ('completed', 'retired', 'walkover', 'default')
          AND winner_id IS NOT NULL
          AND temporal_order IS NOT NULL
          AND (
              elo_post_player_a IS NULL
              OR elo_post_player_b IS NULL
              OR elo_needs_recompute = true
          )
    """)


def downgrade() -> None:
    op.drop_index("idx_matches_elo_pending", table_name="matches")
