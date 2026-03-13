"""add home page indexes

Revision ID: a3f8c1d9e2b7
Revises: b2c3d4e5f6a7
Create Date: 2026-03-13 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# Revision identifiers, used by Alembic
revision: str = "a3f8c1d9e2b7"
down_revision: Union[str, None] = "b2c3d4e5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add indexes to support home page queries."""
    op.create_index("ix_player_elo_state_rating", "player_elo_states", ["rating"])
    op.create_index("ix_tournament_edition_start_date", "tournament_editions", ["start_date"])
    op.create_index("ix_tournament_edition_end_date", "tournament_editions", ["end_date"])


def downgrade() -> None:
    """Remove home page indexes."""
    op.drop_index("ix_tournament_edition_end_date", table_name="tournament_editions")
    op.drop_index("ix_tournament_edition_start_date", table_name="tournament_editions")
    op.drop_index("ix_player_elo_state_rating", table_name="player_elo_states")
