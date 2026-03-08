"""add trigram indexes for search

Revision ID: e19fa77e65ee
Revises: b916a7c0890a
Create Date: 2026-03-08 05:21:18.299430+00:00

"""
from typing import Sequence, Union

from alembic import op


# Revision identifiers, used by Alembic
revision: str = 'e19fa77e65ee'
down_revision: Union[str, None] = 'b916a7c0890a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Upgrade database schema.

    This function is called when running 'alembic upgrade'.
    """
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute("CREATE INDEX IF NOT EXISTS idx_players_name_trgm ON players USING gin (canonical_name gin_trgm_ops)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_tournaments_name_trgm ON tournaments USING gin (name gin_trgm_ops)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_tournaments_city_trgm ON tournaments USING gin (city gin_trgm_ops) WHERE city IS NOT NULL")


def downgrade() -> None:
    """
    Downgrade database schema.

    This function is called when running 'alembic downgrade'.
    Note: Some migrations may not be fully reversible.
    """
    op.execute("DROP INDEX IF EXISTS idx_tournaments_city_trgm")
    op.execute("DROP INDEX IF EXISTS idx_tournaments_name_trgm")
    op.execute("DROP INDEX IF EXISTS idx_players_name_trgm")
