"""Add pg_trgm GIN indexes for ILIKE search queries

Revision ID: a7b8c9d0e1f2
Revises: f1a2b3c4d5e6
Create Date: 2026-03-10 13:00:00.000000+00:00
"""
from collections.abc import Sequence

from alembic import op


revision: str = "a7b8c9d0e1f2"
down_revision: str | None = "f1a2b3c4d5e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_players_canonical_name_trgm "
        "ON players USING GIN (lower(canonical_name) gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_tournaments_name_trgm "
        "ON tournaments USING GIN (lower(name) gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_tournaments_city_trgm "
        "ON tournaments USING GIN (lower(city) gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_tournaments_country_trgm "
        "ON tournaments USING GIN (lower(country) gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_player_aliases_alias_trgm "
        "ON player_aliases USING GIN (lower(alias) gin_trgm_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_player_aliases_alias_trgm")
    op.execute("DROP INDEX IF EXISTS idx_tournaments_country_trgm")
    op.execute("DROP INDEX IF EXISTS idx_tournaments_city_trgm")
    op.execute("DROP INDEX IF EXISTS idx_tournaments_name_trgm")
    op.execute("DROP INDEX IF EXISTS idx_players_canonical_name_trgm")
