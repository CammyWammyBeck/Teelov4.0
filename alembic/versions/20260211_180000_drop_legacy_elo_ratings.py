"""Drop legacy elo_ratings table

Revision ID: 7f3c9c1d2a10
Revises: c2b74f9a8f01
Create Date: 2026-02-11 18:00:00.000000+00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# Revision identifiers, used by Alembic.
revision: str = "7f3c9c1d2a10"
down_revision: Union[str, None] = "c2b74f9a8f01"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("uq_elo_ratings_player_match_surface", "elo_ratings", type_="unique")
    op.drop_index("idx_elo_ratings_player_date", table_name="elo_ratings")
    op.drop_index("idx_elo_ratings_match", table_name="elo_ratings")
    op.drop_table("elo_ratings")


def downgrade() -> None:
    op.create_table(
        "elo_ratings",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("match_id", sa.Integer(), nullable=False),
        sa.Column("rating_before", sa.Numeric(precision=8, scale=2), nullable=False),
        sa.Column("rating_after", sa.Numeric(precision=8, scale=2), nullable=False),
        sa.Column("surface", sa.String(length=20), nullable=True),
        sa.Column("is_career_peak", sa.Boolean(), nullable=False),
        sa.Column("rating_date", sa.Date(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["match_id"], ["matches.id"]),
        sa.ForeignKeyConstraint(["player_id"], ["players.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("player_id", "match_id", "surface", name="uq_elo_ratings_player_match_surface"),
    )
    op.create_index("idx_elo_ratings_match", "elo_ratings", ["match_id"], unique=False)
    op.create_index("idx_elo_ratings_player_date", "elo_ratings", ["player_id", "rating_date"], unique=False)
