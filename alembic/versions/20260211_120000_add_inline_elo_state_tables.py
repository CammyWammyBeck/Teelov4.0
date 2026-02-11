"""Add inline ELO state tables and match ELO snapshot columns

Revision ID: c2b74f9a8f01
Revises: a7f22d8e1b5c
Create Date: 2026-02-11 12:00:00.000000+00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# Revision identifiers, used by Alembic.
revision: str = "c2b74f9a8f01"
down_revision: Union[str, None] = "a7f22d8e1b5c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("matches", sa.Column("elo_pre_player_a", sa.Numeric(precision=8, scale=2), nullable=True))
    op.add_column("matches", sa.Column("elo_pre_player_b", sa.Numeric(precision=8, scale=2), nullable=True))
    op.add_column("matches", sa.Column("elo_post_player_a", sa.Numeric(precision=8, scale=2), nullable=True))
    op.add_column("matches", sa.Column("elo_post_player_b", sa.Numeric(precision=8, scale=2), nullable=True))
    op.add_column("matches", sa.Column("elo_params_version", sa.String(length=64), nullable=True))
    op.add_column("matches", sa.Column("elo_processed_at", sa.DateTime(), nullable=True))
    op.add_column(
        "matches",
        sa.Column("elo_needs_recompute", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )

    op.create_table(
        "player_elo_states",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("rating", sa.Numeric(precision=8, scale=2), nullable=False),
        sa.Column("match_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_match_date", sa.Date(), nullable=True),
        sa.Column("last_temporal_order", sa.BigInteger(), nullable=True),
        sa.Column("career_peak", sa.Numeric(precision=8, scale=2), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["player_id"], ["players.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("player_id"),
    )
    op.create_index("ix_player_elo_states_last_temporal_order", "player_elo_states", ["last_temporal_order"])

    op.create_table(
        "elo_parameter_sets",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("params", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source", sa.String(length=30), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index("idx_elo_parameter_sets_active", "elo_parameter_sets", ["is_active"])

    op.execute(
        """
        DELETE FROM elo_ratings a
        USING elo_ratings b
        WHERE a.id > b.id
          AND a.player_id = b.player_id
          AND a.match_id = b.match_id
          AND COALESCE(a.surface, '') = COALESCE(b.surface, '')
        """
    )
    op.create_unique_constraint(
        "uq_elo_ratings_player_match_surface",
        "elo_ratings",
        ["player_id", "match_id", "surface"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_elo_ratings_player_match_surface", "elo_ratings", type_="unique")

    op.drop_index("idx_elo_parameter_sets_active", table_name="elo_parameter_sets")
    op.drop_table("elo_parameter_sets")

    op.drop_index("ix_player_elo_states_last_temporal_order", table_name="player_elo_states")
    op.drop_table("player_elo_states")

    op.drop_column("matches", "elo_needs_recompute")
    op.drop_column("matches", "elo_processed_at")
    op.drop_column("matches", "elo_params_version")
    op.drop_column("matches", "elo_post_player_b")
    op.drop_column("matches", "elo_post_player_a")
    op.drop_column("matches", "elo_pre_player_b")
    op.drop_column("matches", "elo_pre_player_a")
