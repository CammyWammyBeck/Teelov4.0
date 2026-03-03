"""Add surface ELO state and snapshot tables

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-03-02 12:00:00.000000+00:00
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# Revision identifiers, used by Alembic.
revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, None] = "c3d4e5f6a7b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "player_surface_elo_states",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("surface", sa.String(length=20), nullable=False),
        sa.Column("rating", sa.Numeric(precision=8, scale=2), nullable=False, server_default=sa.text("1500.00")),
        sa.Column("match_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_match_date", sa.Date(), nullable=True),
        sa.Column("last_temporal_order", sa.BigInteger(), nullable=True),
        sa.Column("career_peak", sa.Numeric(precision=8, scale=2), nullable=False, server_default=sa.text("1500.00")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["player_id"], ["players.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("player_id", "surface", name="uq_player_surface_elo_state"),
    )
    op.create_index(
        "ix_player_surface_elo_state_surface_rating",
        "player_surface_elo_states",
        ["surface", "rating"],
        unique=False,
    )
    op.create_index(
        "ix_player_surface_elo_states_last_temporal_order",
        "player_surface_elo_states",
        ["last_temporal_order"],
        unique=False,
    )

    op.create_table(
        "match_surface_elo_snapshots",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("match_id", sa.Integer(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("surface", sa.String(length=20), nullable=False),
        sa.Column("temporal_order", sa.BigInteger(), nullable=True),
        sa.Column("elo_pre", sa.Numeric(precision=8, scale=2), nullable=False),
        sa.Column("elo_post", sa.Numeric(precision=8, scale=2), nullable=False),
        sa.Column("elo_change", sa.Numeric(precision=8, scale=2), nullable=False),
        sa.Column("elo_params_version", sa.String(length=64), nullable=True),
        sa.Column("processed_at", sa.DateTime(), nullable=False, server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["match_id"], ["matches.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["player_id"], ["players.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("match_id", "player_id", "surface", name="uq_match_surface_elo_snapshot"),
    )
    op.create_index(
        "ix_match_surface_elo_player_surface",
        "match_surface_elo_snapshots",
        ["player_id", "surface"],
        unique=False,
    )
    op.create_index(
        "ix_match_surface_elo_match",
        "match_surface_elo_snapshots",
        ["match_id"],
        unique=False,
    )
    op.create_index(
        "ix_match_surface_elo_snapshots_temporal_order",
        "match_surface_elo_snapshots",
        ["temporal_order"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_match_surface_elo_snapshots_temporal_order", table_name="match_surface_elo_snapshots")
    op.drop_index("ix_match_surface_elo_match", table_name="match_surface_elo_snapshots")
    op.drop_index("ix_match_surface_elo_player_surface", table_name="match_surface_elo_snapshots")
    op.drop_table("match_surface_elo_snapshots")

    op.drop_index("ix_player_surface_elo_states_last_temporal_order", table_name="player_surface_elo_states")
    op.drop_index("ix_player_surface_elo_state_surface_rating", table_name="player_surface_elo_states")
    op.drop_table("player_surface_elo_states")
