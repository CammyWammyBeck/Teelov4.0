"""add tournament forecast tables

Revision ID: 2b8c0c3d2c1a
Revises: 3979362c70f9
Create Date: 2026-04-03 06:00:00.000000+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# Revision identifiers, used by Alembic
revision: str = "2b8c0c3d2c1a"
down_revision: Union[str, None] = "3979362c70f9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tournament_forecast_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tournament_edition_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="building"),
        sa.Column("build_reason", sa.String(length=30), nullable=False, server_default="initial"),
        sa.Column("structure_signature", sa.String(length=64), nullable=False),
        sa.Column("state_signature", sa.String(length=64), nullable=False),
        sa.Column("feature_set_name", sa.String(length=100), nullable=False),
        sa.Column("model_version", sa.String(length=80), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["tournament_edition_id"], ["tournament_editions.id"]),
    )
    op.create_index(
        "idx_forecast_run_active_edition",
        "tournament_forecast_runs",
        ["tournament_edition_id", "is_active", "status"],
        unique=False,
    )

    op.create_table(
        "tournament_forecast_nodes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("forecast_run_id", sa.Integer(), nullable=False),
        sa.Column("round", sa.String(length=20), nullable=False),
        sa.Column("draw_position", sa.Integer(), nullable=False),
        sa.Column("player_a_id", sa.Integer(), nullable=False),
        sa.Column("player_b_id", sa.Integer(), nullable=False),
        sa.Column("left_parent_node_id", sa.Integer(), nullable=True),
        sa.Column("right_parent_node_id", sa.Integer(), nullable=True),
        sa.Column("source_match_id", sa.Integer(), nullable=True),
        sa.Column("node_type", sa.String(length=20), nullable=False, server_default="scenario"),
        sa.Column("generation_depth", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("feature_set_name", sa.String(length=100), nullable=False),
        sa.Column("prediction_model_version", sa.String(length=80), nullable=True),
        sa.Column("player_a_state_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("player_b_state_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("features_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("prediction_a", sa.Numeric(7, 6), nullable=True),
        sa.Column("predicted_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["forecast_run_id"], ["tournament_forecast_runs.id"]),
        sa.ForeignKeyConstraint(["player_a_id"], ["players.id"]),
        sa.ForeignKeyConstraint(["player_b_id"], ["players.id"]),
        sa.ForeignKeyConstraint(["left_parent_node_id"], ["tournament_forecast_nodes.id"]),
        sa.ForeignKeyConstraint(["right_parent_node_id"], ["tournament_forecast_nodes.id"]),
        sa.ForeignKeyConstraint(["source_match_id"], ["matches.id"]),
        sa.UniqueConstraint(
            "forecast_run_id",
            "round",
            "draw_position",
            "player_a_id",
            "player_b_id",
            "left_parent_node_id",
            "right_parent_node_id",
            name="uq_forecast_node_identity",
        ),
    )
    op.create_index(
        "idx_forecast_nodes_slot",
        "tournament_forecast_nodes",
        ["forecast_run_id", "round", "draw_position"],
        unique=False,
    )
    op.create_index(
        "ix_tournament_forecast_nodes_forecast_run_id",
        "tournament_forecast_nodes",
        ["forecast_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_tournament_forecast_nodes_round",
        "tournament_forecast_nodes",
        ["round"],
        unique=False,
    )
    op.create_index(
        "ix_tournament_forecast_nodes_draw_position",
        "tournament_forecast_nodes",
        ["draw_position"],
        unique=False,
    )
    op.create_index(
        "ix_tournament_forecast_nodes_source_match_id",
        "tournament_forecast_nodes",
        ["source_match_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_tournament_forecast_nodes_source_match_id", table_name="tournament_forecast_nodes")
    op.drop_index("ix_tournament_forecast_nodes_draw_position", table_name="tournament_forecast_nodes")
    op.drop_index("ix_tournament_forecast_nodes_round", table_name="tournament_forecast_nodes")
    op.drop_index("ix_tournament_forecast_nodes_forecast_run_id", table_name="tournament_forecast_nodes")
    op.drop_index("idx_forecast_nodes_slot", table_name="tournament_forecast_nodes")
    op.drop_table("tournament_forecast_nodes")

    op.drop_index("idx_forecast_run_active_edition", table_name="tournament_forecast_runs")
    op.drop_table("tournament_forecast_runs")
