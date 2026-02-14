"""Add pipeline run audit and checkpoint tables

Revision ID: 9b4c1f7d3e2a
Revises: 7f3c9c1d2a10
Create Date: 2026-02-14 10:30:00.000000+00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# Revision identifiers, used by Alembic.
revision: str = "9b4c1f7d3e2a"
down_revision: Union[str, None] = "7f3c9c1d2a10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "pipeline_checkpoints",
        sa.Column("key", sa.String(length=120), nullable=False),
        sa.Column("value_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("key"),
    )

    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("ended_at", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id"),
    )
    op.create_index("idx_pipeline_runs_started_at", "pipeline_runs", ["started_at"], unique=False)
    op.create_index(
        "idx_pipeline_runs_status_started_at",
        "pipeline_runs",
        ["status", "started_at"],
        unique=False,
    )

    op.create_table(
        "pipeline_stage_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("stage_name", sa.String(length=80), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("ended_at", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("metrics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["pipeline_runs.run_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_pipeline_stage_runs_run_stage",
        "pipeline_stage_runs",
        ["run_id", "stage_name"],
        unique=False,
    )
    op.create_index(
        "idx_pipeline_stage_runs_stage_started_at",
        "pipeline_stage_runs",
        ["stage_name", "started_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_pipeline_stage_runs_stage_started_at", table_name="pipeline_stage_runs")
    op.drop_index("idx_pipeline_stage_runs_run_stage", table_name="pipeline_stage_runs")
    op.drop_table("pipeline_stage_runs")

    op.drop_index("idx_pipeline_runs_status_started_at", table_name="pipeline_runs")
    op.drop_index("idx_pipeline_runs_started_at", table_name="pipeline_runs")
    op.drop_table("pipeline_runs")

    op.drop_table("pipeline_checkpoints")
