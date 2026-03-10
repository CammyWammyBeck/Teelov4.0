"""Add composite index on (status, match_date DESC) for default match listing

Revision ID: b2c3d4e5f6a7
Revises: a7b8c9d0e1f2
Create Date: 2026-03-10 14:00:00.000000+00:00
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "b2c3d4e5f6a7"
down_revision: str | None = "a7b8c9d0e1f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # The most common query pattern: filter by status IN (...) + ORDER BY match_date DESC.
    # The existing idx_matches_status_dates uses (status, scheduled_date, match_date)
    # which doesn't cover match_date-first ordering efficiently.
    op.create_index(
        "idx_matches_status_match_date",
        "matches",
        ["status", sa.text("match_date DESC NULLS LAST")],
    )


def downgrade() -> None:
    op.drop_index("idx_matches_status_match_date", table_name="matches")
