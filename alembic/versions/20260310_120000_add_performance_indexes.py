"""Add performance indexes for API queries

Revision ID: f1a2b3c4d5e6
Revises: 396f7d39f9f6
Create Date: 2026-03-10 12:00:00.000000+00:00

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# Revision identifiers, used by Alembic
revision: str = "f1a2b3c4d5e6"
down_revision: Union[str, None] = "396f7d39f9f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # tournaments.tour — core filter dimension, used in almost every listing query
    op.create_index("idx_tournaments_tour", "tournaments", ["tour"])

    # tournaments.tour + surface — composite for the common (tour, surface) filter pair
    op.create_index("idx_tournaments_tour_surface", "tournaments", ["tour", "surface"])

    # tournament_editions.start_date — date-window queries (e.g. "editions this season")
    op.create_index("idx_tournament_editions_start_date", "tournament_editions", ["start_date"])

    # tournament_editions(tournament_id, start_date DESC) — latest-edition lookups per tournament
    op.create_index(
        "idx_tournament_editions_tournament_start",
        "tournament_editions",
        ["tournament_id", sa.text("start_date DESC")],
    )

    # matches composite for tournament detail pages:
    # "all matches for edition X, ordered by status then date"
    op.create_index(
        "idx_matches_edition_status_date",
        "matches",
        ["tournament_edition_id", "status", sa.text("match_date DESC")],
    )


def downgrade() -> None:
    op.drop_index("idx_matches_edition_status_date", table_name="matches")
    op.drop_index("idx_tournament_editions_tournament_start", table_name="tournament_editions")
    op.drop_index("idx_tournament_editions_start_date", table_name="tournament_editions")
    op.drop_index("idx_tournaments_tour_surface", table_name="tournaments")
    op.drop_index("idx_tournaments_tour", table_name="tournaments")
