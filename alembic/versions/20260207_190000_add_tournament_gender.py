"""Add tournament gender and include it in tournament uniqueness

Revision ID: a7f22d8e1b5c
Revises: 6d2f8c0b7f1a
Create Date: 2026-02-07 19:00:00.000000+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# Revision identifiers, used by Alembic
revision: str = 'a7f22d8e1b5c'
down_revision: Union[str, None] = '6d2f8c0b7f1a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade database schema."""
    op.add_column('tournaments', sa.Column('gender', sa.String(length=10), nullable=True))

    # Backfill obvious cases from tour value.
    op.execute("UPDATE tournaments SET gender = 'men' WHERE gender IS NULL AND tour IN ('ATP', 'Challenger', 'CHALLENGER')")
    op.execute("UPDATE tournaments SET gender = 'women' WHERE gender IS NULL AND tour IN ('WTA', 'WTA 125', 'WTA_125')")

    # ITF codes usually include m-itf-* or w-itf-*.
    op.execute("UPDATE tournaments SET gender = 'men' WHERE gender IS NULL AND tour = 'ITF' AND lower(tournament_code) LIKE 'm-itf-%'")
    op.execute("UPDATE tournaments SET gender = 'women' WHERE gender IS NULL AND tour = 'ITF' AND lower(tournament_code) LIKE 'w-itf-%'")

    # Fallback heuristics for ITF naming conventions such as M15/W25.
    op.execute("UPDATE tournaments SET gender = 'men' WHERE gender IS NULL AND tour = 'ITF' AND name ~* '^\\s*m\\d{2,3}'")
    op.execute("UPDATE tournaments SET gender = 'women' WHERE gender IS NULL AND tour = 'ITF' AND name ~* '^\\s*w\\d{2,3}'")

    op.drop_constraint('uq_tournament_code_tour', 'tournaments', type_='unique')
    op.create_unique_constraint(
        'uq_tournament_code_tour_gender',
        'tournaments',
        ['tournament_code', 'tour', 'gender'],
    )


def downgrade() -> None:
    """Downgrade database schema."""
    op.drop_constraint('uq_tournament_code_tour_gender', 'tournaments', type_='unique')
    op.create_unique_constraint(
        'uq_tournament_code_tour',
        'tournaments',
        ['tournament_code', 'tour'],
    )
    op.drop_column('tournaments', 'gender')
