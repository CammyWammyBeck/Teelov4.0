"""add tournament timezone column

Revision ID: fce2e68d323b
Revises: a3f8c1d9e2b7
Create Date: 2026-03-19 09:15:16.367700+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# Revision identifiers, used by Alembic
revision: str = 'fce2e68d323b'
down_revision: Union[str, None] = 'a3f8c1d9e2b7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('tournaments', sa.Column('timezone', sa.String(length=50), nullable=True))


def downgrade() -> None:
    op.drop_column('tournaments', 'timezone')
