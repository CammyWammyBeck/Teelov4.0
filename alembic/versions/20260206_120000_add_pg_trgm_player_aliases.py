"""Enable pg_trgm and add trigram index for player aliases

Revision ID: 6d2f8c0b7f1a
Revises: 319bd5f03dbf
Create Date: 2026-02-06 12:00:00.000000+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# Revision identifiers, used by Alembic
revision: str = '6d2f8c0b7f1a'
down_revision: Union[str, None] = '319bd5f03dbf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Upgrade database schema.

    This function is called when running 'alembic upgrade'.
    """
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.create_index(
        "idx_player_aliases_alias_trgm",
        "player_aliases",
        ["alias"],
        postgresql_using="gin",
        postgresql_ops={"alias": "gin_trgm_ops"},
    )


def downgrade() -> None:
    """
    Downgrade database schema.

    This function is called when running 'alembic downgrade'.
    Note: Some migrations may not be fully reversible.
    """
    op.drop_index("idx_player_aliases_alias_trgm", table_name="player_aliases")
    op.execute("DROP EXTENSION IF EXISTS pg_trgm")
