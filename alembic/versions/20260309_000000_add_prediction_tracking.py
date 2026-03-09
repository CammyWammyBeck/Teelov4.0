"""add prediction tracking

Revision ID: 396f7d39f9f6
Revises: db077cb9dba9
Create Date: 2026-03-09 00:00:00.000000+00:00

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# Revision identifiers, used by Alembic
revision: str = '396f7d39f9f6'
down_revision: Union[str, None] = 'db077cb9dba9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add prediction_source column to matches table
    op.add_column('matches', sa.Column('prediction_source', sa.String(20), nullable=True))

    # Create model_evaluation_snapshots table
    op.create_table(
        'model_evaluation_snapshots',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('model_version', sa.String(50), nullable=False),
        sa.Column('snapshot_date', sa.Date(), nullable=False),
        sa.Column('source_filter', sa.String(20), nullable=False),
        sa.Column('metrics', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('computed_at', sa.DateTime(), nullable=True),
        sa.UniqueConstraint('model_version', 'snapshot_date', 'source_filter', name='uq_eval_snapshot'),
    )
    op.create_index('idx_eval_snapshot_date', 'model_evaluation_snapshots', ['snapshot_date'])


def downgrade() -> None:
    op.drop_index('idx_eval_snapshot_date', table_name='model_evaluation_snapshots')
    op.drop_table('model_evaluation_snapshots')
    op.drop_column('matches', 'prediction_source')
