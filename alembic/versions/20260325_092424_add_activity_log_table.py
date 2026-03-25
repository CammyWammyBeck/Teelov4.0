"""add activity_log table

Revision ID: 3979362c70f9
Revises: fce2e68d323b
Create Date: 2026-03-25 09:24:24.607775+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# Revision identifiers, used by Alembic
revision: str = '3979362c70f9'
down_revision: Union[str, None] = 'fce2e68d323b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('activity_log',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('event_type', sa.String(length=50), nullable=False),
        sa.Column('count', sa.Integer(), nullable=False),
        sa.Column('message', sa.String(length=200), nullable=False),
        sa.Column('pipeline_run_id', sa.String(length=64), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['pipeline_run_id'], ['pipeline_runs.run_id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_activity_log_created_at', 'activity_log', ['created_at'], unique=False)
    op.create_index('idx_activity_log_type_created_at', 'activity_log', ['event_type', 'created_at'], unique=False)


def downgrade() -> None:
    op.drop_index('idx_activity_log_type_created_at', table_name='activity_log')
    op.drop_index('idx_activity_log_created_at', table_name='activity_log')
    op.drop_table('activity_log')
