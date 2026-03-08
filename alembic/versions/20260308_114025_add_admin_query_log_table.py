"""add admin_query_log table

Revision ID: db077cb9dba9
Revises: e19fa77e65ee
Create Date: 2026-03-08 11:40:25.647112+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# Revision identifiers, used by Alembic
revision: str = 'db077cb9dba9'
down_revision: Union[str, None] = 'e19fa77e65ee'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('admin_query_log',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('admin_user_id', sa.Integer(), nullable=False),
    sa.Column('query_text', sa.Text(), nullable=False),
    sa.Column('query_type', sa.String(length=20), nullable=False),
    sa.Column('affected_rows', sa.Integer(), nullable=True),
    sa.Column('success', sa.Boolean(), nullable=False),
    sa.Column('error_message', sa.Text(), nullable=True),
    sa.Column('executed_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['admin_user_id'], ['admin_users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_admin_query_log_executed', 'admin_query_log', ['executed_at'], unique=False)
    op.create_index('idx_admin_query_log_user', 'admin_query_log', ['admin_user_id'], unique=False)


def downgrade() -> None:
    op.drop_index('idx_admin_query_log_user', table_name='admin_query_log')
    op.drop_index('idx_admin_query_log_executed', table_name='admin_query_log')
    op.drop_table('admin_query_log')
