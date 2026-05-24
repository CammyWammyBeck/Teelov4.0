"""add social_content_items, social_content_versions, social_content_posts tables

Revision ID: 7f3a2c1b9d55
Revises: 9a4c7b3e1d22
Create Date: 2026-05-24 15:36:00.000000+00:00

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# Revision identifiers, used by Alembic
revision: str = "7f3a2c1b9d55"
down_revision: str | Sequence[str] | None = "9a4c7b3e1d22"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "social_content_items",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("content_key", sa.String(length=20), nullable=False),
        sa.Column("content_type", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("channel", sa.String(length=20), nullable=False),
        sa.Column("current_version_key", sa.String(length=30), nullable=False),
        sa.Column("reply_to_tweet_id", sa.String(length=64), nullable=True),
        sa.Column("reply_to_handle", sa.String(length=255), nullable=True),
        sa.Column("post_at", sa.DateTime(), nullable=True),
        sa.Column("posted_at", sa.DateTime(), nullable=True),
        sa.Column("posted_tweet_id", sa.String(length=64), nullable=True),
        sa.Column("draft_file_path", sa.String(length=500), nullable=True),
        sa.Column("summary", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("content_key"),
    )
    op.create_index(
        "idx_social_content_status_post_at",
        "social_content_items",
        ["status", "post_at"],
    )
    op.create_index(
        "idx_social_content_channel_status",
        "social_content_items",
        ["channel", "status"],
    )
    op.create_index(
        "idx_social_content_key_type",
        "social_content_items",
        ["content_key", "content_type"],
    )

    op.create_table(
        "social_content_versions",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("content_item_id", sa.Integer(), nullable=False),
        sa.Column("version_key", sa.String(length=30), nullable=False),
        sa.Column("content_text", sa.Text(), nullable=True),
        sa.Column("event", sa.String(length=50), nullable=False),
        sa.Column("note", sa.String(length=500), nullable=True),
        sa.Column("review_result", sa.String(length=20), nullable=True),
        sa.Column("char_count", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["content_item_id"],
            ["social_content_items.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("content_item_id", "version_key"),
    )
    op.create_index(
        "idx_social_version_item_event",
        "social_content_versions",
        ["content_item_id", "event"],
    )

    op.create_table(
        "social_content_posts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("content_item_id", sa.Integer(), nullable=False),
        sa.Column("posted_tweet_id", sa.String(length=64), nullable=True),
        sa.Column("posted_at", sa.DateTime(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["content_item_id"],
            ["social_content_items.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_social_post_item_status",
        "social_content_posts",
        ["content_item_id", "status"],
    )


def downgrade() -> None:
    op.drop_index("idx_social_post_item_status", table_name="social_content_posts")
    op.drop_table("social_content_posts")
    op.drop_index("idx_social_version_item_event", table_name="social_content_versions")
    op.drop_table("social_content_versions")
    op.drop_index("idx_social_content_key_type", table_name="social_content_items")
    op.drop_index("idx_social_content_channel_status", table_name="social_content_items")
    op.drop_index("idx_social_content_status_post_at", table_name="social_content_items")
    op.drop_table("social_content_items")
