"""
Query service for social content / tweet activity.

All read queries for the admin tweet activity dashboard live here.
"""
from datetime import datetime
from typing import Optional

from sqlalchemy import desc
from sqlalchemy.orm import Session, joinedload

from teelo.db.models import SocialContentItem, SocialContentVersion, SocialContentPost


def list_content_items(
    db: Session,
    channel: Optional[str] = None,
    content_type: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    """List social content items with optional filters."""
    q = db.query(SocialContentItem).options(
        joinedload(SocialContentItem.versions),
        joinedload(SocialContentItem.posts),
    )
    if channel:
        q = q.filter(SocialContentItem.channel == channel)
    if content_type:
        q = q.filter(SocialContentItem.content_type == content_type)
    if status:
        q = q.filter(SocialContentItem.status == status)
    return (
        q.order_by(desc(SocialContentItem.updated_at))
        .offset(offset)
        .limit(limit)
        .all()
    )


def get_content_item_by_key(db: Session, content_key: str) -> Optional[SocialContentItem]:
    """Fetch a single content item by its stable key (e.g. D-190)."""
    return (
        db.query(SocialContentItem)
        .options(
            joinedload(SocialContentItem.versions),
            joinedload(SocialContentItem.posts),
        )
        .filter(SocialContentItem.content_key == content_key)
        .first()
    )


def get_content_item_versions(db: Session, content_item_id: int):
    """Return all versions for a content item, oldest first."""
    return (
        db.query(SocialContentVersion)
        .filter(SocialContentVersion.content_item_id == content_item_id)
        .order_by(SocialContentVersion.created_at)
        .all()
    )


def get_content_item_posts(db: Session, content_item_id: int):
    """Return all post attempts for a content item."""
    return (
        db.query(SocialContentPost)
        .filter(SocialContentPost.content_item_id == content_item_id)
        .order_by(SocialContentPost.posted_at)
        .all()
    )


def content_item_count(
    db: Session,
    channel: Optional[str] = None,
    content_type: Optional[str] = None,
    status: Optional[str] = None,
) -> int:
    """Total count for pagination."""
    q = db.query(SocialContentItem)
    if channel:
        q = q.filter(SocialContentItem.channel == channel)
    if content_type:
        q = q.filter(SocialContentItem.content_type == content_type)
    if status:
        q = q.filter(SocialContentItem.status == status)
    return q.count()