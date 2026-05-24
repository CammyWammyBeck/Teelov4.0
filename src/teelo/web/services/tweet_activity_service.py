"""
Query service for social content / tweet activity.

All read queries for the admin tweet activity dashboard live here.
"""

from sqlalchemy import desc, or_
from sqlalchemy.orm import Session, joinedload

from teelo.db.models import SocialContentItem, SocialContentPost, SocialContentVersion


def list_content_items(
    db: Session,
    channel: str | None = None,
    content_type: str | None = None,
    status: str | None = None,
    query: str | None = None,
    limit: int = 50,
    offset: int = 0,
):
    """List social content items with optional filters."""
    q = db.query(SocialContentItem).options(
        joinedload(SocialContentItem.versions),
        joinedload(SocialContentItem.posts),
    )
    q = _apply_content_filters(
        q,
        channel=channel,
        content_type=content_type,
        status=status,
        query=query,
    )
    return (
        q.order_by(desc(SocialContentItem.updated_at))
        .offset(offset)
        .limit(limit)
        .all()
    )


def get_content_item_by_key(db: Session, content_key: str) -> SocialContentItem | None:
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


def display_content_version(item: SocialContentItem) -> SocialContentVersion | None:
    """Pick the version text that should be shown as the primary content."""
    non_empty_versions = [version for version in item.versions if version.content_text]
    if not non_empty_versions:
        return None

    preferred_by_status = {
        "posted": ("posted", "approved", "queued", "submitted"),
        "queued": ("queued", "approved", "submitted"),
        "approved": ("approved", "queued", "submitted"),
        "killed": ("killed", "approved", "queued", "submitted"),
        "blocked": ("blocked", "approved", "queued", "submitted"),
        "failed": ("failed", "approved", "queued", "submitted"),
        "failed_review": ("failed_review", "approved", "queued", "submitted"),
    }
    for preferred_key in preferred_by_status.get(item.status, ()):
        for version in reversed(non_empty_versions):
            if version.version_key == preferred_key:
                return version

    for version in non_empty_versions:
        if version.version_key == item.current_version_key:
            return version

    return non_empty_versions[-1]


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
    channel: str | None = None,
    content_type: str | None = None,
    status: str | None = None,
    query: str | None = None,
) -> int:
    """Total count for pagination."""
    q = db.query(SocialContentItem)
    q = _apply_content_filters(
        q,
        channel=channel,
        content_type=content_type,
        status=status,
        query=query,
    )
    if query:
        return q.distinct().count()
    return q.count()


def _apply_content_filters(q, channel=None, content_type=None, status=None, query=None):
    if channel:
        q = q.filter(SocialContentItem.channel == channel)
    if content_type:
        q = q.filter(SocialContentItem.content_type == content_type)
    if status:
        q = q.filter(SocialContentItem.status == status)
    if query:
        needle = f"%{query.strip()}%"
        q = q.outerjoin(SocialContentPost).filter(
            or_(
                SocialContentItem.content_key.ilike(needle),
                SocialContentItem.summary.ilike(needle),
                SocialContentItem.posted_tweet_id.ilike(needle),
                SocialContentPost.posted_tweet_id.ilike(needle),
            )
        )
    return q
