"""Idempotent writer helpers for social content activity rows."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from teelo.db.models import SocialContentItem, SocialContentPost, SocialContentVersion


def upsert_item(
    db: Session,
    content_key: str,
    *,
    content_type: str = "broadcast",
    status: str = "draft",
    channel: str = "x_twitter",
    summary: str | None = None,
    draft_file_path: str | None = None,
    reply_to_tweet_id: str | None = None,
    reply_to_handle: str | None = None,
    post_at: datetime | None = None,
    posted_at: datetime | None = None,
    posted_tweet_id: str | None = None,
    current_version_key: str | None = None,
) -> SocialContentItem:
    """Create or update a social content item by stable workflow key."""
    item = (
        db.query(SocialContentItem)
        .filter(SocialContentItem.content_key == content_key)
        .one_or_none()
    )
    if item is None:
        item = SocialContentItem(
            content_key=content_key,
            content_type=content_type,
            status=status,
            channel=channel,
            current_version_key=current_version_key or "v1",
        )
        db.add(item)
        db.flush()

    item.content_type = content_type
    item.status = status
    item.channel = channel
    if summary is not None:
        item.summary = summary[:255]
    if draft_file_path is not None:
        item.draft_file_path = draft_file_path[:500]
    if reply_to_tweet_id is not None:
        item.reply_to_tweet_id = reply_to_tweet_id
    if reply_to_handle is not None:
        item.reply_to_handle = reply_to_handle
    if post_at is not None:
        item.post_at = post_at
    if posted_at is not None:
        item.posted_at = posted_at
    if posted_tweet_id is not None:
        item.posted_tweet_id = posted_tweet_id
    if current_version_key is not None:
        item.current_version_key = current_version_key
    return item


def record_version(
    db: Session,
    content_key: str,
    version_key: str,
    *,
    event: str,
    content_text: str | None = None,
    note: str | None = None,
    review_result: str | None = None,
    char_count: int | None = None,
    created_at: datetime | None = None,
    set_current: bool = False,
    item_defaults: dict[str, Any] | None = None,
) -> SocialContentVersion:
    """Create or update a version snapshot for a content item."""
    if item_defaults is None:
        item = (
            db.query(SocialContentItem)
            .filter(SocialContentItem.content_key == content_key)
            .one_or_none()
        )
        if item is None:
            item = upsert_item(db, content_key)
    else:
        item = upsert_item(db, content_key, **item_defaults)
    version_key = version_key[:30]
    version = (
        db.query(SocialContentVersion)
        .filter(
            SocialContentVersion.content_item_id == item.id,
            SocialContentVersion.version_key == version_key,
        )
        .one_or_none()
    )
    if version is None:
        version = SocialContentVersion(
            content_item_id=item.id,
            version_key=version_key,
            event=event,
            created_at=created_at or datetime.utcnow(),
        )
        db.add(version)

    version.event = event
    version.content_text = content_text
    version.note = note[:500] if note else None
    version.review_result = review_result
    version.char_count = char_count if char_count is not None else _char_count(content_text)
    if created_at is not None:
        version.created_at = created_at
    if set_current:
        item.current_version_key = version_key
    return version


def record_queue_state(
    db: Session,
    content_key: str,
    *,
    post_at: datetime | None,
    tweets: list[dict[str, Any]] | None = None,
    queued_by: str | None = None,
    item_defaults: dict[str, Any] | None = None,
) -> SocialContentItem:
    """Mark an item queued and store the queued tweet text snapshot."""
    defaults = {"status": "queued", **(item_defaults or {})}
    item = upsert_item(db, content_key, post_at=post_at, **defaults)
    text = _tweets_to_text(tweets or [])
    record_version(
        db,
        content_key,
        "queued",
        event="queued",
        content_text=text,
        note=f"Queued by {queued_by}" if queued_by else None,
        set_current=True,
        item_defaults=defaults,
    )
    return item


def record_posted(
    db: Session,
    content_key: str,
    *,
    external_post_ids: list[str],
    posted_at: datetime,
    tweets: list[dict[str, Any]] | None = None,
    item_defaults: dict[str, Any] | None = None,
) -> SocialContentItem:
    """Mark an item posted and upsert successful external post IDs."""
    first_post_id = external_post_ids[0] if external_post_ids else None
    defaults = {"status": "posted", **(item_defaults or {})}
    item = upsert_item(
        db,
        content_key,
        posted_at=posted_at,
        posted_tweet_id=first_post_id,
        **defaults,
    )
    for external_post_id in external_post_ids:
        post = (
            db.query(SocialContentPost)
            .filter(
                SocialContentPost.content_item_id == item.id,
                SocialContentPost.posted_tweet_id == external_post_id,
            )
            .one_or_none()
        )
        if post is None:
            post = SocialContentPost(
                content_item_id=item.id,
                posted_tweet_id=external_post_id,
                status="success",
            )
            db.add(post)
        post.posted_at = posted_at
        post.status = "success"
        post.error_message = None

    record_version(
        db,
        content_key,
        "posted",
        event="posted",
        content_text=_tweets_to_text(tweets or []),
        note=", ".join(external_post_ids),
        set_current=True,
        item_defaults=defaults,
    )
    return item


def record_blocked_or_failed(
    db: Session,
    content_key: str,
    *,
    status: str,
    reason: str,
    item_defaults: dict[str, Any] | None = None,
) -> SocialContentItem:
    """Mark an item blocked or failed and preserve the reason as a version event."""
    if status not in {"blocked", "failed", "failed_review"}:
        raise ValueError("status must be blocked, failed, or failed_review")
    defaults = {"status": status, **(item_defaults or {})}
    item = upsert_item(db, content_key, **defaults)
    record_version(
        db,
        content_key,
        status,
        event=status,
        note=reason,
        set_current=True,
        item_defaults=defaults,
    )
    return item


def record_killed(
    db: Session,
    content_key: str,
    *,
    reason: str | None = None,
    item_defaults: dict[str, Any] | None = None,
) -> SocialContentItem:
    """Mark an item killed and preserve the reason as a version event."""
    defaults = {"status": "killed", **(item_defaults or {})}
    item = upsert_item(db, content_key, **defaults)
    record_version(
        db,
        content_key,
        "killed",
        event="killed",
        note=reason,
        set_current=True,
        item_defaults=defaults,
    )
    return item


def _tweets_to_text(tweets: list[dict[str, Any]]) -> str:
    return "\n\n".join(str(tweet.get("text", "")).strip() for tweet in tweets).strip()


def _char_count(content_text: str | None) -> int | None:
    return len(content_text) if content_text else None
