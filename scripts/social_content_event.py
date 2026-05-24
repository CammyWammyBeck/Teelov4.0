#!/usr/bin/env python3
"""Apply one social content activity event to the configured Teelo database.

Input is a JSON object on stdin:

    {"event": "queued", "entry": {...}}
    {"event": "posted", "entry": {...}}
    {"event": "blocked", "entry": {...}, "reason": "..."}
    {"event": "failed", "entry": {...}, "reason": "..."}
    {"event": "killed", "draft_id": "D-123", "row": {...}, "reason": "..."}
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Any

from teelo.db.session import get_session
from teelo.services.social_content_writer import (
    record_blocked_or_failed,
    record_killed,
    record_posted,
    record_queue_state,
)


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw or raw == "—":
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M",):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def item_defaults_from_entry(entry: dict[str, Any], draft_id: str | None = None) -> dict[str, Any]:
    content_key = draft_id or str(entry.get("draft_id") or "")
    return {
        "content_type": "reply" if content_key.startswith("R-") else "broadcast",
        "channel": "x_twitter",
        "summary": entry.get("content"),
        "draft_file_path": entry.get("draft_file"),
        "reply_to_tweet_id": entry.get("reply_to_tweet_id"),
    }


def apply_event(db, payload: dict[str, Any]) -> None:
    event = payload["event"]
    entry = payload.get("entry") or {}
    draft_id = str(payload.get("draft_id") or entry.get("draft_id") or "")
    if not draft_id:
        raise ValueError("payload must include draft_id or entry.draft_id")

    item_defaults = item_defaults_from_entry(entry, draft_id)

    if event == "queued":
        record_queue_state(
            db,
            draft_id,
            post_at=parse_datetime(entry.get("post_at")),
            tweets=entry.get("tweets") or [],
            queued_by=entry.get("queued_by"),
            item_defaults=item_defaults,
        )
        return

    if event == "posted":
        posted_at = parse_datetime(entry.get("posted_at")) or datetime.utcnow()
        record_posted(
            db,
            draft_id,
            external_post_ids=[str(tweet_id) for tweet_id in entry.get("tweet_ids", [])],
            posted_at=posted_at,
            tweets=entry.get("tweets") or [],
            item_defaults=item_defaults,
        )
        return

    if event in {"blocked", "failed"}:
        reason = str(
            payload.get("reason")
            or entry.get("blocked_reason")
            or entry.get("error")
            or ""
        )
        record_blocked_or_failed(
            db,
            draft_id,
            status=event,
            reason=reason,
            item_defaults=item_defaults,
        )
        return

    if event == "killed":
        row = payload.get("row") or {}
        defaults = item_defaults_from_entry(row, draft_id)
        record_killed(
            db,
            draft_id,
            reason=str(payload.get("reason") or "Killed"),
            item_defaults=defaults,
        )
        return

    raise ValueError(f"unknown event: {event}")


def main() -> int:
    payload = json.load(sys.stdin)
    with get_session() as db:
        apply_event(db, payload)
    print(json.dumps({"ok": True, "event": payload.get("event")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
