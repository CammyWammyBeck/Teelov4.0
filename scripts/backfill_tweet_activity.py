#!/usr/bin/env python3
"""
Backfill social_content tables from existing workspace files.

Reads: content-plan.md, data/tweet_queue.jsonl, data/tweet_queue_deleted.jsonl,
       drafts/active/*.md, drafts/archive/*.md, tweet-history.md

Produces a dry-run report showing what would be written. Run with --write to
actually write to the database.

Usage:
    python scripts/backfill_tweet_activity.py --dry-run   (default)
    python scripts/backfill_tweet_activity.py --write
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

WORKSPACE = Path.home() / ".openclaw" / "workspace"
QUEUE_FILE = WORKSPACE / "data" / "tweet_queue.jsonl"
DELETED_QUEUE_FILE = WORKSPACE / "data" / "tweet_queue_deleted.jsonl"
CONTENT_PLAN = WORKSPACE / "content-plan.md"
TWEET_HISTORY = WORKSPACE / "tweet-history.md"
DRAFTS_ACTIVE = WORKSPACE / "drafts" / "active"
DRAFTS_ARCHIVE = WORKSPACE / "drafts" / "archive"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class DraftVersion:
    version_key: str
    content_text: str = ""
    event: str = "draft_created"
    note: str = ""
    review_result: str | None = None
    char_count: int | None = None
    created_at: datetime | None = None


@dataclass
class SocialContentRecord:
    content_key: str
    content_type: str = "broadcast"
    status: str = "draft"
    channel: str = "x_twitter"
    reply_to_tweet_id: str | None = None
    reply_to_handle: str | None = None
    post_at: datetime | None = None
    posted_at: datetime | None = None
    posted_tweet_ids: list[str] = field(default_factory=list)
    draft_file_path: str | None = None
    summary: str = ""
    versions: list[DraftVersion] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

STATUS_MAP = {
    "✅ posted": "posted",
    "❌ killed": "killed",
    "❌ killed by reviewer": "killed",
    "✅ approved / queued": "queued",
    "✅ posted (": "posted",
    "✅ approved": "approved",
    "✅ queued": "queued",
    "🔶 posted": "posted",
    "❌ killed —": "killed",
    "❌ killed — reviewer blocked": "killed",
    "✅ posted (queued": "posted",
    "✅ posted (reply": "posted",
    "❌ killed — data contamination": "killed",
}


def _normalise_status(raw: str) -> str:
    raw = raw.strip().lower()
    for pattern, status in [
        ("posted", "posted"),
        ("killed", "killed"),
        ("approved / queued", "queued"),
        ("approved", "approved"),
        ("queued", "queued"),
        ("review_pending", "review_pending"),
        ("failed_review", "failed_review"),
    ]:
        if pattern in raw:
            return status
    return "draft"


def _parse_posted_ids(raw: str) -> list[str]:
    """Extract tweet IDs from content-plan status string like '✅ Posted (ID1, ID2, ID3)'"""
    ids = re.findall(r"\d{16,}", raw)
    return ids


def _parse_datetime(raw: Any) -> datetime | None:
    """Parse workspace date strings without guessing deliberately loose values."""
    if raw is None:
        return None
    value = str(raw).strip()
    if not value or value in {"—", "-", "ASAP"}:
        return None

    value = value.replace("⚠️ stale", "").strip()
    tz_map = {
        "AEDT": timezone(timedelta(hours=11)),
        "AEST": timezone(timedelta(hours=10)),
    }
    tzinfo = None
    for suffix, mapped_tz in tz_map.items():
        if value.endswith(f" {suffix}"):
            value = value[: -len(suffix)].strip()
            tzinfo = mapped_tz
            break

    try:
        parsed = datetime.fromisoformat(value)
        return parsed.replace(tzinfo=tzinfo) if tzinfo and parsed.tzinfo is None else parsed
    except ValueError:
        pass

    value = re.sub(r"^[A-Za-z]{3}\s+", "", value)
    for fmt in ("%Y-%m-%d %H:%M", "%d %b %H:%M"):
        try:
            if fmt.startswith("%d"):
                current_year = datetime.now(UTC).year
                parsed = datetime.strptime(f"{current_year} {value}", f"%Y {fmt}")
            else:
                parsed = datetime.strptime(value, fmt)
            return parsed.replace(tzinfo=tzinfo) if tzinfo else parsed
        except ValueError:
            continue
    return None


def _parse_tweet_history(path: Path) -> dict[str, tuple[str, datetime | None, str | None]]:
    """Parse tweet-history.md into {tweet_id: (text, posted_at, reply_to_tweet_id)}.

    Supports two section header formats:
      - `### Tweet {id}`  (most recent entries)
      - `### Tweet {id} (ATP Miami Final preview — tweet 1)`  (legacy named entries)
    """
    results: dict[str, tuple[str, datetime | None, str | None]] = {}

    if not path.exists():
        return results

    with open(path) as f:
        full_content = f.read()

    # Split into tweet sections on ### Tweet {id} header
    sections = re.split(r"(?m)^### Tweet ", full_content)

    for raw_section in sections:
        if not raw_section.strip():
            continue

        # First line is the tweet ID (may include legacy label)
        lines = raw_section.splitlines()
        m = re.match(r"^(\d+)\b", lines[0])
        if not m:
            continue
        tweet_id = m.group(1)

        # Everything after the first line is the section body
        body = "\n".join(lines[1:])

        text = ""
        posted_at: datetime | None = None
        reply_to: str | None = None

        for line in body.splitlines():
            line = line.rstrip()
            if not line:
                continue
            # Match lines like: - **Posted:** 2026-03-30 15:30 AEDT
            m = re.match(r"^-\s+\*\*Posted:\*\*\s+(.+)", line)
            if m:
                posted_at = _parse_datetime(m.group(1))
                continue
            m = re.match(r"^-\s+\*\*Text:\*\*\s+(.+)", line)
            if m:
                text = m.group(1)
                continue
            m = re.match(r"^-\s+\*\*Notes:\*\*\s+Reply to\s+(\d+)", line)
            if m:
                reply_to = m.group(1)
                continue

        results[tweet_id] = (text.strip(), posted_at, reply_to)

    return results


def _parse_queue_file(path: Path) -> list[dict]:
    records = []
    if not path.exists():
        return records
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def _parse_content_plan(path: Path) -> list[dict]:
    """Extract D-XXX and R-XXX entries from content-plan.md markdown table."""
    entries = []
    in_table = False

    with open(path) as f:
        lines = f.readlines()

    for line in lines:
        # Detect the table header
        if re.match(r"\| ID \|", line):
            in_table = True
            continue
        if in_table and re.match(r"\|\s*[-–]+\s*\|", line):
            continue
        # End of table
        if in_table and not line.startswith("|"):
            break

        if not in_table:
            continue

        # Split by pipe, strip non-whitespace
        cols = [c.strip() for c in line.split("|")]
        cols = [c for c in cols if c]

        if not cols or not (cols[0].startswith("D-") or cols[0].startswith("R-")):
            continue

        draft_id = re.match(r"[DR]-(\d+)", cols[0])
        if not draft_id:
            continue

        # cols: ID, Content, Status, Post At, Constraints, Draft File
        record: dict[str, Any] = {
            "draft_id": cols[0],
            "content": cols[1] if len(cols) > 1 else "",
            "status": cols[2] if len(cols) > 2 else "",
            "post_at": cols[3] if len(cols) > 3 else "",
            "constraints": cols[4] if len(cols) > 4 else "",
            "draft_file": cols[5] if len(cols) > 5 else "",
        }

        # Extract posted tweet IDs from status field
        ids = _parse_posted_ids(record["status"])
        if ids:
            record["posted_tweet_ids"] = ids

        entries.append(record)

    return entries


def _parse_draft_file(path: Path) -> tuple[str | None, list[DraftVersion]]:
    """Parse a draft .md file into summary + list of DraftVersion."""
    if not path.exists():
        return None, []

    with open(path) as f:
        raw = f.read()

    summary = re.search(r"\*\*Content:\*\* (.+)", raw)
    summary = summary.group(1) if summary else ""

    versions: list[DraftVersion] = []

    # Extract v1, v2, Final Approved Version, Notes sections
    # Each draft section has a header and content block

    sections = re.split(r"(?m)^---+\s*$", raw)
    for section in sections:
        section = section.strip()
        if not section:
            continue

        # Detect section type
        vk: str | None = None
        event = "draft_created"
        content_text = ""
        note = ""
        review_result: str | None = None
        created_at: datetime | None = None

        # Created timestamp
        created_m = re.search(r"\*\*Created:\*\* (.+)", section)
        if created_m:
            created_at = _parse_datetime(created_m.group(1))

        # Version header detection
        draft_version_m = re.search(r"## Draft v(\d+)", section)
        if draft_version_m:
            vk = f"v{draft_version_m.group(1)}"
            event = "draft_created"
            # Strip header lines and metadata, keep body
            body_lines = []
            in_body = False
            for ln in section.splitlines():
                if re.match(r"## Draft v\d+", ln):
                    in_body = True
                    continue
                if in_body and ln.startswith("**"):
                    break
                if in_body:
                    body_lines.append(ln)
            content_text = "\n".join(body_lines).strip()

        elif re.search(r"## Final Approved Version", section):
            vk = "approved"
            event = "approved"
            for ln in section.splitlines():
                if ln.startswith(">"):
                    content_text += ln[1:].strip() + "\n"
            content_text = content_text.strip()

        elif re.search(r"## Notes", section):
            vk = "note"
            event = "note"
            note = re.sub(r"\*\*.*?\*\*", "", section).strip()

        elif re.search(r"## Submitted", section):
            vk = "submitted"
            event = "submitted"
            created_m2 = re.search(r"\*\*Submitted:\*\* (.+)", section)
            if created_m2:
                created_at = _parse_datetime(created_m2.group(1)) or created_at

        elif re.search(r"## Reviewer feedback", section):
            vk = "reviewer_feedback"
            event = "reviewer_feedback"
            note = section
            review_result_m = re.search(
                r"(Clean|Auto-approved|Needs revision|Killed|Auto-killed)",
                section,
            )
            if review_result_m:
                review_result = review_result_m.group(1).lower().replace(" ", "_")

        if vk:
            versions.append(DraftVersion(
                version_key=vk,
                content_text=content_text,
                event=event,
                note=note,
                review_result=review_result,
                char_count=len(content_text) if content_text else None,
                created_at=created_at,
            ))

    return summary, versions


# ---------------------------------------------------------------------------
# Merge sources into unified records
# ---------------------------------------------------------------------------

def _build_content_key(draft_id: str) -> str:
    """Convert D-001 → D-001 (unchanged)."""
    return draft_id


def backfill() -> list[SocialContentRecord]:
    """
    Merge all sources into SocialContentRecord list.
    Runs dry-run only — no database writes.
    """
    records: dict[str, SocialContentRecord] = {}

    # 1. Parse content-plan.md for D-XXX/R-XXX entries
    print("Parsing content-plan.md...")
    plan_entries = _parse_content_plan(CONTENT_PLAN)
    print(f"  → {len(plan_entries)} entries found")

    for entry in plan_entries:
        ck = entry["draft_id"]  # e.g. D-001
        summary = entry.get("content", "") or entry.get("draft_file", "") or ""
        status_raw = entry.get("status", "")
        status = _normalise_status(status_raw)

        post_at = _parse_datetime(entry.get("post_at"))

        draft_file = entry.get("draft_file", "")
        posted_ids = entry.get("posted_tweet_ids", [])

        rec = records.setdefault(ck, SocialContentRecord(
            content_key=ck,
            summary=summary,
            draft_file_path=draft_file,
            post_at=post_at,
        ))
        rec.status = status
        if posted_ids:
            rec.posted_tweet_ids = posted_ids

        # If posted, record posted_at from first tweet ID in history if available
        if status == "posted" and not rec.posted_at:
            if posted_ids:
                # posted_at will be resolved from tweet-history later
                pass

    # 2. Parse tweet_queue.jsonl
    print("Parsing tweet_queue.jsonl...")
    queue_records = _parse_queue_file(QUEUE_FILE)
    print(f"  → {len(queue_records)} entries found")

    for qr in queue_records:
        ck = qr.get("draft_id", "")
        if not ck or not (ck.startswith("D-") or ck.startswith("R-")):
            continue

        post_at = _parse_datetime(qr.get("post_at"))
        posted_at = _parse_datetime(qr.get("posted_at"))

        rec = records.setdefault(ck, SocialContentRecord(content_key=ck))
        rec.status = qr.get("status", "draft")
        rec.post_at = post_at
        rec.posted_at = posted_at
        rec.posted_tweet_ids = qr.get("tweet_ids", [])
        rec.draft_file_path = qr.get("draft_file", "") or rec.draft_file_path
        rec.summary = qr.get("content", "") or rec.summary
        if ck.startswith("R-"):
            rec.content_type = "reply"
            rec.reply_to_tweet_id = qr.get("reply_to_tweet_id") or None

        # Add a version for each tweet in the thread
        tweets = qr.get("tweets", [])
        if tweets:
            # First tweet text becomes the approved version
            first_text = "\n".join(t.get("text", "") for t in tweets)
            rec.versions.append(DraftVersion(
                version_key="approved",
                content_text=first_text,
                event="approved",
                char_count=len(first_text),
            ))

    # 3. Parse deleted queue
    print("Parsing tweet_queue_deleted.jsonl...")
    deleted_records = _parse_queue_file(DELETED_QUEUE_FILE)
    print(f"  → {len(deleted_records)} entries found")

    for dr in deleted_records:
        ck = dr.get("draft_id", "")
        if not ck or not (ck.startswith("D-") or ck.startswith("R-")):
            continue
        rec = records.setdefault(ck, SocialContentRecord(content_key=ck))
        rec.status = "killed"
        rec.draft_file_path = dr.get("draft_file", "") or rec.draft_file_path
        rec.summary = dr.get("content", "") or rec.summary

    # 4. Parse draft files for version history
    print("Parsing draft files...")
    draft_files = list(DRAFTS_ACTIVE.glob("*.md")) + list(DRAFTS_ARCHIVE.glob("*.md"))
    print(f"  → {len(draft_files)} draft files found")

    for fp in draft_files:
        summary, versions = _parse_draft_file(fp)
        if not versions and not summary:
            continue

        # Derive content_key from filename
        # e.g. 2026-03-30-atp-miami-reaction.md → D-001 via content-plan lookup
        # For files without explicit D-XXX in frontmatter, match via draft file path in plan_entries
        ck = None
        for entry in plan_entries:
            entry_file = entry.get("draft_file", "")
            if entry_file and fp.name in entry_file:
                ck = entry["draft_id"]
                break

        if ck is None:
            # Try to find via queue
            for qr in queue_records:
                if qr.get("draft_file", "") and fp.name in qr["draft_file"]:
                    ck = qr["draft_id"]
                    break

        if ck is None:
            # Last resort: skip (orphan file)
            print(f"  ⚠ Could not resolve content_key for {fp.name}")
            continue

        rec = records.setdefault(ck, SocialContentRecord(content_key=ck))
        if summary:
            rec.summary = summary
        if fp.name.startswith("drafts/active"):
            pass  # don't change status
        if not rec.draft_file_path:
            rec.draft_file_path = str(fp.relative_to(WORKSPACE))

        for v in versions:
            # Avoid duplicate version keys
            existing_keys = {vr.version_key for vr in rec.versions}
            if v.version_key not in existing_keys:
                rec.versions.append(v)

    # 5. Parse tweet-history.md for posted tweet metadata
    print("Parsing tweet-history.md...")
    history = _parse_tweet_history(TWEET_HISTORY)
    print(f"  → {len(history)} tweet entries found")

    for tweet_id, (text, posted_at, reply_to) in history.items():
        # Resolve reply relationships: attach reply_to_tweet_id to items
        # that have this tweet as their posted_tweet_id
        for rec in records.values():
            if tweet_id in rec.posted_tweet_ids:
                if not rec.posted_at and posted_at:
                    rec.posted_at = posted_at
                if reply_to and rec.reply_to_tweet_id is None:
                    # Set reply metadata on the content item
                    pass  # resolved at item level after records merged
                break

    return list(records.values())


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_to_db(records: list[SocialContentRecord], batch_size: int = 10) -> tuple[int, int, int]:
    """Write all records to Postgres in batches.

    Returns (items_created, versions_created, posts_created).
    """
    from teelo.db.models import SocialContentItem, SocialContentPost, SocialContentVersion
    from teelo.db.session import get_session

    items_created = 0
    versions_created = 0
    posts_created = 0

    for batch_start in range(0, len(records), batch_size):
        batch = records[batch_start:batch_start + batch_size]
        with get_session() as db:
            for rec in batch:
                # Check if item already exists
                existing = db.query(SocialContentItem).filter(
                    SocialContentItem.content_key == rec.content_key
                ).first()
                if existing:
                    db_item = existing
                else:
                    db_item = SocialContentItem(
                        content_key=rec.content_key,
                        current_version_key="v1",
                        content_type=rec.content_type,
                        status=rec.status,
                        channel=rec.channel,
                    )
                    db.add(db_item)
                    db.flush()
                    items_created += 1

                version_keys = [v.version_key for v in rec.versions]
                meaningful_vks = [vk for vk in version_keys if vk != "note"]
                db_item.content_type = rec.content_type
                db_item.status = rec.status
                db_item.channel = rec.channel
                db_item.current_version_key = (meaningful_vks or ["v1"])[-1]
                db_item.reply_to_tweet_id = rec.reply_to_tweet_id
                db_item.reply_to_handle = rec.reply_to_handle
                db_item.post_at = rec.post_at
                db_item.posted_at = rec.posted_at
                db_item.posted_tweet_id = rec.posted_tweet_ids[0] if rec.posted_tweet_ids else None
                db_item.draft_file_path = (rec.draft_file_path or "")[:500]
                db_item.summary = (rec.summary or "")[:255]

                # Versions — deduplicate by version_key before inserting
                seen_vkeys: set[str] = set()
                for v in rec.versions:
                    vk = (v.version_key[:30] if v.version_key else v.version_key)
                    if vk in seen_vkeys:
                        continue
                    seen_vkeys.add(vk)
                    existing_v = db.query(SocialContentVersion).filter(
                        SocialContentVersion.content_item_id == db_item.id,
                        SocialContentVersion.version_key == vk,
                    ).first()
                    if not existing_v:
                        db_v = SocialContentVersion(
                            content_item_id=db_item.id,
                            version_key=vk,
                            content_text=v.content_text,
                            event=v.event,
                            note=(v.note or "")[:500] if v.note else None,
                            review_result=v.review_result,
                            char_count=v.char_count,
                            created_at=v.created_at or datetime.utcnow(),
                        )
                        db.add(db_v)
                        versions_created += 1

                # Posts
                for tweet_id in rec.posted_tweet_ids:
                    existing_post = db.query(SocialContentPost).filter(
                        SocialContentPost.content_item_id == db_item.id,
                        SocialContentPost.posted_tweet_id == tweet_id,
                    ).first()
                    if not existing_post:
                        db_post = SocialContentPost(
                            content_item_id=db_item.id,
                            posted_tweet_id=tweet_id,
                            posted_at=rec.posted_at or datetime.utcnow(),
                            status="success" if rec.status == "posted" else "cancelled",
                        )
                        db.add(db_post)
                        posts_created += 1
        print(f"  Batch {batch_start // batch_size + 1}: wrote {len(batch)} items")

    return items_created, versions_created, posts_created


def print_dry_run(records: list[SocialContentRecord]):
    print("\n" + "=" * 80)
    print(f"DRY RUN — {len(records)} content items would be created\n")

    for rec in sorted(records, key=lambda r: r.content_key):
        print(f"{rec.content_key} | {rec.status:20s} | {rec.summary[:60]!r}")
        print(f"  Types: {rec.content_type} | Channel: {rec.channel}")
        print(f"  Post at: {rec.post_at} | Posted at: {rec.posted_at}")
        print(f"  Draft file: {rec.draft_file_path}")
        if rec.posted_tweet_ids:
            print(f"  Tweet IDs: {rec.posted_tweet_ids}")
        print(f"  Versions ({len(rec.versions)}): ", end="")
        if rec.versions:
            vkeys = ", ".join(v.version_key for v in rec.versions)
            print(vkeys)
        else:
            print("(none)")
        print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="write records to the configured DB")
    parser.add_argument(
        "--confirm-db-write",
        action="store_true",
        help="required with --write to guard against accidental live DB backfills",
    )
    args = parser.parse_args()

    if args.write and not args.confirm_db_write:
        print("Refusing to write without --confirm-db-write.")
        print("Run a dry-run first and get explicit approval before live DB writes.")
        sys.exit(2)

    records = backfill()

    if not args.write:
        print_dry_run(records)
        print("\nTo write to the configured database, rerun with --write --confirm-db-write.")
    else:
        print(f"Writing {len(records)} records to database...")
        items, versions, posts = write_to_db(records)
        print(f"Done. Created: {items} items, {versions} versions, {posts} posts.")
