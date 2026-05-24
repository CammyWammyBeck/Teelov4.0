"""Tests for tweet activity dashboard helpers and backfill parsing."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

from teelo.db.models import SocialContentItem, SocialContentPost, SocialContentVersion
from teelo.services.social_content_writer import (
    record_blocked_or_failed,
    record_killed,
    record_posted,
    record_queue_state,
    record_version,
    upsert_item,
)
from teelo.web.services.tweet_activity_service import (
    content_item_count,
    display_content_version,
    list_content_items,
)

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "backfill_tweet_activity.py"
EVENT_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "social_content_event.py"


def _load_backfill_module():
    spec = importlib.util.spec_from_file_location("backfill_tweet_activity", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_event_module():
    spec = importlib.util.spec_from_file_location("social_content_event", EVENT_SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_content_plan_parser_accepts_broadcasts_and_replies(tmp_path):
    backfill = _load_backfill_module()
    plan = tmp_path / "content-plan.md"
    plan.write_text(
        "\n".join(
            [
                "| ID | Content | Status | Post At (AEDT) | Constraints | Draft File |",
                "|----|---------|--------|----------------|-------------|------------|",
                "| D-190 | RG board | ✅ Posted (2058419801584992695) | "
                "Sun 24 May 15:30 AEST | "
                r"dont_post_if:match_started:1 \| dont_post_if:match_complete:1"
                " | drafts/active/d.md |",
                "",
                "| R-002 | Reply to mention | ⏳ Pending | ASAP | "
                "reply_to_tweet:2057555862944624774 | drafts/active/r.md |",
                "",
            ]
        )
    )

    entries = backfill._parse_content_plan(plan)

    assert [entry["draft_id"] for entry in entries] == ["D-190", "R-002"]
    assert entries[0]["posted_tweet_ids"] == ["2058419801584992695"]
    assert entries[0]["constraints"] == (
        "dont_post_if:match_started:1 | dont_post_if:match_complete:1"
    )
    assert entries[0]["draft_file"] == "drafts/active/d.md"


def test_draft_file_id_parser_reads_embedded_workflow_id(tmp_path):
    backfill = _load_backfill_module()
    draft = tmp_path / "orphan.md"
    draft.write_text("# Draft\n\n**ID:** R-002\n**Created:** 2026-05-22 12:12 AEST\n")

    assert backfill._parse_draft_file_id(draft) == "R-002"


def test_parse_datetime_handles_aest_and_aedt_offsets():
    backfill = _load_backfill_module()

    aest = backfill._parse_datetime("Sun 24 May 15:30 AEST")
    aedt = backfill._parse_datetime("2026-04-04T09:53:30.188887+11:00")

    assert aest is not None
    assert aest.utcoffset().total_seconds() == 10 * 60 * 60
    assert aedt is not None
    assert aedt.utcoffset().total_seconds() == 11 * 60 * 60


def test_backfill_write_mode_requires_explicit_confirmation():
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--write"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "Refusing to write without --confirm-db-write" in result.stdout


def test_backfill_dry_run_summary_counts_versions_and_gaps():
    backfill = _load_backfill_module()
    records = [
        backfill.SocialContentRecord(
            content_key="D-001",
            status="posted",
            content_type="broadcast",
            posted_tweet_ids=["2058419801584992695"],
            versions=[backfill.DraftVersion(version_key="approved")],
        ),
        backfill.SocialContentRecord(
            content_key="R-002",
            status="queued",
            content_type="reply",
            versions=[],
        ),
    ]

    summary = backfill.build_dry_run_summary(records)

    assert summary["records"] == 2
    assert summary["status_counts"] == {"posted": 1, "queued": 1}
    assert summary["type_counts"] == {"broadcast": 1, "reply": 1}
    assert summary["version_count"] == 1
    assert summary["post_count"] == 1
    assert summary["records_with_versions"] == 1
    assert summary["records_without_versions"] == 1
    assert summary["records_without_versions_keys"] == ["R-002"]
    assert summary["posted_missing_posted_at"] == ["D-001"]


def test_tweet_activity_search_matches_child_post_ids(db_session):
    item = SocialContentItem(
        content_key="D-999",
        content_type="broadcast",
        status="posted",
        channel="x_twitter",
        current_version_key="approved",
        summary="Roland Garros board",
    )
    db_session.add(item)
    db_session.flush()
    db_session.add_all(
        [
            SocialContentPost(
                content_item_id=item.id,
                posted_tweet_id="2058419801584992695",
                status="success",
            ),
            SocialContentPost(
                content_item_id=item.id,
                posted_tweet_id="2058419803275321352",
                status="success",
            ),
        ]
    )
    db_session.commit()

    results = list_content_items(db_session, query="2058419803275321352")

    assert [result.content_key for result in results] == ["D-999"]
    assert content_item_count(db_session, query="205841980") == 1


def test_display_content_version_prefers_status_snapshot_then_current():
    item = SocialContentItem(
        content_key="D-998",
        content_type="broadcast",
        status="posted",
        channel="x_twitter",
        current_version_key="v2",
        versions=[
            SocialContentVersion(version_key="v1", event="draft_created", content_text="first"),
            SocialContentVersion(version_key="approved", event="approved", content_text="approved"),
            SocialContentVersion(version_key="v2", event="draft_created", content_text="current"),
        ],
    )
    fallback = SocialContentItem(
        content_key="D-997",
        content_type="broadcast",
        status="draft",
        channel="x_twitter",
        current_version_key="v2",
        versions=[
            SocialContentVersion(version_key="v1", event="draft_created", content_text="first"),
            SocialContentVersion(version_key="approved", event="approved", content_text="approved"),
            SocialContentVersion(version_key="v2", event="draft_created", content_text="current"),
        ],
    )

    assert display_content_version(item).content_text == "approved"
    assert display_content_version(fallback).content_text == "current"


def test_social_content_writer_is_idempotent(db_session):
    upsert_item(db_session, "D-200", summary="Original", status="draft")
    upsert_item(db_session, "D-200", summary="Updated", status="approved")
    record_version(
        db_session,
        "D-200",
        "v1",
        event="draft_created",
        content_text="first text",
        set_current=True,
    )
    record_version(
        db_session,
        "D-200",
        "v1",
        event="draft_created",
        content_text="revised text",
        set_current=True,
    )
    db_session.commit()

    item = db_session.query(SocialContentItem).filter_by(content_key="D-200").one()

    assert item.summary == "Updated"
    assert item.status == "approved"
    assert item.current_version_key == "v1"
    assert len(item.versions) == 1
    assert item.versions[0].content_text == "revised text"


def test_social_content_writer_records_queue_post_and_kill(db_session):
    tweets = [{"text": "Tweet one"}, {"text": "Tweet two"}]

    queued = record_queue_state(
        db_session,
        "D-201",
        post_at=None,
        tweets=tweets,
        queued_by="test-agent",
        item_defaults={"summary": "Queue test"},
    )
    posted = record_posted(
        db_session,
        "D-201",
        external_post_ids=["2058419801584992695", "2058419803275321352"],
        posted_at=queued.created_at,
        tweets=tweets,
        item_defaults={"summary": "Queue test"},
    )
    record_posted(
        db_session,
        "D-201",
        external_post_ids=["2058419801584992695", "2058419803275321352"],
        posted_at=queued.created_at,
        tweets=tweets,
        item_defaults={"summary": "Queue test"},
    )
    record_killed(db_session, "D-202", reason="bad data")
    record_blocked_or_failed(db_session, "D-203", status="blocked", reason="constraint")
    db_session.commit()

    assert posted.status == "posted"
    assert posted.posted_tweet_id == "2058419801584992695"
    assert len(posted.posts) == 2
    assert {post.posted_tweet_id for post in posted.posts} == {
        "2058419801584992695",
        "2058419803275321352",
    }
    assert db_session.query(SocialContentPost).count() == 2
    killed = db_session.query(SocialContentItem).filter_by(content_key="D-202").one()
    blocked = db_session.query(SocialContentItem).filter_by(content_key="D-203").one()
    assert killed.status == "killed"
    assert blocked.status == "blocked"


def test_social_content_event_dispatches_queue_post_block_and_kill(db_session):
    events = _load_event_module()
    entry = {
        "draft_id": "R-300",
        "content": "Reply test",
        "draft_file": "drafts/active/reply.md",
        "queued_by": "test-agent",
        "post_at": "2026-05-24 20:30",
        "reply_to_tweet_id": "2057555862944624774",
        "tweets": [{"text": "Reply text"}],
    }

    events.apply_event(db_session, {"event": "queued", "entry": entry})
    events.apply_event(
        db_session,
        {
            "event": "posted",
            "entry": {
                **entry,
                "posted_at": "2026-05-24T20:35:00+10:00",
                "tweet_ids": ["2059000000000000001"],
            },
        },
    )
    events.apply_event(
        db_session,
        {
            "event": "blocked",
            "entry": {**entry, "draft_id": "D-301"},
            "reason": "constraint",
        },
    )
    events.apply_event(
        db_session,
        {
            "event": "killed",
            "draft_id": "D-302",
            "row": {"content": "Kill test", "draft_file": "drafts/archive/kill.md"},
            "reason": "Killed by Cam",
        },
    )
    db_session.commit()

    reply = db_session.query(SocialContentItem).filter_by(content_key="R-300").one()
    blocked = db_session.query(SocialContentItem).filter_by(content_key="D-301").one()
    killed = db_session.query(SocialContentItem).filter_by(content_key="D-302").one()

    assert reply.content_type == "reply"
    assert reply.status == "posted"
    assert reply.reply_to_tweet_id == "2057555862944624774"
    assert reply.posted_tweet_id == "2059000000000000001"
    assert blocked.status == "blocked"
    assert killed.status == "killed"
