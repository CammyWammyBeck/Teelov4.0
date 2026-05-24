"""Tests for tweet activity dashboard helpers and backfill parsing."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

from teelo.db.models import SocialContentItem, SocialContentPost
from teelo.services.social_content_writer import (
    record_blocked_or_failed,
    record_killed,
    record_posted,
    record_queue_state,
    record_version,
    upsert_item,
)
from teelo.web.services.tweet_activity_service import content_item_count, list_content_items

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "backfill_tweet_activity.py"


def _load_backfill_module():
    spec = importlib.util.spec_from_file_location("backfill_tweet_activity", SCRIPT_PATH)
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
