#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

from sqlalchemy import and_, or_
import structlog

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db import Player, get_session
from teelo.scrape.player_enrichment import PlayerEnrichmentScraper, PlayerProfile
from teelo.tasks import DBCheckpointStore
from teelo.utils.geo import country_to_ioc

logger = structlog.get_logger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _append_jsonl(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")


def _slugify(name: str) -> str:
    return name.lower().replace(" ", "-").replace("'", "").replace(".", "")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Incremental player profile enrichment (gender-driven ATP/WTA)."
    )
    parser.add_argument("--batch-size", type=int, default=100, help="Maximum players per DB batch.")
    parser.add_argument("--max-players", type=int, default=0, help="Optional cap on processed players (0 = no cap).")
    parser.add_argument(
        "--source",
        choices=["atp", "wta", "both"],
        default="both",
        help="Deprecated/ignored. Source is determined by player gender.",
    )
    parser.add_argument(
        "--checkpoint-key",
        default="player_enrichment_incremental",
        help="Checkpoint key in pipeline_checkpoints.",
    )
    parser.add_argument(
        "--resume",
        dest="resume",
        action="store_true",
        default=True,
        help="Resume from checkpoint cursor (default).",
    )
    parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Ignore checkpoint cursor and scan from beginning.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not persist updates/checkpoints.")
    parser.add_argument(
        "--headless",
        action="store_true",
        default=True,
        help="Run scraper in headless mode (default true).",
    )
    parser.add_argument(
        "--headed",
        dest="headless",
        action="store_false",
        help="Run scraper in headed mode.",
    )
    parser.add_argument("--metrics-json", default=None, help="Write stage metrics JSON")
    parser.add_argument("--status-jsonl", default=None, help="Append status events JSONL")
    return parser


def _profile_updates(player: Player, profile: PlayerProfile) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    if profile.birth_date and not player.birth_date:
        updates["birth_date"] = profile.birth_date
    if profile.height_cm and not player.height_cm:
        updates["height_cm"] = profile.height_cm
    if profile.hand and not player.hand:
        updates["hand"] = profile.hand
    if profile.backhand and not player.backhand:
        updates["backhand"] = profile.backhand
    if profile.turned_pro_year and not player.turned_pro_year:
        updates["turned_pro_year"] = profile.turned_pro_year
    if profile.nationality and not player.nationality_ioc:
        ioc = country_to_ioc(profile.nationality)
        if ioc:
            updates["nationality_ioc"] = ioc
    return updates


async def main_async() -> int:
    args = _build_parser().parse_args()
    status_path = Path(args.status_jsonl) if args.status_jsonl else None
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    payload: dict[str, Any] = {
        "stage": "player_enrichment_incremental",
        "started_at": started_at.isoformat(),
        "batch_size": args.batch_size,
        "max_players": args.max_players,
        "source": "gender",
        "checkpoint_key": args.checkpoint_key,
        "resume": args.resume,
        "dry_run": args.dry_run,
        "processed": 0,
        "updated": 0,
        "unchanged": 0,
        "errors": 0,
        "batches": 0,
        "total": 0,
        "checkpoint_in": None,
        "checkpoint_out": None,
    }

    _append_jsonl(
        status_path,
        {
            "event": "player_enrichment_started",
            "timestamp": _utc_now_iso(),
            "batch_size": args.batch_size,
            "max_players": args.max_players,
            "source": "gender",
            "resume": args.resume,
            "dry_run": args.dry_run,
        },
    )

    with get_session() as session:
        checkpoint_store = DBCheckpointStore(session)
        cursor_id = 0
        if args.resume:
            checkpoint = checkpoint_store.get(args.checkpoint_key)
            if checkpoint:
                saved = checkpoint.get("cursor", {}).get("last_player_id")
                if isinstance(saved, int) and saved > 0:
                    cursor_id = saved
                    payload["checkpoint_in"] = {"last_player_id": cursor_id}

        max_cap = args.max_players if args.max_players > 0 else None

        async with PlayerEnrichmentScraper(headless=args.headless) as scraper:
            needs_fields = or_(
                Player.birth_date.is_(None),
                Player.height_cm.is_(None),
                Player.hand.is_(None),
                Player.backhand.is_(None),
                Player.turned_pro_year.is_(None),
                Player.nationality_ioc.is_(None),
            )
            source_filter = or_(
                and_(Player.gender == "M", Player.atp_id.isnot(None)),
                and_(Player.gender == "F", Player.wta_id.isnot(None)),
            )
            base_query = (
                session.query(Player)
                .filter(
                    Player.id > cursor_id,
                    source_filter,
                    needs_fields,
                )
                .order_by(Player.id.asc())
            )
            total_available = int(base_query.count())
            payload["total"] = (
                min(total_available, max_cap) if max_cap is not None else total_available
            )

            while True:
                if max_cap is not None and payload["processed"] >= max_cap:
                    break

                query = (
                    session.query(Player)
                    .filter(
                        Player.id > cursor_id,
                        source_filter,
                        needs_fields,
                    )
                    .order_by(Player.id.asc())
                )

                limit = args.batch_size
                if max_cap is not None:
                    limit = min(limit, max_cap - payload["processed"])
                players = query.limit(limit).all()
                if not players:
                    break

                payload["batches"] += 1
                batch_processed = 0
                batch_updated = 0
                batch_unchanged = 0
                batch_errors = 0
                for player in players:
                    payload["processed"] += 1
                    batch_processed += 1
                    cursor_id = int(player.id)

                    player_name = player.canonical_name
                    status = "failed"
                    try:
                        profile: PlayerProfile | None = None
                        slug = _slugify(player_name)

                        if player.gender == "M":
                            if not player.atp_id:
                                status = "failed"
                                payload["errors"] += 1
                                batch_errors += 1
                                continue
                            profile = await scraper.scrape_atp_profile(player.atp_id, slug)
                        elif player.gender == "F":
                            if not player.wta_id:
                                status = "failed"
                                payload["errors"] += 1
                                batch_errors += 1
                                continue
                            profile = await scraper.scrape_wta_profile(player.wta_id, slug)
                        else:
                            status = "failed"
                            payload["errors"] += 1
                            batch_errors += 1
                            continue

                        if profile is None:
                            status = "blocked_by_cloudflare"
                            payload["errors"] += 1
                            batch_errors += 1
                            continue

                        updates = _profile_updates(player, profile)
                        if not updates:
                            status = "unchanged"
                            payload["unchanged"] += 1
                            batch_unchanged += 1
                            continue

                        if not args.dry_run:
                            for field, value in updates.items():
                                setattr(player, field, value)
                        status = "updated"
                        payload["updated"] += 1
                        batch_updated += 1
                    except Exception:
                        status = "failed"
                        payload["errors"] += 1
                        batch_errors += 1
                    finally:
                        remaining = max(payload["total"] - payload["processed"], 0)
                        logger.info(
                            "player_enrichment.progress",
                            player=player_name,
                            status=status,
                            processed=payload["processed"],
                            total=payload["total"],
                            remaining=remaining,
                        )
                logger.info(
                    "player_enrichment.batch_summary",
                    updated=batch_updated,
                    unchanged=batch_unchanged,
                    errors=batch_errors,
                    batch_size=batch_processed,
                )

                if args.dry_run:
                    session.rollback()
                else:
                    session.commit()
                    checkpoint_store.set(
                        args.checkpoint_key,
                        {
                            "cursor": {"last_player_id": cursor_id},
                            "updated_at": _utc_now_iso(),
                        },
                    )
                    session.commit()

        if cursor_id > 0:
            payload["checkpoint_out"] = {"last_player_id": cursor_id}

    ended_at = datetime.now(timezone.utc).replace(tzinfo=None)
    payload["ended_at"] = ended_at.isoformat()
    payload["duration_s"] = (ended_at - started_at).total_seconds()
    payload["status"] = "success"

    _append_jsonl(
        status_path,
        {
            "event": "player_enrichment_finished",
            "timestamp": _utc_now_iso(),
            "status": payload["status"],
            "processed": payload["processed"],
            "updated": payload["updated"],
            "errors": payload["errors"],
            "duration_s": payload["duration_s"],
        },
    )
    logger.info(
        "player_enrichment.complete",
        processed=payload["processed"],
        updated=payload["updated"],
        unchanged=payload["unchanged"],
        errors=payload["errors"],
    )

    if args.metrics_json:
        _write_json(Path(args.metrics_json), payload)

    print(
        "Player enrichment incremental complete: "
        f"processed={payload['processed']} updated={payload['updated']} "
        f"unchanged={payload['unchanged']} errors={payload['errors']}"
    )
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
