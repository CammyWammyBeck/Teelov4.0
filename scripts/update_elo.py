#!/usr/bin/env python3
"""
Apply incremental ELO updates for terminal matches.

Normal usage (hourly pipeline — process all unprocessed matches):
    python scripts/update_elo.py

Full rebuild (after param change or data corruption):
    python scripts/update_elo.py --rebuild

Fast inline mode (after scraping a batch — only process specific players):
    python scripts/update_elo.py --player-ids 123,456,789

Dry run (see what would be processed without writing anything):
    python scripts/update_elo.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

# Add src to path so this script can be run directly
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db import get_session
from teelo.elo.updater import EloUpdater


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Apply ELO updates for terminal matches.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Full rebuild: clear all ELO data and reprocess from scratch.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute updates but do not write to the database.",
    )
    parser.add_argument(
        "--player-ids",
        default=None,
        help="Comma-separated player IDs to process (fast path for post-scrape use).",
    )
    parser.add_argument(
        "--metrics-json",
        default=None,
        help="Write a JSON summary to this path on completion.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()

    # Parse optional player ID filter
    player_ids: set[int] | None = None
    if args.player_ids:
        try:
            player_ids = {int(x.strip()) for x in args.player_ids.split(",") if x.strip()}
        except ValueError as exc:
            print(f"ERROR: --player-ids must be comma-separated integers: {exc}")
            return 1

    mode = "rebuild" if args.rebuild else "incremental"
    started_at = _utc_now_iso()
    print(f"ELO UPDATE  mode={mode}  dry_run={args.dry_run}  started={started_at}")
    if player_ids:
        print(f"Player filter: {sorted(player_ids)}")
    print("-" * 60)

    t_start = perf_counter()

    with get_session() as session:
        updater = EloUpdater.from_session(session)

        if args.rebuild:
            result = updater.rebuild(session)
        else:
            result = updater.run(session, player_ids=player_ids)

        if args.dry_run:
            session.rollback()
            print("(dry run — changes rolled back)")
        else:
            session.commit()

    elapsed = perf_counter() - t_start

    # Print summary
    print("-" * 60)
    print(f"Processed:              {result.processed}")
    print(f"Pre-snapshots updated:  {result.pre_snapshots_refreshed}")
    if result.backfill_triggered:
        print(f"Backfill triggered:     YES  (temporal={result.backfill_temporal})")
    print(f"Elapsed:                {elapsed:.2f}s")

    # Write metrics JSON if requested (used by run_hourly_update.py)
    if args.metrics_json:
        payload = {
            "status": "success",
            "mode": mode,
            "dry_run": args.dry_run,
            "started_at": started_at,
            "elapsed_s": round(elapsed, 3),
            "processed": result.processed,
            "pre_snapshots_refreshed": result.pre_snapshots_refreshed,
            "backfill_triggered": result.backfill_triggered,
            "backfill_temporal": result.backfill_temporal,
        }
        metrics_path = Path(args.metrics_json)
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
