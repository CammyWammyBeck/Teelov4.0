#!/usr/bin/env python3
"""
Remove synthetic BYE matches and BYE player records from the database.

BYE matches were created by draw_ingestion as a workaround to make the
tournament forecast readiness check work. This has since been fixed — the
forecast check now uses bracket math instead. BYE matches should never have
existed: they caused ELO adjustments on real players and showed up on the
website as completed matches.

This script:
  1. Finds all Match rows where score='BYE' (or player_b has canonical_name='BYE')
  2. Clears ELO fields on all matches processed AFTER the earliest BYE match,
     marking them for recompute
  3. Deletes the BYE Match rows
  4. Deletes the BYE Player rows

Usage:
    python scripts/remove_bye_matches.py --dry-run   # preview only
    python scripts/remove_bye_matches.py             # apply changes
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db.session import get_session
from teelo.db.models import Match, Player


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Remove synthetic BYE matches and players.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be changed without modifying the database.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    dry_run = args.dry_run

    with get_session() as session:
        # Find BYE player IDs
        bye_players = (
            session.query(Player)
            .filter(Player.canonical_name == "BYE")
            .all()
        )
        bye_player_ids = {p.id for p in bye_players}
        print(f"BYE players found: {len(bye_players)} (ids: {sorted(bye_player_ids)})")

        # Find all BYE matches
        bye_matches_query = session.query(Match).filter(Match.score == "BYE")
        if bye_player_ids:
            from sqlalchemy import or_
            bye_matches_query = session.query(Match).filter(
                or_(
                    Match.score == "BYE",
                    Match.player_b_id.in_(bye_player_ids),
                )
            )
        bye_matches = bye_matches_query.all()
        print(f"BYE matches found: {len(bye_matches)}")

        if not bye_matches:
            print("Nothing to clean up.")
            return 0

        # Find the earliest temporal_order among BYE matches to know which
        # real matches need ELO recompute (anything processed after the first BYE)
        bye_temporal_orders = [
            m.temporal_order for m in bye_matches if m.temporal_order is not None
        ]
        earliest_bye_order = min(bye_temporal_orders) if bye_temporal_orders else None
        print(f"Earliest BYE temporal_order: {earliest_bye_order}")

        # Mark all real matches at or after the earliest BYE for ELO recompute
        if earliest_bye_order is not None:
            recompute_query = (
                session.query(Match)
                .filter(
                    Match.temporal_order >= earliest_bye_order,
                    Match.score != "BYE",
                    Match.elo_post_player_a.isnot(None),
                )
            )
            if bye_player_ids:
                from sqlalchemy import and_
                recompute_query = (
                    session.query(Match)
                    .filter(
                        Match.temporal_order >= earliest_bye_order,
                        Match.score != "BYE",
                        ~Match.player_b_id.in_(bye_player_ids),
                        Match.elo_post_player_a.isnot(None),
                    )
                )
            recompute_matches = recompute_query.all()
            print(f"Real matches to flag for ELO recompute: {len(recompute_matches)}")

            if not dry_run:
                for m in recompute_matches:
                    m.elo_needs_recompute = True

        if dry_run:
            print("\n-- DRY RUN: no changes made --")
            print(f"Would delete {len(bye_matches)} BYE match(es)")
            print(f"Would delete {len(bye_players)} BYE player(s)")
            if earliest_bye_order is not None:
                print(f"Would flag {len(recompute_matches)} match(es) for ELO recompute")
        else:
            from teelo.db.models import MatchFeatures, PlayerEloState, PlayerSurfaceEloState
            bye_match_ids = [m.id for m in bye_matches]

            features_deleted = (
                session.query(MatchFeatures)
                .filter(MatchFeatures.match_id.in_(bye_match_ids))
                .delete(synchronize_session=False)
            )
            print(f"Deleted {features_deleted} match_features row(s) for BYE matches")

            elo_states_deleted = (
                session.query(PlayerEloState)
                .filter(PlayerEloState.player_id.in_(bye_player_ids))
                .delete(synchronize_session=False)
            )
            surface_elo_states_deleted = (
                session.query(PlayerSurfaceEloState)
                .filter(PlayerSurfaceEloState.player_id.in_(bye_player_ids))
                .delete(synchronize_session=False)
            )
            print(f"Deleted {elo_states_deleted} player_elo_states and {surface_elo_states_deleted} player_surface_elo_states for BYE players")

            for m in bye_matches:
                session.delete(m)
            for p in bye_players:
                session.delete(p)
            session.commit()
            print(f"Deleted {len(bye_matches)} BYE match(es)")
            print(f"Deleted {len(bye_players)} BYE player(s)")
            if earliest_bye_order is not None:
                print(f"Flagged {len(recompute_matches)} match(es) for ELO recompute")
            print("Done. Run the ELO updater to recompute affected ratings.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
