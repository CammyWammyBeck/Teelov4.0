#!/usr/bin/env python3
"""
List corrupt self-vs-self matches (player_a_id == player_b_id).

Usage:
    python scripts/find_corrupt_self_matches.py
    python scripts/find_corrupt_self_matches.py --limit 500
    python scripts/find_corrupt_self_matches.py --all-statuses
    python scripts/find_corrupt_self_matches.py --json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import or_

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db.session import get_session
from teelo.db.models import Match, Tournament, TournamentEdition

TERMINAL_STATUSES = ("completed", "retired", "walkover", "default")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Find corrupt matches where player_a_id == player_b_id."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Max rows to print (default 200).",
    )
    parser.add_argument(
        "--all-statuses",
        action="store_true",
        help="Include all match statuses (default is terminal statuses only).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of table output.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()

    with get_session() as session:
        query = (
            session.query(
                Match.id,
                Match.external_id,
                Match.temporal_order,
                Match.status,
                Match.round,
                Match.match_date,
                Match.player_a_id,
                Match.player_b_id,
                Match.winner_id,
                Match.elo_post_player_a,
                Match.elo_post_player_b,
                Match.elo_needs_recompute,
                TournamentEdition.year,
                Tournament.name.label("tournament_name"),
            )
            .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
            .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
            .filter(Match.player_a_id == Match.player_b_id)
            .order_by(Match.temporal_order.asc().nullsfirst(), Match.id.asc())
        )

        if not args.all_statuses:
            query = query.filter(Match.status.in_(TERMINAL_STATUSES))

        limit = max(args.limit, 0)
        rows = query.limit(limit).all() if limit > 0 else query.all()

        total_query = session.query(Match.id).filter(Match.player_a_id == Match.player_b_id)
        if not args.all_statuses:
            total_query = total_query.filter(Match.status.in_(TERMINAL_STATUSES))
        total = total_query.count()

        pending_count = (
            session.query(Match.id)
            .filter(Match.player_a_id == Match.player_b_id)
            .filter(Match.status.in_(TERMINAL_STATUSES))
            .filter(Match.winner_id.isnot(None))
            .filter(Match.temporal_order.isnot(None))
            .filter(
                or_(
                    Match.elo_post_player_a.is_(None),
                    Match.elo_post_player_b.is_(None),
                    Match.elo_needs_recompute.is_(True),
                )
            )
            .count()
        )

    payload = []
    for r in rows:
        elo_pending = bool(
            r.winner_id is not None
            and r.temporal_order is not None
            and (
                r.elo_post_player_a is None
                or r.elo_post_player_b is None
                or r.elo_needs_recompute
            )
        )
        payload.append(
            {
                "id": r.id,
                "external_id": r.external_id,
                "temporal_order": r.temporal_order,
                "status": r.status,
                "round": r.round,
                "match_date": r.match_date.isoformat() if r.match_date else None,
                "player_a_id": r.player_a_id,
                "player_b_id": r.player_b_id,
                "winner_id": r.winner_id,
                "elo_needs_recompute": bool(r.elo_needs_recompute),
                "elo_pending": elo_pending,
                "tournament": r.tournament_name,
                "year": r.year,
            }
        )

    if args.json:
        print(
            json.dumps(
                {
                    "total_corrupt_matches": total,
                    "total_elo_pending_terminal_corrupt_matches": pending_count,
                    "returned_rows": len(payload),
                    "rows": payload,
                },
                indent=2,
                default=str,
            )
        )
        return 0

    print(f"Corrupt self-vs-self matches (terminal_only={not args.all_statuses}): {total}")
    print(f"ELO-pending terminal corrupt matches: {pending_count}")
    print(f"Showing {len(payload)} row(s):")
    print(
        "id | temporal_order | status | rnd | player_id | winner_id | elo_pending | elo_needs_recompute | tournament (year)"
    )
    print("-" * 120)
    for row in payload:
        print(
            f"{row['id']} | {row['temporal_order']} | {row['status']} | {row['round']} | "
            f"{row['player_a_id']} | {row['winner_id']} | {row['elo_pending']} | "
            f"{row['elo_needs_recompute']} | {row['tournament']} ({row['year']})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

