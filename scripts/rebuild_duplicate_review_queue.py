#!/usr/bin/env python3
"""
Rebuild player_review_queue from current players in the database.

Rules:
- Clear existing queue items first.
- Add queue rows only for players that have MULTIPLE (>1) candidates
  with similarity >= threshold (default 0.95).
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from sqlalchemy.orm import selectinload

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db.models import Player, PlayerAlias, PlayerReviewQueue
from teelo.db.session import get_session
from teelo.players.aliases import compare_names, extract_last_name, normalize_name


@dataclass
class Candidate:
    player_id: int
    confidence: float


def _build_similarity_map(players: list[Player], threshold: float) -> dict[int, list[Candidate]]:
    """Build candidate list per player using last-name grouping."""
    by_last_name: dict[str, list[Player]] = {}
    for player in players:
        last_name = extract_last_name(player.canonical_name)
        if not last_name:
            continue
        by_last_name.setdefault(last_name, []).append(player)

    # Cache normalized canonical names.
    normalized_name: dict[int, str] = {
        p.id: normalize_name(p.canonical_name) for p in players
    }

    candidates_by_player: dict[int, dict[int, float]] = {p.id: {} for p in players}

    for group in by_last_name.values():
        if len(group) < 2:
            continue
        for i, p1 in enumerate(group):
            n1 = normalized_name[p1.id]
            for j in range(i + 1, len(group)):
                p2 = group[j]
                n2 = normalized_name[p2.id]
                score = compare_names(n1, n2)
                if score < threshold:
                    continue
                candidates_by_player[p1.id][p2.id] = max(candidates_by_player[p1.id].get(p2.id, 0.0), score)
                candidates_by_player[p2.id][p1.id] = max(candidates_by_player[p2.id].get(p1.id, 0.0), score)

    result: dict[int, list[Candidate]] = {}
    for pid, matches in candidates_by_player.items():
        if len(matches) <= 1:
            continue
        result[pid] = [
            Candidate(player_id=other_id, confidence=score)
            for other_id, score in sorted(matches.items(), key=lambda x: x[1], reverse=True)
        ]
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild duplicate review queue from players")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.95,
        help="Minimum similarity score (default: 0.95)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be queued without writing",
    )
    args = parser.parse_args()

    with get_session() as session:
        players = (
            session.query(Player)
            .options(selectinload(Player.aliases))
            .all()
        )
        candidate_map = _build_similarity_map(players, args.threshold)

        if args.dry_run:
            print(f"Would clear existing queue and insert {len(candidate_map)} pending items.")
            for pid, suggestions in list(candidate_map.items())[:20]:
                player = session.get(Player, pid)
                top = ", ".join(f"{c.player_id}:{c.confidence:.3f}" for c in suggestions[:3])
                print(f"- {pid} ({player.canonical_name if player else 'unknown'}) -> {top}")
            if len(candidate_map) > 20:
                print(f"... and {len(candidate_map) - 20} more")
            return 0

        deleted = session.query(PlayerReviewQueue).delete(synchronize_session=False)

        inserted = 0
        for player_id, suggestions in candidate_map.items():
            player = session.get(Player, player_id)
            if not player:
                continue

            row = PlayerReviewQueue(
                scraped_name=player.canonical_name,
                scraped_source="admin_seed",
                scraped_external_id=None,
                match_external_id=f"seed-player-{player_id}",
                tournament_name="Duplicate Seed",
                status="pending",
            )

            top3 = suggestions[:3]
            for idx, candidate in enumerate(top3, start=1):
                setattr(row, f"suggested_player_{idx}_id", candidate.player_id)
                setattr(
                    row,
                    f"suggested_player_{idx}_confidence",
                    Decimal(str(round(candidate.confidence, 4))),
                )

            session.add(row)
            inserted += 1

        session.flush()
        print(
            f"Queue rebuilt: deleted {deleted} existing rows, inserted {inserted} pending duplicate rows "
            f"(threshold={args.threshold:.2f})."
        )
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
