#!/usr/bin/env python3
"""
Split mixed-gender player records into separate players.

A player is considered mixed when they appear in both men and women tournaments.
This script moves the minority-gender matches to a newly created player record.

Usage:
  py scripts/split_mixed_gender_players.py --dry-run
  py scripts/split_mixed_gender_players.py --execute
  py scripts/split_mixed_gender_players.py --execute --player-id 7
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import case, func, or_

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db.models import Match, Player, PlayerAlias, PlayerEloState, Tournament, TournamentEdition
from teelo.db.session import get_session
from teelo.players.aliases import extract_last_name, normalize_name


@dataclass
class MixedPlayer:
    player_id: int
    name: str
    men_matches: int
    women_matches: int
    majority_gender: str
    minority_gender: str
    minority_match_ids: list[int]


def _find_mixed_players(session, player_id: int | None = None) -> list[MixedPlayer]:
    events_a = (
        session.query(
            Match.player_a_id.label("pid"),
            Tournament.gender.label("gender"),
            Match.id.label("match_id"),
        )
        .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .filter(Tournament.gender.in_(("men", "women")))
    )
    events_b = (
        session.query(
            Match.player_b_id.label("pid"),
            Tournament.gender.label("gender"),
            Match.id.label("match_id"),
        )
        .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .filter(Tournament.gender.in_(("men", "women")))
    )
    events = events_a.union_all(events_b).subquery()

    counts_query = (
        session.query(
            events.c.pid.label("pid"),
            func.sum(case((events.c.gender == "men", 1), else_=0)).label("men_count"),
            func.sum(case((events.c.gender == "women", 1), else_=0)).label("women_count"),
        )
        .group_by(events.c.pid)
    )
    if player_id is not None:
        counts_query = counts_query.filter(events.c.pid == player_id)

    mixed_rows = [
        row
        for row in counts_query.all()
        if int(row.men_count or 0) > 0 and int(row.women_count or 0) > 0
    ]
    if not mixed_rows:
        return []

    player_ids = [int(r.pid) for r in mixed_rows]
    players = {
        p.id: p
        for p in session.query(Player).filter(Player.id.in_(player_ids)).all()
    }

    results: list[MixedPlayer] = []
    for row in mixed_rows:
        pid = int(row.pid)
        men_count = int(row.men_count or 0)
        women_count = int(row.women_count or 0)
        majority_gender = "men" if men_count >= women_count else "women"
        minority_gender = "women" if majority_gender == "men" else "men"

        minority_match_ids = [
            int(mid)
            for (mid,) in (
                session.query(Match.id)
                .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
                .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
                .filter(
                    Tournament.gender == minority_gender,
                    or_(Match.player_a_id == pid, Match.player_b_id == pid),
                )
                .all()
            )
        ]
        if not minority_match_ids:
            continue

        player = players.get(pid)
        if not player:
            continue

        results.append(
            MixedPlayer(
                player_id=pid,
                name=player.canonical_name,
                men_matches=men_count,
                women_matches=women_count,
                majority_gender=majority_gender,
                minority_gender=minority_gender,
                minority_match_ids=minority_match_ids,
            )
        )
    return sorted(results, key=lambda x: len(x.minority_match_ids), reverse=True)


def _create_split_player(session, source_player: Player, minority_gender: str) -> Player:
    source_norm = normalize_name(source_player.canonical_name)
    source_parts = source_norm.split()
    initial = source_parts[0][0] if source_parts and source_parts[0] else "x"
    last_name = extract_last_name(source_norm) or (source_parts[-1] if source_parts else "unknown")
    canonical_name = f"{initial.upper()}. {' '.join(part.capitalize() for part in last_name.split())}"

    new_player = Player(
        canonical_name=canonical_name,
        nationality_ioc=source_player.nationality_ioc,
        birth_date=source_player.birth_date,
        turned_pro_year=source_player.turned_pro_year,
        hand=source_player.hand,
        backhand=source_player.backhand,
        height_cm=source_player.height_cm,
    )
    session.add(new_player)
    session.flush()

    alias_value = normalize_name(canonical_name)
    if alias_value:
        # uq_player_alias_source is global on (alias, source), so source must
        # be unique per split player to avoid collisions on common names.
        alias_source = f"split_{minority_gender}_{new_player.id}"[:50]
        session.add(
            PlayerAlias(
                player_id=new_player.id,
                alias=alias_value,
                source=alias_source,
            )
        )
    return new_player


def _apply_split(session, mixed: MixedPlayer) -> tuple[int, int]:
    source_player = session.get(Player, mixed.player_id)
    if not source_player:
        raise ValueError(f"Player {mixed.player_id} not found")

    new_player = _create_split_player(session, source_player, mixed.minority_gender)

    session.query(Match).filter(
        Match.id.in_(mixed.minority_match_ids),
        Match.player_a_id == mixed.player_id,
    ).update({Match.player_a_id: new_player.id}, synchronize_session=False)
    session.query(Match).filter(
        Match.id.in_(mixed.minority_match_ids),
        Match.player_b_id == mixed.player_id,
    ).update({Match.player_b_id: new_player.id}, synchronize_session=False)
    session.query(Match).filter(
        Match.id.in_(mixed.minority_match_ids),
        Match.winner_id == mixed.player_id,
    ).update({Match.winner_id: new_player.id}, synchronize_session=False)

    # Clear inline ELO state for both IDs. Rebuild is recommended after split.
    session.query(PlayerEloState).filter(
        PlayerEloState.player_id.in_([mixed.player_id, new_player.id])
    ).delete(synchronize_session=False)
    session.query(Match).filter(
        or_(
            Match.player_a_id.in_([mixed.player_id, new_player.id]),
            Match.player_b_id.in_([mixed.player_id, new_player.id]),
            Match.id.in_(mixed.minority_match_ids),
        )
    ).update({Match.elo_needs_recompute: True}, synchronize_session=False)

    return mixed.player_id, new_player.id


def main() -> int:
    parser = argparse.ArgumentParser(description="Split mixed-gender players into separate records")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes only")
    parser.add_argument("--execute", action="store_true", help="Apply the split")
    parser.add_argument("--player-id", type=int, default=None, help="Only process one player id")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of players processed")
    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        parser.error("Choose --dry-run or --execute")

    with get_session() as session:
        mixed_players = _find_mixed_players(session, player_id=args.player_id)
        if args.limit > 0:
            mixed_players = mixed_players[: args.limit]

        if not mixed_players:
            print("No mixed-gender players found.")
            return 0

        print(f"Found {len(mixed_players)} mixed-gender players.")
        for item in mixed_players:
            print(
                f"- id={item.player_id} name='{item.name}' men={item.men_matches} women={item.women_matches} "
                f"split={item.minority_gender} matches_to_move={len(item.minority_match_ids)}"
            )

        if args.dry_run:
            print("\nDry run only. No changes applied.")
            return 0

        created = 0
        for item in mixed_players:
            old_id, new_id = _apply_split(session, item)
            created += 1
            print(
                f"Split player {old_id} -> new player {new_id}; moved {len(item.minority_match_ids)} "
                f"{item.minority_gender} matches."
            )

        session.flush()
        print(
            f"\nDone. Created {created} split players. "
            "Run ELO rebuild next: python scripts/update_elo.py --rebuild"
        )
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
