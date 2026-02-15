#!/usr/bin/env python3
"""
Find and optionally merge likely duplicate players.

Usage:
  python scripts/find_and_merge_duplicate_players.py
  python scripts/find_and_merge_duplicate_players.py --execute
"""

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import case, func, text
from sqlalchemy.orm import selectinload

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db.models import Match, Player, PlayerEloState, PlayerReviewQueue, UpdateLog
from teelo.db.session import get_session
from teelo.players.aliases import (
    compare_names,
    extract_last_name,
    is_abbreviated_name,
    normalize_name,
)


@dataclass
class DuplicateCandidate:
    player_a_id: int
    player_a_name: str
    player_b_id: int
    player_b_name: str
    score: float
    match_alias_a: str
    match_alias_b: str
    matches_a: int
    matches_b: int
    ext_ids_a: int
    ext_ids_b: int


def _external_id_count(player: Player) -> int:
    return int(bool(player.atp_id)) + int(bool(player.wta_id)) + int(bool(player.itf_id))


def _player_name_variants(player: Player) -> list[str]:
    variants = {normalize_name(player.canonical_name)}
    for alias in player.aliases:
        variants.add(alias.alias)
    variants.discard("")
    return sorted(variants)


def _player_match_counts(session) -> dict[int, int]:
    counts: dict[int, int] = {}

    for player_id, cnt in session.query(Match.player_a_id, func.count(Match.id)).filter(
        Match.player_a_id.isnot(None)
    ).group_by(Match.player_a_id):
        counts[player_id] = counts.get(player_id, 0) + int(cnt)

    for player_id, cnt in session.query(Match.player_b_id, func.count(Match.id)).filter(
        Match.player_b_id.isnot(None)
    ).group_by(Match.player_b_id):
        counts[player_id] = counts.get(player_id, 0) + int(cnt)

    for player_id, cnt in session.query(Match.winner_id, func.count(Match.id)).filter(
        Match.winner_id.isnot(None)
    ).group_by(Match.winner_id):
        counts[player_id] = counts.get(player_id, 0) + int(cnt)

    return counts


def _player_majority_gender(session) -> dict[int, str]:
    events_a = (
        session.query(
            Match.player_a_id.label("pid"),
            Tournament.gender.label("gender"),
        )
        .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .filter(Tournament.gender.in_(("men", "women")))
    )
    events_b = (
        session.query(
            Match.player_b_id.label("pid"),
            Tournament.gender.label("gender"),
        )
        .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .filter(Tournament.gender.in_(("men", "women")))
    )
    events = events_a.union_all(events_b).subquery()

    rows = (
        session.query(
            events.c.pid,
            func.sum(case((events.c.gender == "men", 1), else_=0)).label("men_count"),
            func.sum(case((events.c.gender == "women", 1), else_=0)).label("women_count"),
        )
        .group_by(events.c.pid)
        .all()
    )
    result: dict[int, str] = {}
    for pid, men_count, women_count in rows:
        men = int(men_count or 0)
        women = int(women_count or 0)
        if men > women:
            result[int(pid)] = "men"
        elif women > men:
            result[int(pid)] = "women"
    return result


def _pick_keep_id(candidate: DuplicateCandidate) -> tuple[int, int]:
    a_strength = (candidate.matches_a, candidate.ext_ids_a, -candidate.player_a_id)
    b_strength = (candidate.matches_b, candidate.ext_ids_b, -candidate.player_b_id)
    if a_strength >= b_strength:
        return candidate.player_a_id, candidate.player_b_id
    return candidate.player_b_id, candidate.player_a_id


def find_candidates(session, report_threshold: float = 0.95) -> list[DuplicateCandidate]:
    players = session.query(Player).options(selectinload(Player.aliases)).all()
    match_counts = _player_match_counts(session)
    gender_by_player = _player_majority_gender(session)

    by_last_name: dict[str, list[Player]] = {}
    for player in players:
        last_name = extract_last_name(player.canonical_name)
        if not last_name:
            continue
        by_last_name.setdefault(last_name, []).append(player)

    candidates: list[DuplicateCandidate] = []
    for group in by_last_name.values():
        if len(group) < 2:
            continue

        for i in range(len(group)):
            p1 = group[i]
            names_1 = _player_name_variants(p1)
            for j in range(i + 1, len(group)):
                p2 = group[j]
                g1 = gender_by_player.get(p1.id)
                g2 = gender_by_player.get(p2.id)
                if g1 and g2 and g1 != g2:
                    # Never auto-merge across opposite inferred genders.
                    continue
                names_2 = _player_name_variants(p2)

                best_score = 0.0
                best_alias_a = names_1[0]
                best_alias_b = names_2[0]
                for n1 in names_1:
                    for n2 in names_2:
                        score = compare_names(n1, n2)
                        if score > best_score:
                            best_score = score
                            best_alias_a = n1
                            best_alias_b = n2

                if best_score < report_threshold:
                    continue

                candidates.append(
                    DuplicateCandidate(
                        player_a_id=p1.id,
                        player_a_name=p1.canonical_name,
                        player_b_id=p2.id,
                        player_b_name=p2.canonical_name,
                        score=best_score,
                        match_alias_a=best_alias_a,
                        match_alias_b=best_alias_b,
                        matches_a=match_counts.get(p1.id, 0),
                        matches_b=match_counts.get(p2.id, 0),
                        ext_ids_a=_external_id_count(p1),
                        ext_ids_b=_external_id_count(p2),
                    )
                )

    candidates.sort(key=lambda c: c.score, reverse=True)
    return candidates


def _resolve_merge_map(pairs: list[tuple[int, int]]) -> dict[int, int]:
    """
    Resolve merge chains into a direct map: merge_id -> final_keep_id.

    Example:
      (3476 -> 11075), (11075 -> 4711) becomes
      {3476: 4711, 11075: 4711}
    """
    redirect: dict[int, int] = {}

    def resolve(player_id: int) -> int:
        current = player_id
        while current in redirect:
            current = redirect[current]
        return current

    for keep_id, merge_id in pairs:
        final_keep = resolve(keep_id)
        final_merge = resolve(merge_id)
        if final_keep == final_merge:
            continue
        redirect[final_merge] = final_keep

    return {merge_id: resolve(merge_id) for merge_id in redirect}


def _apply_bulk_merge_chunk(
    session,
    chunk: list[tuple[int, int, float]],
) -> list[tuple[int, int]]:
    """
    Apply a chunk of merges with set-based SQL operations.

    Returns:
        List of (merge_id, keep_id) that were applied.
    """
    raw_pairs = [(keep_id, merge_id) for keep_id, merge_id, _ in chunk]
    merge_map = _resolve_merge_map(raw_pairs)
    if not merge_map:
        return []

    merge_ids = list(merge_map.keys())
    keep_ids = sorted(set(merge_map.values()))
    all_ids = sorted(set(merge_ids + keep_ids))

    players = {
        p.id: p
        for p in session.query(Player).filter(Player.id.in_(all_ids)).all()
    }

    def _maybe_promote_name(keep_player: Player, merge_player: Player) -> None:
        keep_norm = normalize_name(keep_player.canonical_name)
        merge_norm = normalize_name(merge_player.canonical_name)
        if not keep_norm or not merge_norm:
            return
        if not is_abbreviated_name(keep_norm) or is_abbreviated_name(merge_norm):
            return
        if extract_last_name(keep_norm) != extract_last_name(merge_norm):
            return
        keep_initial = keep_norm.split()[0].rstrip(".")
        merge_first = merge_norm.split()[0].rstrip(".")
        if not merge_first.startswith(keep_initial):
            return
        if compare_names(keep_norm, merge_norm) >= 0.90:
            keep_player.canonical_name = merge_player.canonical_name

    # External ID transfer plan: clear on merge rows first, then assign to keep rows.
    transfers: list[tuple[Player, str, str]] = []
    for merge_id, keep_id in merge_map.items():
        merge_player = players.get(merge_id)
        keep_player = players.get(keep_id)
        if not merge_player or not keep_player:
            continue
        _maybe_promote_name(keep_player, merge_player)
        for field_name in ("atp_id", "wta_id", "itf_id"):
            merge_value = getattr(merge_player, field_name)
            keep_value = getattr(keep_player, field_name)
            if not merge_value or keep_value:
                continue
            setattr(merge_player, field_name, None)
            transfers.append((keep_player, field_name, merge_value))

    if transfers:
        session.flush()

        for field_name in ("atp_id", "wta_id", "itf_id"):
            field_transfers = [(kp, val) for kp, f, val in transfers if f == field_name]
            if not field_transfers:
                continue

            values = sorted({value for _, value in field_transfers})
            conflicts = set(
                value for (value,) in session.query(getattr(Player, field_name)).filter(
                    Player.id.notin_(all_ids),
                    getattr(Player, field_name).in_(values),
                ).all()
            )
            for keep_player, value in field_transfers:
                if value in conflicts:
                    continue
                if getattr(keep_player, field_name) is None:
                    setattr(keep_player, field_name, value)

    # Preserve aliases before deleting merged players.
    # Important: inserting from merge rows while those rows still exist can
    # conflict on uq_player_alias_source and silently skip all rows. Instead,
    # delete only true duplicates against keep rows, then move remaining rows.
    values_sql_parts: list[str] = []
    values_params: dict[str, int] = {}
    for idx, (merge_id, keep_id) in enumerate(merge_map.items()):
        values_sql_parts.append(f"(:m{idx}, :k{idx})")
        values_params[f"m{idx}"] = merge_id
        values_params[f"k{idx}"] = keep_id
    values_sql = ", ".join(values_sql_parts)

    # 1) Drop merge aliases that are exact duplicates of an existing keep alias/source.
    session.execute(
        text(
            f"""
            DELETE FROM player_aliases pa_merge
            USING (VALUES {values_sql}) AS map(merge_id, keep_id), player_aliases pa_keep
            WHERE pa_merge.player_id = map.merge_id
              AND pa_keep.player_id = map.keep_id
              AND pa_keep.alias = pa_merge.alias
              AND pa_keep.source = pa_merge.source
            """
        ),
        values_params,
    )

    # 2) Move remaining merge aliases onto keep players.
    session.execute(
        text(
            f"""
            UPDATE player_aliases pa
            SET player_id = map.keep_id
            FROM (VALUES {values_sql}) AS map(merge_id, keep_id)
            WHERE pa.player_id = map.merge_id
            """
        ),
        values_params,
    )

    case_map = case(merge_map, value=Match.player_a_id, else_=Match.player_a_id)
    session.query(Match).filter(Match.player_a_id.in_(merge_ids)).update(
        {Match.player_a_id: case_map},
        synchronize_session=False,
    )
    case_map = case(merge_map, value=Match.player_b_id, else_=Match.player_b_id)
    session.query(Match).filter(Match.player_b_id.in_(merge_ids)).update(
        {Match.player_b_id: case_map},
        synchronize_session=False,
    )
    case_map = case(merge_map, value=Match.winner_id, else_=Match.winner_id)
    session.query(Match).filter(Match.winner_id.in_(merge_ids)).update(
        {Match.winner_id: case_map},
        synchronize_session=False,
    )

    session.query(Match).filter(
        Match.player_a_id.in_(keep_ids) | Match.player_b_id.in_(keep_ids)
    ).update({"elo_needs_recompute": True}, synchronize_session=False)

    case_map = case(
        merge_map,
        value=PlayerReviewQueue.suggested_player_1_id,
        else_=PlayerReviewQueue.suggested_player_1_id,
    )
    session.query(PlayerReviewQueue).filter(
        PlayerReviewQueue.suggested_player_1_id.in_(merge_ids)
    ).update({PlayerReviewQueue.suggested_player_1_id: case_map}, synchronize_session=False)
    case_map = case(
        merge_map,
        value=PlayerReviewQueue.suggested_player_2_id,
        else_=PlayerReviewQueue.suggested_player_2_id,
    )
    session.query(PlayerReviewQueue).filter(
        PlayerReviewQueue.suggested_player_2_id.in_(merge_ids)
    ).update({PlayerReviewQueue.suggested_player_2_id: case_map}, synchronize_session=False)
    case_map = case(
        merge_map,
        value=PlayerReviewQueue.suggested_player_3_id,
        else_=PlayerReviewQueue.suggested_player_3_id,
    )
    session.query(PlayerReviewQueue).filter(
        PlayerReviewQueue.suggested_player_3_id.in_(merge_ids)
    ).update({PlayerReviewQueue.suggested_player_3_id: case_map}, synchronize_session=False)
    case_map = case(
        merge_map,
        value=PlayerReviewQueue.resolved_player_id,
        else_=PlayerReviewQueue.resolved_player_id,
    )
    session.query(PlayerReviewQueue).filter(
        PlayerReviewQueue.resolved_player_id.in_(merge_ids)
    ).update({PlayerReviewQueue.resolved_player_id: case_map}, synchronize_session=False)

    session.query(PlayerEloState).filter(
        PlayerEloState.player_id.in_(merge_ids)
    ).delete(synchronize_session=False)

    for merge_id, keep_id in merge_map.items():
        keep_player = players.get(keep_id)
        merge_player = players.get(merge_id)
        session.add(
            UpdateLog(
                update_type="player_merge",
                details={
                    "keep_id": keep_id,
                    "merge_id": merge_id,
                    "keep_name": keep_player.canonical_name if keep_player else None,
                    "merge_name": merge_player.canonical_name if merge_player else None,
                },
                success=True,
            )
        )

    session.query(Player).filter(Player.id.in_(merge_ids)).delete(synchronize_session=False)
    return [(merge_id, keep_id) for merge_id, keep_id in merge_map.items()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Find and merge duplicate players")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Merge high-confidence duplicate pairs (score >= 0.95)",
    )
    args = parser.parse_args()

    with get_session() as session:
        candidates = find_candidates(session, report_threshold=0.95)
        if not candidates:
            print("No duplicate candidates found (score >= 0.95).")
            return 0

        print(f"Found {len(candidates)} duplicate candidate pairs (score >= 0.95):")
        for candidate in candidates:
            print(
                f"- {candidate.player_a_id}:{candidate.player_a_name} <-> "
                f"{candidate.player_b_id}:{candidate.player_b_name} "
                f"[score={candidate.score:.3f}] "
                f"aliases=('{candidate.match_alias_a}' vs '{candidate.match_alias_b}') "
                f"matches=({candidate.matches_a},{candidate.matches_b}) "
                f"external_ids=({candidate.ext_ids_a},{candidate.ext_ids_b})"
            )

        if not args.execute:
            print("\nDry run complete. Re-run with --execute to merge score >= 0.95 pairs.")
            return 0

        merges = [c for c in candidates if c.score >= 0.95]
        if not merges:
            print("\nNo high-confidence pairs to merge (score >= 0.95).")
            return 0

        planned_merges: list[tuple[int, int, float]] = []
        merged_ids: set[int] = set()
        for candidate in merges:
            if candidate.player_a_id in merged_ids or candidate.player_b_id in merged_ids:
                continue
            keep_id, merge_id = _pick_keep_id(candidate)
            planned_merges.append((keep_id, merge_id, candidate.score))
            merged_ids.add(merge_id)

        chunk_size = 200
        merge_count = 0
        chunk_count = 0
        score_by_merge: dict[int, float] = {
            merge_id: score for _, merge_id, score in planned_merges
        }

        try:
            for i in range(0, len(planned_merges), chunk_size):
                chunk = planned_merges[i:i + chunk_size]
                applied = _apply_bulk_merge_chunk(session, chunk)
                session.commit()
                chunk_count += 1
                merge_count += len(applied)
                for merge_id, keep_id in applied:
                    score = score_by_merge.get(merge_id, 1.0)
                    print(f"Merged {merge_id} into {keep_id} (score={score:.3f})")
                print(f"Committed chunk {chunk_count} ({len(applied)} merges).")
        except Exception:
            session.rollback()
            raise

        print(f"\nCompleted {merge_count} merges across {chunk_count} chunks.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
