#!/usr/bin/env python3
"""
Repair abbreviated canonical player names (e.g. "J. Pegula").

Uses each player's aliases to find likely full-name alternatives and updates
canonical_name to a fuller display name.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy.orm import selectinload

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db.models import Player
from teelo.db.session import get_session
from teelo.players.aliases import compare_names, extract_last_name, is_abbreviated_name, normalize_name


def _display_title(name: str) -> str:
    """Convert normalized alias text into a readable display name."""
    particles = {"de", "del", "van", "von", "da", "di", "la", "le"}
    parts = name.split()
    formatted: list[str] = []
    for i, part in enumerate(parts):
        if i > 0 and part in particles:
            formatted.append(part)
        else:
            formatted.append(part.capitalize())
    return " ".join(formatted)


def _best_full_alias(player: Player) -> str | None:
    keep_norm = normalize_name(player.canonical_name)
    if not keep_norm or not is_abbreviated_name(keep_norm):
        return None

    initial = keep_norm.split()[0].rstrip(".")
    last_name = extract_last_name(keep_norm)
    if not last_name:
        return None

    candidates: list[str] = []
    for alias in player.aliases:
        alias_norm = normalize_name(alias.alias)
        if not alias_norm or is_abbreviated_name(alias_norm):
            continue
        if extract_last_name(alias_norm) != last_name:
            continue
        first = alias_norm.split()[0].rstrip(".")
        if not first.startswith(initial):
            continue
        candidates.append(alias_norm)

    if not candidates:
        return None

    # Prefer richer names: longer first token, then longer total length.
    candidates.sort(key=lambda n: (len(n.split()[0]), len(n)), reverse=True)
    return candidates[0]


def _best_cross_player_name(
    player: Player,
    all_players: list[Player],
    threshold: float,
) -> str | None:
    """
    Find a full-name replacement from other players when aliases are missing.

    Only returns when exactly one strong candidate exists, to reduce risk.
    """
    keep_norm = normalize_name(player.canonical_name)
    if not keep_norm or not is_abbreviated_name(keep_norm):
        return None

    keep_parts = keep_norm.split()
    initial = keep_parts[0].rstrip(".")
    last_name = extract_last_name(keep_norm)
    if not last_name:
        return None

    matches: list[str] = []
    for other in all_players:
        if other.id == player.id:
            continue
        other_norm = normalize_name(other.canonical_name)
        if not other_norm or is_abbreviated_name(other_norm):
            continue
        if extract_last_name(other_norm) != last_name:
            continue
        other_first = other_norm.split()[0].rstrip(".")
        if not other_first.startswith(initial):
            continue
        if compare_names(keep_norm, other_norm) >= threshold:
            matches.append(other_norm)

    if len(matches) != 1:
        return None
    return matches[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair abbreviated canonical player names")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.95,
        help="Similarity threshold for cross-player fallback (default: 0.95)",
    )
    args = parser.parse_args()

    with get_session() as session:
        players = session.query(Player).options(selectinload(Player.aliases)).all()
        changes: list[tuple[int, str, str]] = []

        for player in players:
            best = _best_full_alias(player)
            if not best:
                best = _best_cross_player_name(player, players, threshold=args.threshold)
            if not best:
                continue
            new_name = _display_title(best)
            if new_name == player.canonical_name:
                continue
            changes.append((player.id, player.canonical_name, new_name))
            if not args.dry_run:
                player.canonical_name = new_name

        if args.dry_run:
            print(f"Would update {len(changes)} players.")
        else:
            session.flush()
            print(f"Updated {len(changes)} players.")

        for pid, old, new in changes[:100]:
            print(f"- {pid}: {old} -> {new}")
        if len(changes) > 100:
            print(f"... and {len(changes) - 100} more")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
