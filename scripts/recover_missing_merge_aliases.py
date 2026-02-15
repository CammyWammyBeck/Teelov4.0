#!/usr/bin/env python3
"""
Recover potentially lost aliases from historical player merges.

Looks at update_log records with update_type='player_merge', and for each merge:
- If merge_name looks usable and the kept player no longer has that alias,
  propose adding it as source='merge_recovery'.

Usage:
  py scripts/recover_missing_merge_aliases.py --dry-run
  py scripts/recover_missing_merge_aliases.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db.models import Player, PlayerAlias, UpdateLog
from teelo.db.session import get_session
from teelo.players.aliases import compare_names, extract_last_name, is_abbreviated_name, normalize_name


def _extract_recovery_candidates(session) -> list[dict]:
    print("Loading merge audit logs...", flush=True)
    logs = (
        session.query(UpdateLog)
        .filter(UpdateLog.update_type == "player_merge")
        .order_by(UpdateLog.created_at.asc(), UpdateLog.id.asc())
        .all()
    )
    print(f"Loaded {len(logs)} merge logs.", flush=True)

    raw_proposals: list[dict] = []
    keep_ids: set[int] = set()
    alias_values: set[str] = set()
    for log in logs:
        details = log.details or {}
        keep_id = details.get("keep_id")
        merge_name = details.get("merge_name")
        if not keep_id or not merge_name:
            continue

        alias_value = normalize_name(str(merge_name))
        if not alias_value:
            continue

        keep_id_int = int(keep_id)
        keep_ids.add(keep_id_int)
        alias_values.add(alias_value)
        raw_proposals.append(
            {
                "keep_id": keep_id_int,
                "keep_name": None,
                "alias": alias_value,
                "log_id": log.id,
                "created_at": log.created_at,
            }
        )

    if not raw_proposals:
        return []

    print(
        f"Pre-filtered to {len(raw_proposals)} raw proposals. "
        "Resolving players/aliases in bulk...",
        flush=True,
    )
    players = {
        p.id: p.canonical_name
        for p in session.query(Player).filter(Player.id.in_(sorted(keep_ids))).all()
    }
    existing_aliases = {
        alias
        for (alias,) in session.query(PlayerAlias.alias).filter(
            PlayerAlias.alias.in_(sorted(alias_values))
        ).all()
    }

    proposals: list[dict] = []
    for item in raw_proposals:
        keep_name = players.get(item["keep_id"])
        if not keep_name:
            continue
        if item["alias"] in existing_aliases:
            continue
        keep_norm = normalize_name(keep_name)
        alias_norm = normalize_name(item["alias"])
        if not keep_norm or not alias_norm:
            continue
        # Safety guard: only recover aliases that plausibly refer to same person.
        # Prevent bad additions like "charlotte ruud" -> "casper ruud".
        if compare_names(keep_norm, alias_norm) < 0.90:
            continue
        if extract_last_name(keep_norm) != extract_last_name(alias_norm):
            continue
        if is_abbreviated_name(keep_norm) and not is_abbreviated_name(alias_norm):
            keep_initial = keep_norm.split()[0].rstrip(".")
            alias_first = alias_norm.split()[0].rstrip(".")
            if not alias_first.startswith(keep_initial):
                continue
        item["keep_name"] = keep_name
        proposals.append(item)

    # Deduplicate by (keep_id, alias)
    seen = set()
    deduped = []
    for p in proposals:
        key = (p["keep_id"], p["alias"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(p)
    return deduped


def main() -> int:
    parser = argparse.ArgumentParser(description="Recover missing aliases from merge audit logs")
    parser.add_argument("--dry-run", action="store_true", help="Show recoverable aliases only")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of aliases to apply (0 = all)",
    )
    args = parser.parse_args()

    with get_session() as session:
        candidates = _extract_recovery_candidates(session)
        total = len(candidates)
        print(f"Recoverable aliases found: {total}")
        preview = candidates if args.limit <= 0 else candidates[: args.limit]

        for item in preview[:200]:
            created = item["created_at"].strftime("%Y-%m-%d %H:%M") if item["created_at"] else "n/a"
            print(
                f"- keep_id={item['keep_id']} ({item['keep_name']}) "
                f"add_alias='{item['alias']}' from_log={item['log_id']} at {created}"
            )
        if len(preview) > 200:
            print(f"... and {len(preview) - 200} more in selected set")

        if args.dry_run:
            return 0

        apply_items = preview
        existing_aliases = {
            alias
            for (alias,) in session.query(PlayerAlias.alias).filter(
                PlayerAlias.alias.in_(sorted({i["alias"] for i in apply_items}))
            ).all()
        }
        added = 0
        for item in apply_items:
            # Avoid duplicates in this run and against already-existing aliases.
            if item["alias"] in existing_aliases:
                continue
            existing_aliases.add(item["alias"])
            session.add(
                PlayerAlias(
                    player_id=item["keep_id"],
                    alias=item["alias"],
                    source="merge_recovery",
                )
            )
            added += 1

        session.flush()
        print(f"Inserted {added} recovered aliases.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
