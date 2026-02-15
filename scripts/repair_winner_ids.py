"""
Repair incorrect winner_id values caused by the hardcoded player_a_id bug.

The bug: results_ingestion.py and scrape/pipeline.py always set
winner_id = player_a_id, ignoring which player actually won.

Fix approach:
1. Load all match rows (id, player_a_id, player_b_id, winner_id, score)
   in a single query — lightweight column tuples, not full ORM objects.
2. Parse each score locally to determine the actual winner (A or B).
3. Collect mismatches.
4. Push a single batched UPDATE for all fixes.

Usage:
    # Dry run (no changes):
    python scripts/repair_winner_ids.py

    # Apply fixes:
    python scripts/repair_winner_ids.py --apply
"""

import argparse
import sys

sys.path.insert(0, "src")

from sqlalchemy import update

from teelo.db import get_session
from teelo.db.models import Match, Player
from teelo.scrape.parsers.score import ScoreParseError, parse_score


def main():
    parser = argparse.ArgumentParser(description="Repair incorrect winner_id values")
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually apply fixes (default is dry-run)",
    )
    args = parser.parse_args()
    dry_run = not args.apply

    if dry_run:
        print("=== DRY RUN (pass --apply to commit changes) ===\n")
    else:
        print("=== APPLYING FIXES ===\n")

    with get_session() as session:
        # ---------------------------------------------------------------
        # 1. Single lightweight query: only the columns we need
        # ---------------------------------------------------------------
        print("Loading matches...")
        rows = (
            session.query(
                Match.id,
                Match.player_a_id,
                Match.player_b_id,
                Match.winner_id,
                Match.score,
                Match.round,
            )
            .filter(
                Match.winner_id.isnot(None),
                Match.score.isnot(None),
                Match.score != "",
                Match.score != "W/O",
                Match.player_a_id.isnot(None),
                Match.player_b_id.isnot(None),
            )
            .all()
        )
        print(f"Loaded {len(rows)} matches\n")

        # ---------------------------------------------------------------
        # 2. Process locally — no DB calls
        # ---------------------------------------------------------------
        fixes: list[dict] = []  # [{id, winner_id}]
        correct = 0
        skipped_parse = 0
        skipped_no_winner = 0
        affected_player_ids: set[int] = set()

        for match_id, pa_id, pb_id, winner_id, score, round_code in rows:
            try:
                parsed = parse_score(score)
            except ScoreParseError:
                skipped_parse += 1
                continue

            if not parsed.winner:
                skipped_no_winner += 1
                continue

            correct_winner_id = pa_id if parsed.winner == "A" else pb_id

            if winner_id == correct_winner_id:
                correct += 1
                continue

            fixes.append({"id": match_id, "winner_id": correct_winner_id})
            affected_player_ids.update([pa_id, pb_id, winner_id, correct_winner_id])

        # ---------------------------------------------------------------
        # 3. Display sample fixes (load names only for affected players)
        # ---------------------------------------------------------------
        if fixes and dry_run:
            name_rows = (
                session.query(Player.id, Player.canonical_name)
                .filter(Player.id.in_(affected_player_ids))
                .all()
            )
            names = {pid: name for pid, name in name_rows}

            # Re-scan the first 50 fixes to display them
            fix_ids = {f["id"]: f["winner_id"] for f in fixes}
            display_rows = (
                session.query(
                    Match.id, Match.player_a_id, Match.player_b_id,
                    Match.winner_id, Match.score, Match.round,
                )
                .filter(Match.id.in_(list(fix_ids.keys())[:50]))
                .all()
            )

            for mid, pa_id, pb_id, wrong_wid, score, rnd in display_rows:
                right_wid = fix_ids[mid]
                print(
                    f"  FIX: {names.get(pa_id, pa_id)} vs {names.get(pb_id, pb_id)} | "
                    f"Round {rnd} | Score: {score} | "
                    f"{names.get(wrong_wid, wrong_wid)} -> {names.get(right_wid, right_wid)}"
                )

            if len(fixes) > 50:
                print(f"  ... and {len(fixes) - 50} more")

        # ---------------------------------------------------------------
        # 4. Summary + single batched update
        # ---------------------------------------------------------------
        total = len(rows)
        print(f"\n{'=' * 60}")
        print(f"Summary:")
        print(f"  Total matches checked:        {total}")
        print(f"  Already correct:              {correct}")
        print(f"  Wrong winner (to fix):        {len(fixes)}")
        print(f"  Skipped (score parse error):  {skipped_parse}")
        print(f"  Skipped (no winner in score): {skipped_no_winner}")

        if fixes and not dry_run:
            print(f"\nApplying {len(fixes)} fixes in a single batch update...")
            session.execute(
                update(Match),
                [{"id": f["id"], "winner_id": f["winner_id"]} for f in fixes],
            )
            session.commit()
            print("Done!")
        elif fixes:
            print(f"\n{len(fixes)} matches need fixing. Run with --apply to commit.")
        else:
            print("\nNo fixes needed.")


if __name__ == "__main__":
    main()
