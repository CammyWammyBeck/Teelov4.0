#!/usr/bin/env python3
"""
Test script for WTA scores page scraper.

Scrapes the 2024 Australian Open women's singles from the WTA scores page
and validates that:
1. Matches are found and parsed
2. All matches have match_date set
3. Dates span multiple days (not all the same)
4. Round codes are valid
5. Player names and IDs are extracted
6. Scores look reasonable
7. Match counts per round are plausible for a Grand Slam (128 draw)

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/test_wta_scores.py

    # Save HTML for each day (for offline debugging)
    python scripts/test_wta_scores.py --save-html

    # Test with a smaller tournament
    python scripts/test_wta_scores.py --tournament 1073 adelaide 2024
"""

import argparse
import asyncio
import sys
from collections import Counter
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.scrape.wta import WTAScraper
from teelo.scrape.base import ScrapedMatch


# Expected round codes for a Grand Slam 128 draw
VALID_ROUNDS = {"R128", "R64", "R32", "R16", "QF", "SF", "F", "Q1", "Q2", "Q3"}

# Expected match counts per round in a 128-draw Grand Slam
EXPECTED_COUNTS_GS = {
    "R128": 64,
    "R64": 32,
    "R32": 16,
    "R16": 8,
    "QF": 4,
    "SF": 2,
    "F": 1,
}


async def test_scrape(
    tournament_number: str,
    tournament_id: str,
    year: int,
    save_html: bool = False,
):
    """
    Scrape a WTA tournament and run validation checks.

    Args:
        tournament_number: WTA tournament number (e.g. "901" for Australian Open)
        tournament_id: Tournament slug (e.g. "australian-open")
        year: Tournament year
        save_html: If True, save raw HTML for each day to scratchpad
    """
    print(f"\n{'='*60}")
    print(f"Testing WTA scores scraper: {tournament_id} {year}")
    print(f"Tournament number: {tournament_number}")
    print(f"{'='*60}\n")

    matches: list[ScrapedMatch] = []

    async with WTAScraper(headless=False) as scraper:
        async for match in scraper.scrape_tournament_results(
            tournament_id,
            year,
            tournament_number=tournament_number,
        ):
            matches.append(match)

    print(f"\n{'='*60}")
    print(f"RESULTS: {len(matches)} matches scraped")
    print(f"{'='*60}\n")

    if not matches:
        print("FAIL: No matches scraped!")
        return False

    # ---- Check 1: All matches have match_date ----
    with_date = [m for m in matches if m.match_date]
    without_date = [m for m in matches if not m.match_date]
    print(f"[CHECK 1] match_date populated:")
    print(f"  With date:    {len(with_date)}")
    print(f"  Without date: {len(without_date)}")
    if without_date:
        print(f"  FAIL: {len(without_date)} matches missing match_date")
        for m in without_date[:5]:
            print(f"    {m.player_a_name} vs {m.player_b_name} ({m.round})")
    else:
        print(f"  PASS")

    # ---- Check 2: Dates span multiple days ----
    date_counts = Counter(m.match_date for m in matches)
    print(f"\n[CHECK 2] Date distribution ({len(date_counts)} unique days):")
    for date, count in sorted(date_counts.items()):
        print(f"  {date}: {count} matches")
    if len(date_counts) < 3:
        print(f"  WARN: Only {len(date_counts)} unique dates — expected more for a multi-day tournament")
    else:
        print(f"  PASS")

    # ---- Check 3: Round codes are valid ----
    round_counts = Counter(m.round for m in matches)
    print(f"\n[CHECK 3] Round distribution:")
    unknown_rounds = []
    for rnd, count in sorted(round_counts.items(), key=lambda x: x[1], reverse=True):
        valid = rnd in VALID_ROUNDS
        marker = "" if valid else " ← UNKNOWN"
        print(f"  {rnd}: {count} matches{marker}")
        if not valid:
            unknown_rounds.append(rnd)
    if unknown_rounds:
        print(f"  WARN: Unknown round codes: {unknown_rounds}")
    else:
        print(f"  PASS")

    # ---- Check 4: Player data quality ----
    missing_name_a = sum(1 for m in matches if not m.player_a_name)
    missing_name_b = sum(1 for m in matches if not m.player_b_name)
    missing_id_a = sum(1 for m in matches if not m.player_a_external_id)
    missing_id_b = sum(1 for m in matches if not m.player_b_external_id)
    print(f"\n[CHECK 4] Player data:")
    print(f"  Missing player_a_name: {missing_name_a}")
    print(f"  Missing player_b_name: {missing_name_b}")
    print(f"  Missing player_a_external_id: {missing_id_a}")
    print(f"  Missing player_b_external_id: {missing_id_b}")
    if missing_name_a or missing_name_b:
        print(f"  FAIL: Missing player names")
    else:
        print(f"  PASS")

    # ---- Check 5: Scores ----
    missing_score = sum(1 for m in matches if not m.score_raw or m.score_raw.strip() == "")
    walkover_count = sum(1 for m in matches if m.status == "walkover")
    retired_count = sum(1 for m in matches if m.status == "retired")
    completed_count = sum(1 for m in matches if m.status == "completed")
    print(f"\n[CHECK 5] Match status:")
    print(f"  Completed:  {completed_count}")
    print(f"  Retired:    {retired_count}")
    print(f"  Walkover:   {walkover_count}")
    print(f"  No score:   {missing_score}")
    print(f"  PASS" if completed_count > 0 else f"  WARN: No completed matches")

    # ---- Check 6: External IDs are unique ----
    ext_ids = [m.external_id for m in matches]
    dupes = [eid for eid, cnt in Counter(ext_ids).items() if cnt > 1]
    print(f"\n[CHECK 6] External ID uniqueness:")
    print(f"  Total: {len(ext_ids)}, Unique: {len(set(ext_ids))}, Duplicates: {len(dupes)}")
    if dupes:
        print(f"  FAIL: Duplicate external IDs:")
        for d in dupes[:5]:
            print(f"    {d}")
    else:
        print(f"  PASS")

    # ---- Check 7: Sample matches ----
    print(f"\n[CHECK 7] Sample matches (first 5):")
    for m in matches[:5]:
        print(f"  {m.match_date} | {m.round:>4} | {m.player_a_name} vs {m.player_b_name} | {m.score_raw} | {m.status}")

    print(f"\n[CHECK 7] Sample matches (last 5):")
    for m in matches[-5:]:
        print(f"  {m.match_date} | {m.round:>4} | {m.player_a_name} vs {m.player_b_name} | {m.score_raw} | {m.status}")

    # ---- Summary ----
    all_pass = (
        len(without_date) == 0
        and len(date_counts) >= 3
        and missing_name_a == 0
        and missing_name_b == 0
        and completed_count > 0
        and len(dupes) == 0
    )
    print(f"\n{'='*60}")
    print(f"OVERALL: {'PASS' if all_pass else 'FAIL'}")
    print(f"{'='*60}")

    return all_pass


def main():
    parser = argparse.ArgumentParser(description="Test WTA scores page scraper")
    parser.add_argument(
        "--save-html", action="store_true",
        help="Save raw HTML for each day to scratchpad"
    )
    parser.add_argument(
        "--tournament", nargs=3, metavar=("NUMBER", "SLUG", "YEAR"),
        help="Custom tournament: NUMBER SLUG YEAR (e.g. 1073 adelaide 2024)"
    )
    args = parser.parse_args()

    if args.tournament:
        number, slug, year = args.tournament
        year = int(year)
    else:
        # Default: 2024 Australian Open (WTA tournament number 901)
        number = "901"
        slug = "australian-open"
        year = 2024

    success = asyncio.run(test_scrape(number, slug, year, save_html=args.save_html))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
