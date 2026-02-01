#!/usr/bin/env python3
"""
WTA Scraper Test Script.

Tests the WTA scores-page scraper with detailed logging.
NO database writes — just prints parsed data for verification.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate

    # Test tournament list for 2024
    python scripts/test_wta_scrape.py --tournaments 2024

    # Test match scraping for Australian Open 2024
    python scripts/test_wta_scrape.py --matches 901 australian-open 2024

    # Both
    python scripts/test_wta_scrape.py --tournaments 2024 --matches 901 australian-open 2024
"""

import argparse
import asyncio
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.scrape.wta import WTAScraper
from teelo.scrape.parsers.score import parse_score, ScoreParseError


async def test_tournament_list(year: int):
    """Test tournament list scraping and print results."""
    print("=" * 70)
    print(f"TEST: WTA Tournament List for {year}")
    print("=" * 70)

    async with WTAScraper(headless=False) as scraper:
        tournaments = await scraper.get_tournament_list(year)

    print(f"\nFound {len(tournaments)} tournaments:\n")
    print(f"{'#':<6} {'Slug':<25} {'Name':<45} {'Level':<15} {'Surface':<10} {'Date':<12}")
    print("-" * 120)

    for t in tournaments:
        print(f"{t['number']:<6} {t['slug']:<25} {t['name'][:44]:<45} {t['level']:<15} {t['surface']:<10} {t.get('start_date', 'N/A'):<12}")

    # Summary by level
    print(f"\nSummary by level:")
    levels = Counter(t["level"] for t in tournaments)
    for level, count in levels.most_common():
        print(f"  {level}: {count}")


async def test_match_scraping(tournament_number: str, tournament_slug: str, year: int):
    """Test match scraping for a single tournament and print results."""
    print("=" * 70)
    print(f"TEST: WTA Match Scraping - {tournament_slug} {year} (#{tournament_number})")
    print("=" * 70)

    matches = []
    score_errors = []

    async with WTAScraper(headless=False) as scraper:
        async for match in scraper.scrape_tournament_results(
            tournament_slug, year, tournament_number=tournament_number
        ):
            matches.append(match)

    print(f"\nScraped {len(matches)} matches:\n")

    # Group by round for display
    by_round = defaultdict(list)
    for m in matches:
        by_round[m.round].append(m)

    round_order = ["Q", "Q1", "Q2", "Q3", "R128", "R64", "R32", "R16", "QF", "SF", "F"]
    for round_code in round_order:
        round_matches = by_round.get(round_code, [])
        if not round_matches:
            continue

        print(f"\n  --- {round_code} ({len(round_matches)} matches) ---")
        for m in round_matches[:5]:  # Show first 5 per round
            score_ok = ""
            try:
                parsed = parse_score(m.score_raw)
                score_ok = "OK"
            except ScoreParseError as e:
                score_ok = f"PARSE_ERR: {e}"
                score_errors.append((m.score_raw, str(e)))

            seed_a = f"({m.player_a_seed})" if m.player_a_seed else ""
            seed_b = f"({m.player_b_seed})" if m.player_b_seed else ""
            winner = "A" if m.winner_name == m.player_a_name else "B"

            print(f"    {m.player_a_name}{seed_a} vs {m.player_b_name}{seed_b}")
            print(f"      Score: {m.score_raw}  [{score_ok}]  Winner: {winner}  Date: {m.match_date}  Status: {m.status}")

        if len(round_matches) > 5:
            print(f"    ... and {len(round_matches) - 5} more")

    # Summary
    print(f"\n{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"  Total matches: {len(matches)}")
    print(f"  By round: {dict((k, len(by_round[k])) for k in round_order if k in by_round)}")

    # Qualifying check
    qualifying = [m for m in matches if m.round.startswith("Q")]
    print(f"  Qualifying matches: {len(qualifying)}")

    # Date check
    dates_populated = sum(1 for m in matches if m.match_date)
    print(f"  Matches with date: {dates_populated}/{len(matches)}")

    # Score parse errors
    print(f"  Score parse errors: {len(score_errors)}")
    for raw, err in score_errors[:5]:
        print(f"    '{raw}': {err}")

    # Status breakdown
    status_counts = Counter(m.status for m in matches)
    print(f"  Status: {dict(status_counts)}")

    # Duplicates check
    ext_ids = [m.external_id for m in matches]
    dups = len(ext_ids) - len(set(ext_ids))
    print(f"  Duplicate external_ids: {dups}")

    # Final
    finals = by_round.get("F", [])
    if finals:
        f = finals[0]
        print(f"\n  Final: {f.player_a_name} vs {f.player_b_name} -> {f.score_raw}")
        print(f"  Winner: {f.winner_name}  Date: {f.match_date}")


async def main():
    parser = argparse.ArgumentParser(description="Test WTA scraper")
    parser.add_argument("--tournaments", type=int, help="Test tournament list for given year")
    parser.add_argument("--matches", nargs=3, metavar=("NUMBER", "SLUG", "YEAR"),
                       help="Test match scraping: tournament_number slug year")
    args = parser.parse_args()

    if not args.tournaments and not args.matches:
        # Default: test match scraping with AO 2024
        await test_match_scraping("901", "australian-open", 2024)
    else:
        if args.tournaments:
            await test_tournament_list(args.tournaments)
        if args.matches:
            await test_match_scraping(args.matches[0], args.matches[1], int(args.matches[2]))


if __name__ == "__main__":
    asyncio.run(main())
