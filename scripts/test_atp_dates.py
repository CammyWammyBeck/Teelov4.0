#!/usr/bin/env python3
"""
Test ATP scraper date extraction using saved HTML.

Parses the saved 2026 Brisbane HTML file offline (no browser needed)
and validates match_date extraction.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/test_atp_dates.py
"""

import asyncio
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.scrape.atp import ATPScraper
from teelo.scrape.base import ScrapedMatch

# Saved HTML from the planning session
BRISBANE_HTML = Path(
    "/tmp/claude-1000/-home-cammybeck-Documents-programming-Teelov4-0/"
    "9249a961-dd1e-407c-8d3e-06dc77abb46b/scratchpad/atp_2026_brisbane.html"
)


async def test_brisbane():
    """Test ATP date extraction against saved 2026 Brisbane HTML."""
    print("=" * 60)
    print("Testing ATP date extraction: 2026 Brisbane (offline)")
    print("=" * 60)

    if not BRISBANE_HTML.exists():
        print(f"SKIP: Saved HTML not found at {BRISBANE_HTML}")
        print("This test requires the saved HTML from the planning session.")
        return True  # Not a failure, just can't run

    html = BRISBANE_HTML.read_text()
    print(f"Loaded {len(html)} chars of HTML\n")

    # Create scraper without browser (just need the parsing methods)
    scraper = ATPScraper.__new__(ATPScraper)
    tournament_info = {
        "id": "brisbane",
        "name": "Brisbane International",
        "year": 2026,
        "level": "ATP 250",
        "surface": "Hard",
        "location": "Brisbane, Australia",
    }

    matches: list[ScrapedMatch] = []
    async for m in scraper._parse_results_page(html, tournament_info, "main"):
        matches.append(m)

    print(f"Parsed {len(matches)} matches\n")

    # Check 1: Expected match count (ATP 250 = 28 draw + qualifying)
    print(f"[CHECK 1] Match count: {len(matches)}")
    if len(matches) < 20:
        print(f"  FAIL: Expected ~30-50 matches for an ATP 250, got {len(matches)}")
    else:
        print(f"  PASS")

    # Check 2: All matches have dates
    with_date = sum(1 for m in matches if m.match_date)
    without_date = sum(1 for m in matches if not m.match_date)
    print(f"\n[CHECK 2] match_date populated: {with_date}/{len(matches)}")
    if without_date:
        print(f"  FAIL: {without_date} matches missing match_date")
    else:
        print(f"  PASS")

    # Check 3: Date distribution
    date_counts = Counter(m.match_date for m in matches)
    print(f"\n[CHECK 3] Date distribution ({len(date_counts)} days):")
    for date, count in sorted(date_counts.items()):
        print(f"  {date}: {count} matches")

    # Check 4: Dates are in January 2026 (Brisbane is early January)
    bad_dates = [d for d in date_counts if not d.startswith("2026-01")]
    if bad_dates:
        print(f"\n  WARN: Unexpected dates outside Jan 2026: {bad_dates}")
    else:
        print(f"  PASS (all dates in January 2026)")

    # Check 5: Round distribution
    round_counts = Counter(m.round for m in matches)
    print(f"\n[CHECK 4] Round distribution:")
    for rnd, count in sorted(round_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {rnd}: {count}")

    # Check 6: Sample matches
    print(f"\n[CHECK 5] Sample matches:")
    for m in matches[:3]:
        print(f"  {m.match_date} | {m.round:>4} | {m.player_a_name} vs {m.player_b_name} | {m.score_raw}")
    print(f"  ...")
    for m in matches[-2:]:
        print(f"  {m.match_date} | {m.round:>4} | {m.player_a_name} vs {m.player_b_name} | {m.score_raw}")

    all_pass = without_date == 0 and len(matches) >= 20 and len(date_counts) >= 3
    print(f"\n{'='*60}")
    print(f"OVERALL: {'PASS' if all_pass else 'FAIL'}")
    print(f"{'='*60}")
    return all_pass


def main():
    success = asyncio.run(test_brisbane())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
