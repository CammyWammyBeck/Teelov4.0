#!/usr/bin/env python3
"""
Test script: Scrape an ITF tournament draw and print all matches.

Iteratively tests the ITF scraper logic until all 31 matches are captured
correctly with proper rounds, players, scores, and winners.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/test_itf_scrape.py
"""

import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# Completed M15 Monastir Jan 2025
TEST_URL = "https://www.itftennis.com/en/tournament/m15-monastir/tun/2025/m-itf-tun-2025-001/draws-and-results/"

# Expected: 16 + 8 + 4 + 2 + 1 = 31 matches for a 32-draw tournament

# Round name normalization for ITF 32-draw tournaments
ITF_ROUND_MAP = {
    "1st round": "R32",
    "2nd round": "R16",
    "quarter-finals": "QF",
    "quarterfinals": "QF",
    "semi-finals": "SF",
    "semifinals": "SF",
    "final": "F",
}


def normalize_round(raw: str) -> str:
    """Normalize ITF round name to standard code."""
    return ITF_ROUND_MAP.get(raw.lower().strip(), raw.upper())


def parse_match(widget, round_name: str) -> dict | None:
    """
    Parse a single match from a drawsheet-widget element.

    Returns dict with player_a, player_b, winner, score, itf_id_a, itf_id_b, round.
    Returns None if the match can't be parsed (e.g. BYE).
    """
    # Get both team-info wrappers
    team1 = widget.select_one('.drawsheet-widget__team-info--team-1')
    team2 = widget.select_one('.drawsheet-widget__team-info--team-2')

    if not team1 or not team2:
        return None

    # Extract player info from each team
    player_a = _extract_player(team1)
    player_b = _extract_player(team2)

    if not player_a or not player_b:
        return None

    # Skip BYEs
    if player_a["name"].lower() == "bye" or player_b["name"].lower() == "bye":
        return None

    # Determine winner from is-winner class
    winner = None
    if "is-winner" in (team1.get("class") or []):
        winner = player_a["name"]
    elif "is-winner" in (team2.get("class") or []):
        winner = player_b["name"]

    # Extract scores - each team has score spans
    scores_a = [s.get_text(strip=True) for s in team1.select('.drawsheet-widget__score')]
    scores_b = [s.get_text(strip=True) for s in team2.select('.drawsheet-widget__score')]

    # Build score string (e.g. "6-2 6-4")
    score = _build_score(scores_a, scores_b)

    # Detect walkover: no scores but a winner
    status = "completed"
    if not scores_a and not scores_b and winner:
        status = "walkover"
        score = "W/O"
    elif score and ("ret" in score.lower()):
        status = "retired"

    return {
        "round": round_name,
        "player_a": player_a["name"],
        "player_b": player_b["name"],
        "itf_id_a": player_a["itf_id"],
        "itf_id_b": player_b["itf_id"],
        "nationality_a": player_a["nationality"],
        "nationality_b": player_b["nationality"],
        "seed_a": player_a["seed"],
        "seed_b": player_b["seed"],
        "winner": winner,
        "score": score,
        "status": status,
    }


def _extract_player(team_info) -> dict | None:
    """Extract player name, ITF ID, nationality, and seed from a team-info element."""
    # Player link contains first/last name and ITF ID in href
    player_link = team_info.select_one('.player-wrapper a')
    if not player_link:
        return None

    first = player_link.select_one('.drawsheet-widget__first-name')
    last = player_link.select_one('.drawsheet-widget__last-name')

    if first and last:
        name = f"{first.get_text(strip=True)} {last.get_text(strip=True)}"
    else:
        name = player_link.get_text(strip=True)

    if not name or name.lower() == "bye":
        return {"name": "BYE", "itf_id": None, "nationality": None, "seed": None}

    # ITF ID from href: /en/players/name/800399810/rus/mt/s/
    itf_id = None
    href = player_link.get("href", "")
    id_match = re.search(r'/players/[^/]+/(\d+)/', href)
    if id_match:
        itf_id = id_match.group(1)

    # Nationality from flag span
    nationality = None
    flag = team_info.select_one('.drawsheet-widget__nationality .itf-flags')
    if flag:
        classes = flag.get("class", [])
        for c in classes:
            if c.startswith("itf-flags--"):
                nationality = c.replace("itf-flags--", "")

    # Seed from seeding span
    seed = None
    seed_elem = team_info.select_one('.drawsheet-widget__seeding')
    if seed_elem:
        seed_text = seed_elem.get_text(strip=True)
        seed_match = re.search(r'\[(\d+)\]', seed_text)
        if seed_match:
            seed = int(seed_match.group(1))

    return {"name": name, "itf_id": itf_id, "nationality": nationality, "seed": seed}


def _build_score(scores_a: list[str], scores_b: list[str]) -> str:
    """
    Build a score string from two lists of set scores.

    Handles ITF's tiebreak format where e.g. "64" means lost tiebreak 4.
    """
    if not scores_a and not scores_b:
        return ""

    sets = []
    for sa, sb in zip(scores_a, scores_b):
        # Clean to digits only
        da = "".join(filter(str.isdigit, sa))
        db = "".join(filter(str.isdigit, sb))

        if not da or not db:
            sets.append(f"{sa}-{sb}")
            continue

        # Tiebreak detection: if one score has 2+ digits and starts with 6,
        # the extra digits are tiebreak points
        if len(da) > 1 and da[0] == "6" and db == "7":
            sets.append(f"6-7({da[1:]})")
        elif len(db) > 1 and db[0] == "6" and da == "7":
            sets.append(f"7-6({db[1:]})")
        else:
            sets.append(f"{da}-{db}")

    return " ".join(sets)


def parse_round_container(container) -> list[dict]:
    """Parse all matches from a drawsheet-round-container."""
    title_elem = container.select_one('.drawsheet-round-container__round-title')
    if not title_elem:
        return []

    round_name = normalize_round(title_elem.get_text(strip=True))
    matches = []

    for widget in container.select('.drawsheet-widget'):
        match = parse_match(widget, round_name)
        if match:
            matches.append(match)

    return matches


async def main():
    stealth = Stealth()
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=False)
    ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
    page = await ctx.new_page()
    await stealth.apply_stealth_async(page)

    print(f"Navigating to {TEST_URL}")
    await page.goto(TEST_URL, wait_until="domcontentloaded")
    await asyncio.sleep(4)

    # Accept cookies
    try:
        btn = await page.wait_for_selector("#onetrust-accept-btn-handler", timeout=5000)
        if btn:
            await btn.click()
            await asyncio.sleep(1)
    except:
        pass

    all_matches = []
    seen_rounds = set()

    # Collect matches from up to 3 page views (initial + 2 next clicks)
    for view_idx in range(3):
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        containers = soup.select('.drawsheet-round-container')
        for container in containers:
            title_elem = container.select_one('.drawsheet-round-container__round-title')
            if not title_elem:
                continue
            round_name = normalize_round(title_elem.get_text(strip=True))

            # Skip rounds we've already processed
            if round_name in seen_rounds:
                continue
            seen_rounds.add(round_name)

            matches = parse_round_container(container)
            all_matches.extend(matches)
            print(f"  View {view_idx}: {round_name} -> {len(matches)} matches")

        # Click next button if more views needed
        if view_idx < 2:
            try:
                next_btn = await page.wait_for_selector('button.btn--chevron-next', timeout=3000)
                if next_btn and await next_btn.is_visible():
                    await next_btn.click()
                    await asyncio.sleep(2)
                else:
                    print("  No more next button")
                    break
            except:
                print("  No next button found")
                break

    await browser.close()
    await pw.stop()

    # Print results
    print(f"\n{'='*80}")
    print(f"TOTAL MATCHES: {len(all_matches)}")
    print(f"{'='*80}")

    # Group by round
    from collections import Counter
    round_counts = Counter(m["round"] for m in all_matches)
    print(f"\nMatches by round:")
    for r in ["R32", "R16", "QF", "SF", "F"]:
        count = round_counts.get(r, 0)
        expected = {"R32": 16, "R16": 8, "QF": 4, "SF": 2, "F": 1}.get(r, 0)
        status = "OK" if count == expected else f"WRONG (expected {expected})"
        print(f"  {r}: {count} {status}")

    print(f"\nAll matches:")
    for i, m in enumerate(all_matches, 1):
        seed_a = f"[{m['seed_a']}]" if m['seed_a'] else ""
        seed_b = f"[{m['seed_b']}]" if m['seed_b'] else ""
        winner_marker = ""
        if m["winner"] == m["player_a"]:
            winner_marker = " *A wins*"
        elif m["winner"] == m["player_b"]:
            winner_marker = " *B wins*"
        print(f"  {i:2d}. {m['round']:3s} | {m['player_a']}{seed_a} vs {m['player_b']}{seed_b} | {m['score']} | {m['status']}{winner_marker}")


if __name__ == "__main__":
    asyncio.run(main())
