#!/usr/bin/env python3
"""
Player Profile Enrichment Test Script.

Scrapes player profile pages on ATP and WTA websites and prints extracted
demographic data for verification. No database writes.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/test_player_enrich.py --source atp
    python scripts/test_player_enrich.py --source wta
    python scripts/test_player_enrich.py --source both
"""

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path
from typing import AsyncGenerator, Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bs4 import BeautifulSoup
from teelo.scrape.base import BaseScraper, ScrapedMatch, ScrapedFixture


# Sample players with known data for verification
WTA_TEST_PLAYERS = [
    # (wta_id, slug, expected_dob, expected_height_cm, expected_hand)
    ("320760", "aryna-sabalenka", "1998-05-05", 182, "Right"),
    ("326408", "iga-swiatek", "2001-05-31", 176, "Right"),
    ("328560", "coco-gauff", "2004-03-13", 175, "Right"),
    ("316956", "jessica-pegula", "1994-02-24", 170, "Right"),
    ("324166", "elena-rybakina", "1999-06-17", 184, "Right"),
]

ATP_TEST_PLAYERS = [
    # (atp_id, slug, expected_dob, expected_height_cm, expected_hand)
    ("a0e2", "carlos-alcaraz", "2003-05-05", 183, "Right"),
    ("s0ag", "jannik-sinner", "2001-08-16", 188, "Right"),
    ("D643", "novak-djokovic", "1987-05-22", 188, "Right"),
    ("MM58", "daniil-medvedev", "1996-02-11", 198, "Right"),
    ("Z355", "alexander-zverev", "1997-04-20", 190, "Right"),
]


class ProfileScraper(BaseScraper):
    """Minimal concrete scraper just for browser access."""

    BASE_URL = "https://www.wtatennis.com"

    async def get_tournament_list(self, year: int) -> list[dict]:
        return []

    async def scrape_tournament_results(self, tournament_id: str, year: int) -> AsyncGenerator[ScrapedMatch, None]:
        return
        yield

    async def scrape_fixtures(self, tournament_id: str) -> AsyncGenerator[ScrapedFixture, None]:
        return
        yield


def parse_wta_profile(html: str) -> dict:
    """
    Parse a WTA player profile page and extract demographic data.

    WTA profile structure:
    - JSON-LD schema.org script with birthDate, birthPlace, nationality
    - div.profile-bio__info-block elements with title/content pairs:
      Plays, Career High, Height, Birthday, Birthplace
    """
    soup = BeautifulSoup(html, "lxml")
    result = {}

    # 1. JSON-LD schema.org data (most reliable for DOB/nationality)
    for script in soup.find_all("script"):
        text = script.string or ""
        if '"@type": "Person"' in text or '"@type":"Person"' in text:
            try:
                data = json.loads(text.strip())
                if data.get("birthDate"):
                    result["birth_date"] = data["birthDate"]  # Already YYYY-MM-DD
                if data.get("nationality"):
                    result["nationality"] = data["nationality"]
            except (json.JSONDecodeError, KeyError):
                pass
            break

    # 2. Bio info blocks for height, plays, etc.
    for block in soup.select("div.profile-bio__info-block"):
        title_el = block.select_one(".profile-bio__info-title")
        content_el = block.select_one(".profile-bio__info-content")
        if not title_el or not content_el:
            continue

        label = title_el.get_text(strip=True)
        value = content_el.get_text(strip=True)

        if label == "Plays":
            # "Right-Handed" or "Left-Handed"
            if "right" in value.lower():
                result["hand"] = "Right"
            elif "left" in value.lower():
                result["hand"] = "Left"

        elif label == "Height":
            # "5' 11\" (1.82m)" — extract meters and convert to cm
            m = re.search(r'\((\d+\.\d+)m\)', value)
            if m:
                result["height_cm"] = int(round(float(m.group(1)) * 100))

    # WTA doesn't show backhand style or turned pro year on profile page

    return result


def parse_atp_profile(html: str) -> dict:
    """
    Parse an ATP player profile page and extract demographic data.

    ATP profile structure:
    - div.personal_details contains:
      - ul.pd_left: Age (with DOB), Weight, Height, Turned pro
      - ul.pd_right: Country, Birthplace, Plays (with backhand), Coach
    - Each li has two spans: label and value
    """
    soup = BeautifulSoup(html, "lxml")
    result = {}

    details = soup.select_one("div.personal_details")
    if not details:
        return result

    # Parse all li elements from both columns
    for li in details.select("li"):
        spans = li.select("span")
        if len(spans) < 2:
            continue

        label = spans[0].get_text(strip=True)
        value = spans[1].get_text(strip=True)

        if label == "Age":
            # "22 (2003/05/05)" — extract DOB
            m = re.search(r'\((\d{4})/(\d{2})/(\d{2})\)', value)
            if m:
                result["birth_date"] = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        elif label == "Height":
            # "6'0\" (183cm)" — extract cm
            m = re.search(r'\((\d+)cm\)', value)
            if m:
                result["height_cm"] = int(m.group(1))

        elif label == "Turned pro":
            # "2018"
            m = re.search(r'(\d{4})', value)
            if m:
                result["turned_pro_year"] = int(m.group(1))

        elif label == "Plays":
            # "Right-Handed, Two-Handed Backhand"
            if "right" in value.lower():
                result["hand"] = "Right"
            elif "left" in value.lower():
                result["hand"] = "Left"

            if "two-handed" in value.lower():
                result["backhand"] = "Two-Handed"
            elif "one-handed" in value.lower():
                result["backhand"] = "One-Handed"

        elif label == "Country":
            result["nationality"] = value

        elif label == "Weight":
            # "163 lbs (74kg)" — extract kg
            m = re.search(r'\((\d+)kg\)', value)
            if m:
                result["weight_kg"] = int(m.group(1))

    return result


async def test_wta(scraper: ProfileScraper):
    """Scrape WTA test players and print results."""
    print("\n" + "=" * 70)
    print("WTA PLAYER PROFILES")
    print("=" * 70)

    for wta_id, slug, exp_dob, exp_height, exp_hand in WTA_TEST_PLAYERS:
        url = f"https://www.wtatennis.com/players/{wta_id}/{slug}"
        print(f"\n  {slug} ({wta_id})")
        print(f"  URL: {url}")

        # Open a fresh page per player to avoid SPA stale content issues
        page = await scraper.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)

            # Dismiss cookie consent if visible
            try:
                btn = await page.query_selector("#onetrust-accept-btn-handler")
                if btn and await btn.is_visible():
                    await btn.click()
                    await asyncio.sleep(1)
            except Exception:
                pass

            # Wait for bio section to render
            try:
                await page.wait_for_selector("div.profile-bio__info-block", timeout=10000)
            except Exception:
                pass
            await asyncio.sleep(2)

            html = await page.content()
            data = parse_wta_profile(html)

            print(f"  Extracted: {data}")

            # Verify against expected
            checks = []
            if data.get("birth_date") == exp_dob:
                checks.append("DOB ✓")
            else:
                checks.append(f"DOB ✗ (got {data.get('birth_date')}, expected {exp_dob})")

            if data.get("height_cm") == exp_height:
                checks.append("Height ✓")
            else:
                checks.append(f"Height ✗ (got {data.get('height_cm')}, expected {exp_height})")

            if data.get("hand") == exp_hand:
                checks.append("Hand ✓")
            else:
                checks.append(f"Hand ✗ (got {data.get('hand')}, expected {exp_hand})")

            print(f"  Checks: {', '.join(checks)}")

        except Exception as e:
            print(f"  ERROR: {e}")
        finally:
            await page.close()


async def test_atp(scraper: ProfileScraper):
    """Scrape ATP test players and print results."""
    print("\n" + "=" * 70)
    print("ATP PLAYER PROFILES")
    print("=" * 70)

    for atp_id, slug, exp_dob, exp_height, exp_hand in ATP_TEST_PLAYERS:
        url = f"https://www.atptour.com/en/players/{slug}/{atp_id}/overview"
        print(f"\n  {slug} ({atp_id})")
        print(f"  URL: {url}")

        # Fresh page per player to avoid SPA stale content
        page = await scraper.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(8)  # ATP needs more time (Cloudflare)

            html = await page.content()

            # Check if blocked by Cloudflare
            if len(html) < 5000 or "challenge-platform" in html:
                print("  Cloudflare challenge detected, waiting 15s...")
                await asyncio.sleep(15)
                html = await page.content()

            data = parse_atp_profile(html)
            print(f"  Extracted: {data}")

            # Verify against expected
            checks = []
            if data.get("birth_date") == exp_dob:
                checks.append("DOB ✓")
            else:
                checks.append(f"DOB ✗ (got {data.get('birth_date')}, expected {exp_dob})")

            if data.get("height_cm") == exp_height:
                checks.append("Height ✓")
            else:
                checks.append(f"Height ✗ (got {data.get('height_cm')}, expected {exp_height})")

            if data.get("hand") == exp_hand:
                checks.append("Hand ✓")
            else:
                checks.append(f"Hand ✗ (got {data.get('hand')}, expected {exp_hand})")

            print(f"  Checks: {', '.join(checks)}")

        except Exception as e:
            print(f"  ERROR: {e}")
        finally:
            await page.close()


async def main():
    parser = argparse.ArgumentParser(description="Test player profile scraping")
    parser.add_argument("--source", choices=["atp", "wta", "both"], default="both",
                        help="Which tour to test")
    args = parser.parse_args()

    print("=" * 70)
    print("Player Profile Enrichment Test")
    print("=" * 70)

    async with ProfileScraper(headless=False) as scraper:
        if args.source in ("wta", "both"):
            await test_wta(scraper)
        if args.source in ("atp", "both"):
            await test_atp(scraper)

    print("\n" + "=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
