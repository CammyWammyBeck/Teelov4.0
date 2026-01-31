#!/usr/bin/env python3
"""
Player Profile Page Explorer.

Navigates ATP and WTA player profile pages with Playwright and dumps
HTML structure for CSS selector discovery. This informs how to build
the player enrichment scraper.

Saves rendered HTML files to scratchpad for offline analysis.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/test_player_explore.py
"""

import asyncio
import re
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bs4 import BeautifulSoup
from teelo.scrape.base import BaseScraper, ScrapedMatch, ScrapedFixture
from typing import AsyncGenerator

# Where to save HTML dumps for offline analysis
SCRATCHPAD = Path("/tmp/claude-1000/-home-cammybeck-Documents-programming-Teelov4-0/34007b25-e7b3-4f68-8d4c-e43f15d6f0fe/scratchpad")
SCRATCHPAD.mkdir(parents=True, exist_ok=True)

# Sample players to explore
WTA_PLAYERS = [
    ("320760", "aryna-sabalenka"),
    ("326408", "iga-swiatek"),
]

ATP_PLAYERS = [
    ("a0e2", "carlos-alcaraz"),
    ("s0ag", "jannik-sinner"),
]


class ExplorerScraper(BaseScraper):
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


def save_html(filename: str, html: str) -> Path:
    """Save HTML to scratchpad and return path."""
    path = SCRATCHPAD / filename
    path.write_text(html, encoding="utf-8")
    print(f"  Saved HTML ({len(html):,} chars) to {path}")
    return path


def log_elements(soup: BeautifulSoup, description: str, selector: str, max_items: int = 20):
    """Find and log elements matching a CSS selector."""
    elements = soup.select(selector)
    print(f"\n  {description}: found {len(elements)} elements with '{selector}'")
    for i, elem in enumerate(elements[:max_items]):
        classes = " ".join(elem.get("class", []))
        text = elem.get_text(strip=True)[:200]
        href = elem.get("href", "")
        line = f"    [{i}] <{elem.name} class='{classes}'>"
        if href:
            line += f" href='{href}'"
        line += f" text='{text}'"
        print(line)


def find_demographic_elements(soup: BeautifulSoup):
    """Search for elements containing known demographic keywords."""
    keywords = [
        "birth", "date", "born", "age",
        "height", "weight",
        "hand", "plays", "right", "left",
        "backhand", "one-hand", "two-hand",
        "turned pro", "pro since",
        "nationality", "country", "flag",
        "coach",
    ]

    print("\n  Searching for demographic-related elements...")
    for keyword in keywords:
        # Search in text content
        elements = soup.find_all(string=re.compile(keyword, re.I))
        if elements:
            print(f"\n  Text containing '{keyword}': {len(elements)} matches")
            for i, elem in enumerate(elements[:5]):
                parent = elem.parent
                if parent:
                    classes = " ".join(parent.get("class", []))
                    text = parent.get_text(strip=True)[:200]
                    print(f"    [{i}] <{parent.name} class='{classes}'> text='{text}'")

        # Search in class names
        class_elements = soup.find_all(attrs={"class": re.compile(keyword, re.I)})
        if class_elements:
            print(f"\n  Elements with '{keyword}' in class: {len(class_elements)}")
            for i, elem in enumerate(class_elements[:5]):
                classes = " ".join(elem.get("class", []))
                text = elem.get_text(strip=True)[:200]
                print(f"    [{i}] <{elem.name} class='{classes}'> text='{text}'")


async def explore_wta_player(scraper: ExplorerScraper):
    """Explore WTA player profile pages."""
    print("\n" + "=" * 70)
    print("WTA PLAYER PROFILE EXPLORATION")
    print("=" * 70)

    page = await scraper.new_page()
    try:
        for wta_id, slug in WTA_PLAYERS:
            url = f"https://www.wtatennis.com/players/{wta_id}/{slug}"
            print(f"\n  Navigating to {url}")
            await scraper.navigate(page, url, wait_for="domcontentloaded")
            await asyncio.sleep(5)

            # Dismiss cookie consent
            try:
                for selector in ["#onetrust-accept-btn-handler", "button[id*='accept']"]:
                    btn = await page.query_selector(selector)
                    if btn and await btn.is_visible():
                        await btn.click()
                        print(f"  Cookie consent dismissed via {selector}")
                        await asyncio.sleep(1)
                        break
            except Exception as e:
                print(f"  Cookie consent: {e}")

            html = await page.content()
            save_html(f"wta_player_{slug}.html", html)
            soup = BeautifulSoup(html, "lxml")

            print(f"  Page title: {soup.title.string if soup.title else 'N/A'}")

            # Look for demographic data
            find_demographic_elements(soup)

            # Look for common profile section patterns
            for sel in [
                "[class*='player']",
                "[class*='profile']",
                "[class*='bio']",
                "[class*='detail']",
                "[class*='info']",
                "[class*='stat']",
                "dl", "dt", "dd",  # definition lists often used for profile data
                "table",
            ]:
                log_elements(soup, f"Profile elements ({sel})", sel, max_items=10)

            # Only explore first player in detail
            break

    finally:
        await page.close()


async def explore_atp_player(scraper: ExplorerScraper):
    """Explore ATP player profile pages."""
    print("\n" + "=" * 70)
    print("ATP PLAYER PROFILE EXPLORATION")
    print("=" * 70)

    page = await scraper.new_page()
    try:
        for atp_id, slug in ATP_PLAYERS:
            url = f"https://www.atptour.com/en/players/{slug}/{atp_id}/overview"
            print(f"\n  Navigating to {url}")

            try:
                await scraper.navigate(page, url, wait_for="domcontentloaded")
                await asyncio.sleep(8)  # ATP has Cloudflare, give extra time

                html = await page.content()
                print(f"  HTML length: {len(html):,}")

                # Check if we got blocked by Cloudflare
                if "challenge-platform" in html or len(html) < 5000:
                    print("  WARNING: Likely blocked by Cloudflare")
                    print("  Page text preview:")
                    soup = BeautifulSoup(html, "lxml")
                    print(f"  {soup.get_text(strip=True)[:500]}")
                    # Try waiting longer
                    print("  Waiting 15s for Cloudflare challenge to resolve...")
                    await asyncio.sleep(15)
                    html = await page.content()
                    print(f"  HTML length after wait: {len(html):,}")

                save_html(f"atp_player_{slug}.html", html)
                soup = BeautifulSoup(html, "lxml")

                print(f"  Page title: {soup.title.string if soup.title else 'N/A'}")

                # Check if it's a real profile page
                text = soup.get_text().lower()
                if slug.replace("-", " ") in text:
                    print(f"  *** Player name found in page content ***")

                    find_demographic_elements(soup)

                    for sel in [
                        "[class*='player']",
                        "[class*='profile']",
                        "[class*='bio']",
                        "[class*='detail']",
                        "[class*='info']",
                        "[class*='stat']",
                        "dl", "dt", "dd",
                        "table",
                    ]:
                        log_elements(soup, f"Profile elements ({sel})", sel, max_items=10)
                else:
                    print(f"  Player name NOT found — page may be blocked/wrong")

            except Exception as e:
                print(f"  ERROR: {e}")

            # Only explore first player in detail
            break

    finally:
        await page.close()


async def main():
    """Run all exploration steps."""
    print("=" * 70)
    print("Player Profile Page Explorer")
    print("=" * 70)
    print(f"Scratchpad: {SCRATCHPAD}")

    async with ExplorerScraper(headless=False) as scraper:
        await explore_wta_player(scraper)
        await explore_atp_player(scraper)

    print("\n" + "=" * 70)
    print("EXPLORATION COMPLETE")
    print("=" * 70)
    print(f"\nHTML files saved to: {SCRATCHPAD}")
    print("Review the HTML files to determine the correct CSS selectors.")


if __name__ == "__main__":
    asyncio.run(main())
