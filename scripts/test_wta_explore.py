#!/usr/bin/env python3
"""
WTA Website Structure Explorer.

One-time exploration tool that navigates the WTA website with Playwright
and dumps HTML structure, CSS selectors, and URL patterns for analysis.
This informs how to build the actual WTA scraper.

Saves rendered HTML files to scratchpad for offline analysis.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/test_wta_explore.py
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

WTA_BASE = "https://www.wtatennis.com"


class ExplorerScraper(BaseScraper):
    """Minimal concrete scraper just for browser access."""

    BASE_URL = WTA_BASE

    async def get_tournament_list(self, year: int) -> list[dict]:
        return []

    async def scrape_tournament_results(self, tournament_id: str, year: int) -> AsyncGenerator[ScrapedMatch, None]:
        return
        yield  # Make it a generator

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
    if len(elements) > max_items:
        print(f"    ... and {len(elements) - max_items} more")


def log_links_containing(soup: BeautifulSoup, pattern: str, max_items: int = 30):
    """Find and log <a> tags whose href contains a pattern."""
    links = soup.select(f"a[href*='{pattern}']")
    print(f"\n  Links with '{pattern}' in href: {len(links)}")
    for i, link in enumerate(links[:max_items]):
        href = link.get("href", "")
        text = link.get_text(strip=True)[:100]
        print(f"    [{i}] {href}  ->  '{text}'")
    if len(links) > max_items:
        print(f"    ... and {len(links) - max_items} more")


async def step1_tournament_calendar(scraper: ExplorerScraper):
    """Explore the main tournament calendar page."""
    print("\n" + "=" * 70)
    print("STEP 1: Tournament Calendar Page")
    print("=" * 70)

    page = await scraper.new_page()
    try:
        url = f"{WTA_BASE}/tournaments"
        print(f"  Navigating to {url}")
        await scraper.navigate(page, url, wait_for="domcontentloaded")
        await asyncio.sleep(5)  # Wait for JS to render

        # Try to dismiss cookie consent
        try:
            for selector in ["#onetrust-accept-btn-handler", "button[id*='accept']", "[aria-label*='Accept']"]:
                btn = await page.query_selector(selector)
                if btn and await btn.is_visible():
                    await btn.click()
                    print(f"  Cookie consent dismissed via {selector}")
                    await asyncio.sleep(1)
                    break
        except Exception as e:
            print(f"  Cookie consent: {e}")

        html = await page.content()
        save_html("wta_tournaments_page.html", html)
        soup = BeautifulSoup(html, "lxml")

        print(f"  Page title: {soup.title.string if soup.title else 'N/A'}")

        # Find tournament-related links
        log_links_containing(soup, "tournament")

        # Find tournament card/event elements
        for keyword in ["tournament", "event", "card", "calendar"]:
            elements = soup.find_all(attrs={"class": re.compile(keyword, re.I)})
            if elements:
                print(f"\n  Elements with '{keyword}' in class: {len(elements)}")
                for i, elem in enumerate(elements[:10]):
                    classes = " ".join(elem.get("class", []))
                    text = elem.get_text(strip=True)[:200]
                    print(f"    [{i}] <{elem.name} class='{classes}'> text='{text}'")

        # Look for year filter
        for sel in ["select", "[class*='year']", "[class*='filter']", "[class*='dropdown']"]:
            log_elements(soup, f"Year/filter elements ({sel})", sel, max_items=5)

    finally:
        await page.close()


async def step2_year_filtered_urls(scraper: ExplorerScraper):
    """Try various year-filtered URL patterns."""
    print("\n" + "=" * 70)
    print("STEP 2: Year-Filtered URLs")
    print("=" * 70)

    urls = [
        f"{WTA_BASE}/tournaments?year=2024",
        f"{WTA_BASE}/tournaments/2024",
        f"{WTA_BASE}/tournament-calendar?year=2024",
        f"{WTA_BASE}/tournament-calendar/2024",
    ]

    page = await scraper.new_page()
    try:
        for url in urls:
            try:
                print(f"\n  Trying: {url}")
                response = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(3)

                status = response.status if response else "no response"
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")
                title = soup.title.string if soup.title else "N/A"
                has_tournament = "tournament" in html.lower()
                tournament_links = soup.select("a[href*='tournament']")

                print(f"    Status: {status}")
                print(f"    Title: {title}")
                print(f"    Has 'tournament' in HTML: {has_tournament}")
                print(f"    Tournament links found: {len(tournament_links)}")
                print(f"    HTML length: {len(html):,}")

                # If this looks like a calendar page, save it
                if len(tournament_links) > 5:
                    save_html(f"wta_calendar_{url.split('/')[-1].replace('?', '_')}.html", html)

            except Exception as e:
                print(f"    ERROR: {e}")
    finally:
        await page.close()


async def step3_australian_open(scraper: ExplorerScraper):
    """Try to find the Australian Open 2024 tournament page."""
    print("\n" + "=" * 70)
    print("STEP 3: Australian Open 2024 Tournament Page")
    print("=" * 70)

    urls = [
        f"{WTA_BASE}/tournament/2024/australian-open",
        f"{WTA_BASE}/tournament/australian-open/2024",
        f"{WTA_BASE}/tournaments/2024/australian-open",
        f"{WTA_BASE}/tournament/580/australian-open/2024",
        f"{WTA_BASE}/tournament/australian-open",
    ]

    page = await scraper.new_page()
    successful_url = None
    try:
        for url in urls:
            try:
                print(f"\n  Trying: {url}")
                response = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(3)

                status = response.status if response else "no response"
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")
                title = soup.title.string if soup.title else "N/A"

                print(f"    Status: {status}")
                print(f"    Title: {title}")
                print(f"    HTML length: {len(html):,}")

                # Check if this looks like a real tournament page (not 404/redirect)
                text = soup.get_text().lower()
                if "australian open" in text or "melbourne" in text:
                    print(f"    *** LOOKS LIKE A REAL TOURNAMENT PAGE ***")
                    save_html("wta_ao_2024_page.html", html)
                    successful_url = url

                    # Log navigation tabs/links
                    log_links_containing(soup, "draw")
                    log_links_containing(soup, "result")
                    log_links_containing(soup, "score")

                    # Log all nav-like elements
                    for sel in ["nav a", "[class*='tab']", "[class*='nav'] a", "[role='tab']"]:
                        log_elements(soup, f"Navigation ({sel})", sel, max_items=10)

                    break  # Found it, stop trying other URLs

            except Exception as e:
                print(f"    ERROR: {e}")

        if not successful_url:
            print("\n  !!! Could not find Australian Open 2024 page with any URL pattern")
            print("  Let's check what the calendar page links to...")
            # Go back to calendar and find AO link
            await page.goto(f"{WTA_BASE}/tournaments", wait_until="domcontentloaded")
            await asyncio.sleep(5)
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            log_links_containing(soup, "australian")
            log_links_containing(soup, "open")

    finally:
        await page.close()

    return successful_url


async def step4_find_match_data(scraper: ExplorerScraper, tournament_url: str = None):
    """Try to find match/draw data on the tournament page."""
    print("\n" + "=" * 70)
    print("STEP 4: Finding Match Data")
    print("=" * 70)

    if not tournament_url:
        print("  No tournament URL found in step 3, trying common patterns...")
        tournament_url = f"{WTA_BASE}/tournament/2024/australian-open"

    page = await scraper.new_page()
    try:
        # First load the tournament page
        await scraper.navigate(page, tournament_url, wait_for="domcontentloaded")
        await asyncio.sleep(3)

        # Try appending various suffixes for draws/results
        suffixes = ["/draws", "/results", "/scores", "?tab=draws", "?tab=results"]
        base = tournament_url.rstrip("/")

        for suffix in suffixes:
            try:
                url = base + suffix
                print(f"\n  Trying: {url}")
                response = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(4)

                html = await page.content()
                soup = BeautifulSoup(html, "lxml")

                # Check if there's match-like content
                text = soup.get_text()
                # Look for score patterns like "6-4" or player names
                score_pattern = re.findall(r'\b[0-7]-[0-7]\b', text)
                has_scores = len(score_pattern) > 5

                print(f"    HTML length: {len(html):,}")
                print(f"    Score-like patterns found: {len(score_pattern)}")

                if has_scores:
                    print(f"    *** FOUND MATCH DATA ***")
                    save_html("wta_ao_2024_matches.html", html)

                    # Analyze match structure
                    for keyword in ["match", "score", "player", "draw", "bracket", "result", "winner", "loser", "competitor"]:
                        elements = soup.find_all(attrs={"class": re.compile(keyword, re.I)})
                        if elements:
                            print(f"\n    Elements with '{keyword}' in class: {len(elements)}")
                            for i, elem in enumerate(elements[:5]):
                                classes = " ".join(elem.get("class", []))
                                text_content = elem.get_text(strip=True)[:300]
                                print(f"      [{i}] <{elem.name} class='{classes}'>")
                                print(f"           text='{text_content}'")

                    # Find player links
                    log_links_containing(soup, "player")

                    # Look for round headers
                    for sel in ["[class*='round']", "h2", "h3", "[class*='header']"]:
                        elements = soup.select(sel)
                        round_elements = [e for e in elements if any(
                            w in e.get_text().lower() for w in ["final", "semi", "quarter", "round", "r16", "r32", "r64", "r128"]
                        )]
                        if round_elements:
                            print(f"\n    Round headers ({sel}): {len(round_elements)}")
                            for e in round_elements[:10]:
                                print(f"      '{e.get_text(strip=True)[:100]}'")

                    break  # Found data, stop trying

            except Exception as e:
                print(f"    ERROR: {e}")

        # Also try clicking tabs on the original page
        print("\n  Trying to click Draw/Results tabs...")
        await page.goto(tournament_url, wait_until="domcontentloaded")
        await asyncio.sleep(3)

        for tab_text in ["Draws", "Results", "Scores"]:
            try:
                tab = await page.query_selector(f"text={tab_text}")
                if tab and await tab.is_visible():
                    print(f"    Found '{tab_text}' tab, clicking...")
                    await tab.click()
                    await asyncio.sleep(4)

                    html = await page.content()
                    score_pattern = re.findall(r'\b[0-7]-[0-7]\b', html)
                    print(f"    After click: HTML={len(html):,}, scores={len(score_pattern)}")

                    if len(score_pattern) > 5:
                        save_html(f"wta_ao_2024_{tab_text.lower()}_tab.html", html)
                        print(f"    *** FOUND DATA via {tab_text} tab ***")
            except Exception as e:
                print(f"    Tab '{tab_text}': {e}")

    finally:
        await page.close()


async def main():
    """Run all exploration steps."""
    print("=" * 70)
    print("WTA Website Structure Explorer")
    print("=" * 70)
    print(f"Scratchpad: {SCRATCHPAD}")
    print()

    async with ExplorerScraper(headless=False) as scraper:
        await step1_tournament_calendar(scraper)
        await step2_year_filtered_urls(scraper)
        tournament_url = await step3_australian_open(scraper)
        await step4_find_match_data(scraper, tournament_url)

    print("\n" + "=" * 70)
    print("EXPLORATION COMPLETE")
    print("=" * 70)
    print(f"\nHTML files saved to: {SCRATCHPAD}")
    print("Review the HTML files to determine the correct CSS selectors.")


if __name__ == "__main__":
    asyncio.run(main())
