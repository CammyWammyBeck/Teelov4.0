#!/usr/bin/env python3
"""
Explore WTA tournament list page structure.

Navigates to https://www.wtatennis.com/tournaments?status=all&year=2026
and analyzes the HTML structure to understand how to scrape tournament data.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/explore_wta_tournaments.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

_stealth = Stealth()


async def explore_tournaments_page():
    """Navigate to WTA tournaments page and analyze HTML structure."""

    url = "https://www.wtatennis.com/tournaments?status=all&year=2026"

    print("=" * 80)
    print("WTA Tournament List Page Explorer")
    print(f"URL: {url}")
    print("=" * 80)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()
        await _stealth.apply_stealth_async(page)

        print("\nNavigating to page...")
        await page.goto(url, wait_until="domcontentloaded")

        # Wait for JS to render
        print("Waiting for content to load...")
        await asyncio.sleep(5)

        # Try to wait for tournament elements
        try:
            await page.wait_for_selector("[class*='tournament'], [class*='event'], article", timeout=15000)
        except Exception as e:
            print(f"Warning: Timeout waiting for tournament elements: {e}")

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # Save HTML for manual inspection
        html_path = Path(__file__).parent.parent / "scratchpad" / "wta_tournaments.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(html)
        print(f"\nSaved HTML to: {html_path}")

        print("\n" + "=" * 80)
        print("HTML STRUCTURE ANALYSIS")
        print("=" * 80)

        # Find potential tournament containers
        print("\n--- Searching for tournament containers ---")

        # Try various selectors that might contain tournaments
        selectors_to_try = [
            ("article", "article tags"),
            ("[class*='tournament']", "elements with 'tournament' in class"),
            ("[class*='event']", "elements with 'event' in class"),
            ("[class*='card']", "elements with 'card' in class"),
            (".tournament-card", "tournament-card class"),
            (".event-card", "event-card class"),
            ("a[href*='/tournaments/']", "links to tournaments"),
        ]

        for selector, description in selectors_to_try:
            elements = soup.select(selector)
            if elements:
                print(f"\n✓ Found {len(elements)} {description}")
                # Show first element's structure
                if elements:
                    first = elements[0]
                    print(f"  First element tag: <{first.name}>")
                    print(f"  Classes: {first.get('class', [])}")
                    if first.name == 'a':
                        print(f"  Href: {first.get('href', '')[:80]}...")
                    # Show children
                    children = list(first.children)
                    child_tags = [c.name for c in children if hasattr(c, 'name') and c.name]
                    print(f"  Direct children: {child_tags[:10]}")

        # Look for tournament links specifically
        print("\n--- Tournament Links Analysis ---")
        tournament_links = soup.select("a[href*='/tournaments/']")
        unique_patterns = set()
        for link in tournament_links[:20]:
            href = link.get("href", "")
            # Extract pattern
            import re
            match = re.search(r"/tournaments/(\d+)/([^/]+)/(\d+)", href)
            if match:
                unique_patterns.add(f"/tournaments/{match.group(1)}/{match.group(2)}/{match.group(3)}")

        print(f"Found {len(tournament_links)} tournament links")
        print(f"Unique patterns (first 10):")
        for pattern in list(unique_patterns)[:10]:
            print(f"  {pattern}")

        # Look for tournament names
        print("\n--- Tournament Names ---")
        name_elements = soup.select("[class*='name'], [class*='title'], h2, h3, h4")
        tournament_names = []
        for elem in name_elements:
            text = elem.get_text(strip=True)
            if text and len(text) > 3 and len(text) < 100:
                # Filter out common non-tournament text
                if not any(x in text.lower() for x in ['login', 'menu', 'search', 'filter', 'cookie']):
                    tournament_names.append((elem.name, elem.get('class', []), text[:50]))

        print(f"Potential tournament names (first 15):")
        for tag, classes, name in tournament_names[:15]:
            print(f"  <{tag} class='{' '.join(classes[:3])}'> {name}")

        # Look for dates
        print("\n--- Date Elements ---")
        date_elements = soup.select("[class*='date'], time, [datetime]")
        print(f"Found {len(date_elements)} potential date elements")
        for elem in date_elements[:5]:
            print(f"  <{elem.name}> class={elem.get('class', [])} text='{elem.get_text(strip=True)[:40]}'")

        # Look for location/surface info
        print("\n--- Location/Surface Elements ---")
        for keyword in ['location', 'city', 'country', 'surface', 'venue']:
            elements = soup.select(f"[class*='{keyword}']")
            if elements:
                print(f"  Found {len(elements)} elements with '{keyword}' in class")
                for elem in elements[:2]:
                    print(f"    <{elem.name}> {elem.get_text(strip=True)[:40]}")

        # Print page structure overview
        print("\n--- Page Structure Overview ---")
        body = soup.find("body")
        if body:
            main_sections = body.find_all(recursive=False)
            print(f"Top-level body children: {len(main_sections)}")
            for section in main_sections[:5]:
                classes = section.get('class', [])
                print(f"  <{section.name}> class='{' '.join(classes[:3] if classes else [])}'")

        await browser.close()

        print("\n" + "=" * 80)
        print("EXPLORATION COMPLETE")
        print(f"Review the saved HTML at: {html_path}")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(explore_tournaments_page())
