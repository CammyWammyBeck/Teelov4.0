#!/usr/bin/env python3
"""
Explore WTA tournament draws page structure.

Navigates to https://www.wtatennis.com/tournaments/2088/abu-dhabi/2026/draws
and analyzes the HTML structure to understand how to scrape draw data.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/explore_wta_draws.py
"""

import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

_stealth = Stealth()


async def explore_draws_page():
    """Navigate to WTA draws page and analyze HTML structure."""

    url = "https://www.wtatennis.com/tournaments/2088/abu-dhabi/2026/draws"

    print("=" * 80)
    print("WTA Tournament Draws Page Explorer")
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

        # Try to wait for draw elements
        try:
            await page.wait_for_selector("[class*='draw'], [class*='bracket'], [class*='match']", timeout=15000)
        except Exception as e:
            print(f"Warning: Timeout waiting for draw elements: {e}")

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # Save HTML for manual inspection
        html_path = Path(__file__).parent.parent / "scratchpad" / "wta_draws.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(html)
        print(f"\nSaved HTML to: {html_path}")

        print("\n" + "=" * 80)
        print("HTML STRUCTURE ANALYSIS")
        print("=" * 80)

        # Look for filter buttons (Singles/Doubles/Qualifying)
        print("\n--- Draw Type Filter Buttons ---")
        buttons = soup.select("button, [role='tab'], [class*='tab'], [class*='filter']")
        for btn in buttons[:15]:
            text = btn.get_text(strip=True)
            if text and any(x in text.lower() for x in ['single', 'double', 'qualifying', 'qual']):
                print(f"  <{btn.name}> class={btn.get('class', [])} text='{text}'")

        # Look for draw/bracket containers
        print("\n--- Draw/Bracket Containers ---")
        selectors_to_try = [
            ("[class*='draw']", "elements with 'draw' in class"),
            ("[class*='bracket']", "elements with 'bracket' in class"),
            ("[class*='round']", "elements with 'round' in class"),
            ("[class*='match']", "elements with 'match' in class"),
            ("[class*='player']", "elements with 'player' in class"),
        ]

        for selector, description in selectors_to_try:
            elements = soup.select(selector)
            if elements:
                print(f"\n✓ Found {len(elements)} {description}")
                # Categorize by class patterns
                class_patterns = {}
                for elem in elements:
                    classes = " ".join(elem.get('class', []))
                    class_patterns[classes] = class_patterns.get(classes, 0) + 1
                # Show top patterns
                sorted_patterns = sorted(class_patterns.items(), key=lambda x: -x[1])
                for pattern, count in sorted_patterns[:5]:
                    print(f"    ({count}x) {pattern[:70]}")

        # Look for player names and links
        print("\n--- Player Elements ---")
        player_links = soup.select("a[href*='/players/']")
        print(f"Found {len(player_links)} player links")
        if player_links:
            # Analyze player link structure
            for link in player_links[:5]:
                href = link.get("href", "")
                name = link.get_text(strip=True)
                # Extract player ID from URL
                id_match = re.search(r"/players/(\d+)/", href)
                player_id = id_match.group(1) if id_match else "?"
                print(f"  ID={player_id} Name='{name}' href={href[:60]}...")

        # Look for scores
        print("\n--- Score Elements ---")
        score_patterns = [
            "[class*='score']",
            "[class*='set']",
            "[class*='game']",
            "[class*='result']",
        ]
        for pattern in score_patterns:
            elements = soup.select(pattern)
            if elements:
                print(f"\n  {pattern}: {len(elements)} elements")
                for elem in elements[:3]:
                    text = elem.get_text(strip=True)
                    if text:
                        print(f"    <{elem.name}> class={elem.get('class', [])[:3]} text='{text[:30]}'")

        # Look for round headers/labels
        print("\n--- Round Labels ---")
        round_keywords = ['final', 'semi', 'quarter', 'round', 'r16', 'r32', 'r64']
        for keyword in round_keywords:
            elements = soup.find_all(string=re.compile(keyword, re.I))
            if elements:
                print(f"  '{keyword}': found {len(elements)} text matches")
                for elem in elements[:2]:
                    parent = elem.parent
                    if parent:
                        print(f"    Parent: <{parent.name}> class={parent.get('class', [])[:3]}")

        # Look for seed indicators
        print("\n--- Seed Indicators ---")
        seed_patterns = soup.select("[class*='seed'], [class*='ranking']")
        print(f"Found {len(seed_patterns)} potential seed/ranking elements")
        for elem in seed_patterns[:5]:
            text = elem.get_text(strip=True)
            print(f"  <{elem.name}> class={elem.get('class', [])} text='{text}'")

        # Look for match containers
        print("\n--- Match Container Analysis ---")
        # Find elements that might be individual matches
        match_containers = soup.select("[class*='match'], [class*='fixture'], [class*='tie']")
        if match_containers:
            print(f"Found {len(match_containers)} potential match containers")
            # Analyze first match in detail
            first_match = match_containers[0]
            print(f"\nFirst match container structure:")
            print(f"  Tag: <{first_match.name}>")
            print(f"  Classes: {first_match.get('class', [])}")
            print(f"  Direct children:")
            for child in first_match.children:
                if hasattr(child, 'name') and child.name:
                    child_classes = child.get('class', [])
                    child_text = child.get_text(strip=True)[:50] if child.get_text(strip=True) else ""
                    print(f"    <{child.name}> class={child_classes[:3]} text='{child_text}'")

        # Look for winner indicators
        print("\n--- Winner Indicators ---")
        winner_patterns = soup.select("[class*='winner'], [class*='won'], [class*='active']")
        print(f"Found {len(winner_patterns)} potential winner indicators")

        await browser.close()

        print("\n" + "=" * 80)
        print("EXPLORATION COMPLETE")
        print(f"Review the saved HTML at: {html_path}")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(explore_draws_page())
