#!/usr/bin/env python3
"""
Explore WTA tournament order of play (schedule) page structure.

Navigates to https://www.wtatennis.com/tournaments/2088/abu-dhabi/2026/order-of-play
and analyzes the HTML structure to understand how to scrape schedule data.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/explore_wta_schedule.py
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


async def explore_schedule_page():
    """Navigate to WTA order of play page and analyze HTML structure."""

    url = "https://www.wtatennis.com/tournaments/2088/abu-dhabi/2026/order-of-play"

    print("=" * 80)
    print("WTA Tournament Order of Play Page Explorer")
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

        # Try to wait for schedule elements
        try:
            await page.wait_for_selector("[class*='schedule'], [class*='match'], [class*='order']", timeout=15000)
        except Exception as e:
            print(f"Warning: Timeout waiting for schedule elements: {e}")

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # Save HTML for manual inspection
        html_path = Path(__file__).parent.parent / "scratchpad" / "wta_schedule.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(html)
        print(f"\nSaved HTML to: {html_path}")

        print("\n" + "=" * 80)
        print("HTML STRUCTURE ANALYSIS")
        print("=" * 80)

        # Look for day selector buttons
        print("\n--- Day Selector Buttons ---")
        day_buttons = soup.select("button, [role='tab'], [class*='day'], [class*='date']")
        for btn in day_buttons[:15]:
            text = btn.get_text(strip=True)
            classes = btn.get('class', [])
            # Filter for day-related buttons
            if text and (
                any(x in text.lower() for x in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'day']) or
                re.search(r'\d{1,2}', text)
            ):
                print(f"  <{btn.name}> class={classes[:3]} text='{text}'")

        # Look for court containers
        print("\n--- Court Containers ---")
        court_patterns = [
            "[class*='court']",
            "[class*='venue']",
            "[class*='stadium']",
            "[class*='arena']",
        ]
        for pattern in court_patterns:
            elements = soup.select(pattern)
            if elements:
                print(f"\n  {pattern}: {len(elements)} elements")
                for elem in elements[:5]:
                    text = elem.get_text(strip=True)[:50]
                    print(f"    <{elem.name}> class={elem.get('class', [])[:3]} text='{text}'")

        # Look for schedule/match containers
        print("\n--- Schedule/Match Containers ---")
        selectors_to_try = [
            ("[class*='schedule']", "elements with 'schedule' in class"),
            ("[class*='order']", "elements with 'order' in class"),
            ("[class*='match']", "elements with 'match' in class"),
            ("[class*='fixture']", "elements with 'fixture' in class"),
            ("[class*='event']", "elements with 'event' in class"),
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
                sorted_patterns = sorted(class_patterns.items(), key=lambda x: -x[1])
                for pattern, count in sorted_patterns[:5]:
                    print(f"    ({count}x) {pattern[:70]}")

        # Look for time elements
        print("\n--- Time Elements ---")
        time_patterns = [
            "time",
            "[class*='time']",
            "[class*='start']",
            "[datetime]",
        ]
        for pattern in time_patterns:
            elements = soup.select(pattern)
            if elements:
                print(f"\n  {pattern}: {len(elements)} elements")
                for elem in elements[:5]:
                    text = elem.get_text(strip=True)
                    datetime_attr = elem.get('datetime', '')
                    print(f"    <{elem.name}> datetime='{datetime_attr}' text='{text}'")

        # Look for player elements
        print("\n--- Player Elements ---")
        player_links = soup.select("a[href*='/players/']")
        print(f"Found {len(player_links)} player links")
        for link in player_links[:5]:
            href = link.get("href", "")
            name = link.get_text(strip=True)
            id_match = re.search(r"/players/(\d+)/", href)
            player_id = id_match.group(1) if id_match else "?"
            print(f"  ID={player_id} Name='{name}'")

        # Look for match type indicators (Singles/Doubles)
        print("\n--- Match Type Indicators ---")
        type_elements = soup.find_all(string=re.compile(r'singles|doubles', re.I))
        print(f"Found {len(type_elements)} singles/doubles text matches")
        for elem in type_elements[:5]:
            parent = elem.parent
            if parent:
                print(f"  Parent: <{parent.name}> class={parent.get('class', [])[:3]} text='{str(elem)[:30]}'")

        # Look for round indicators
        print("\n--- Round Indicators ---")
        round_elements = soup.find_all(string=re.compile(r'final|semi|quarter|round|r\d+', re.I))
        print(f"Found {len(round_elements)} round-related text matches")
        seen_rounds = set()
        for elem in round_elements:
            text = str(elem).strip()
            if text and text not in seen_rounds and len(text) < 30:
                seen_rounds.add(text)
        print(f"Unique round texts: {list(seen_rounds)[:10]}")

        # Analyze a single match entry in detail
        print("\n--- Detailed Match Entry Analysis ---")
        match_containers = soup.select("[class*='match'], [class*='fixture']")
        if match_containers:
            # Find one with player links
            for container in match_containers:
                players = container.select("a[href*='/players/']")
                if len(players) >= 2:
                    print(f"\nMatch container with {len(players)} players:")
                    print(f"  Tag: <{container.name}>")
                    print(f"  Classes: {container.get('class', [])}")
                    print(f"  Full text: '{container.get_text(strip=True)[:100]}'")
                    print(f"\n  Children structure:")
                    for child in container.descendants:
                        if hasattr(child, 'name') and child.name:
                            child_classes = child.get('class', [])
                            child_text = child.get_text(strip=True)[:30] if child.string != child.get_text() else ""
                            if child_classes or child_text:
                                indent = "    "
                                print(f"{indent}<{child.name}> class={child_classes[:2]} text='{child_text}'")
                    break

        # Look for status indicators (Not Before, Live, Completed, etc.)
        print("\n--- Status Indicators ---")
        status_keywords = ['not before', 'live', 'completed', 'finished', 'upcoming', 'in progress']
        for keyword in status_keywords:
            elements = soup.find_all(string=re.compile(keyword, re.I))
            if elements:
                print(f"  '{keyword}': found {len(elements)} matches")

        await browser.close()

        print("\n" + "=" * 80)
        print("EXPLORATION COMPLETE")
        print(f"Review the saved HTML at: {html_path}")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(explore_schedule_page())
