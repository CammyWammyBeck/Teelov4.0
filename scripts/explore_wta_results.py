#!/usr/bin/env python3
"""
Explore WTA tournament results/scores page structure.

Navigates to https://www.wtatennis.com/tournaments/2088/abu-dhabi/2026/scores
and analyzes the HTML structure to understand how to scrape results data.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/explore_wta_results.py
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


async def explore_results_page():
    """Navigate to WTA scores/results page and analyze HTML structure."""

    url = "https://www.wtatennis.com/tournaments/2088/abu-dhabi/2026/scores"

    print("=" * 80)
    print("WTA Tournament Results/Scores Page Explorer")
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

        # Try to wait for match elements
        try:
            await page.wait_for_selector("[class*='match'], [class*='score'], [class*='result']", timeout=15000)
        except Exception as e:
            print(f"Warning: Timeout waiting for match elements: {e}")

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # Save HTML for manual inspection
        html_path = Path(__file__).parent.parent / "scratchpad" / "wta_results.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(html)
        print(f"\nSaved HTML to: {html_path}")

        print("\n" + "=" * 80)
        print("HTML STRUCTURE ANALYSIS")
        print("=" * 80)

        # Look for day selector buttons (similar to schedule page)
        print("\n--- Day Selector Buttons ---")
        day_buttons = soup.select("button, [role='tab'], [class*='day'], [class*='date']")
        day_related = []
        for btn in day_buttons:
            text = btn.get_text(strip=True)
            if text and (
                any(x in text.lower() for x in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'day', 'all']) or
                re.search(r'\d{1,2}', text)
            ):
                day_related.append((btn.name, btn.get('class', [])[:3], text))
        for tag, classes, text in day_related[:10]:
            print(f"  <{tag}> class={classes} text='{text}'")

        # Look for round selector/filter
        print("\n--- Round Filter Elements ---")
        round_filters = soup.select("[class*='filter'], [class*='round'], select, [role='listbox']")
        for elem in round_filters[:10]:
            text = elem.get_text(strip=True)[:50]
            if 'round' in text.lower() or 'final' in text.lower():
                print(f"  <{elem.name}> class={elem.get('class', [])[:3]} text='{text}'")

        # Look for match containers
        print("\n--- Match Containers ---")
        selectors_to_try = [
            ("[class*='match']", "elements with 'match' in class"),
            ("[class*='score']", "elements with 'score' in class"),
            ("[class*='result']", "elements with 'result' in class"),
            ("[class*='fixture']", "elements with 'fixture' in class"),
            ("[class*='completed']", "elements with 'completed' in class"),
        ]

        for selector, description in selectors_to_try:
            elements = soup.select(selector)
            if elements:
                print(f"\n✓ Found {len(elements)} {description}")
                class_patterns = {}
                for elem in elements:
                    classes = " ".join(elem.get('class', []))
                    class_patterns[classes] = class_patterns.get(classes, 0) + 1
                sorted_patterns = sorted(class_patterns.items(), key=lambda x: -x[1])
                for pattern, count in sorted_patterns[:5]:
                    print(f"    ({count}x) {pattern[:70]}")

        # Look for player elements
        print("\n--- Player Elements ---")
        player_links = soup.select("a[href*='/players/']")
        print(f"Found {len(player_links)} player links")
        # Analyze unique player ID patterns
        player_ids = set()
        for link in player_links:
            href = link.get("href", "")
            id_match = re.search(r"/players/(\d+)/", href)
            if id_match:
                player_ids.add(id_match.group(1))
        print(f"Unique player IDs found: {len(player_ids)}")
        for link in player_links[:5]:
            href = link.get("href", "")
            name = link.get_text(strip=True)
            print(f"  '{name}' -> {href[:60]}...")

        # Look for score elements in detail
        print("\n--- Score Element Analysis ---")
        score_elements = soup.select("[class*='score'], [class*='set'], [class*='game']")
        print(f"Found {len(score_elements)} score-related elements")
        # Find elements with numeric content
        numeric_scores = []
        for elem in score_elements:
            text = elem.get_text(strip=True)
            if re.match(r'^[\d\-\(\)]+$', text.replace(' ', '')):
                numeric_scores.append((elem.name, elem.get('class', []), text))
        print(f"Elements with numeric scores: {len(numeric_scores)}")
        for tag, classes, text in numeric_scores[:10]:
            print(f"  <{tag}> class={classes[:3]} text='{text}'")

        # Look for winner indicators
        print("\n--- Winner Indicators ---")
        winner_patterns = [
            "[class*='winner']",
            "[class*='won']",
            "[class*='victory']",
            "[class*='bold']",  # Often winners are bolded
        ]
        for pattern in winner_patterns:
            elements = soup.select(pattern)
            if elements:
                print(f"  {pattern}: {len(elements)} elements")

        # Look for round headers
        print("\n--- Round Headers ---")
        round_texts = soup.find_all(string=re.compile(r'(final|semi|quarter|round|r\d+)', re.I))
        seen_rounds = set()
        for elem in round_texts:
            text = str(elem).strip()
            parent = elem.parent
            if text and len(text) < 40:
                parent_class = parent.get('class', []) if parent else []
                if text.lower() not in seen_rounds:
                    seen_rounds.add(text.lower())
                    print(f"  '{text}' in <{parent.name if parent else '?'}> class={parent_class[:2]}")

        # Analyze a single completed match in detail
        print("\n--- Detailed Match Analysis ---")
        match_containers = soup.select("[class*='match']")
        if match_containers:
            for container in match_containers:
                players = container.select("a[href*='/players/']")
                scores = container.select("[class*='score']")
                if len(players) >= 2:
                    print(f"\nMatch with {len(players)} players and {len(scores)} score elements:")
                    print(f"  Container: <{container.name}> class={container.get('class', [])}")
                    print(f"  Full text preview: '{container.get_text(strip=True)[:150]}'")

                    # Show structure
                    print(f"\n  Key elements:")
                    for player in players[:2]:
                        print(f"    Player: '{player.get_text(strip=True)}' href={player.get('href', '')[:50]}")
                    for score in scores[:6]:
                        print(f"    Score: <{score.name}> class={score.get('class', [])[:2]} text='{score.get_text(strip=True)}'")
                    break

        # Look for match status (completed, live, upcoming)
        print("\n--- Match Status Elements ---")
        status_patterns = soup.select("[class*='status'], [class*='state'], [class*='live']")
        print(f"Found {len(status_patterns)} status-related elements")
        for elem in status_patterns[:5]:
            text = elem.get_text(strip=True)
            print(f"  <{elem.name}> class={elem.get('class', [])[:3]} text='{text}'")

        await browser.close()

        print("\n" + "=" * 80)
        print("EXPLORATION COMPLETE")
        print(f"Review the saved HTML at: {html_path}")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(explore_results_page())
