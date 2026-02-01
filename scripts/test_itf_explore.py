#!/usr/bin/env python3
"""
Test script: Explore ITF tournament draw page HTML structure.

Navigates to an ITF tournament draw page, captures HTML at each
navigation step (initial + next button clicks), and saves to files
for inspection.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/test_itf_explore.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.scrape.base import BaseScraper

# A completed ITF tournament to test with
# M15 Monastir is a common ITF tournament with plenty of data
TEST_URL = "https://www.itftennis.com/en/tournament/m15-monastir/tun/2024/m-itf-tun-01a-2024/draws-and-results/"

OUTPUT_DIR = Path(__file__).parent / "itf_html_dumps"


async def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    async with ITFExplorer() as scraper:
        page = await scraper.new_page()

        print(f"Navigating to {TEST_URL}")
        await scraper.navigate(page, TEST_URL, wait_for="domcontentloaded")
        await asyncio.sleep(3)

        # Accept cookies
        try:
            btn = await page.wait_for_selector("#onetrust-accept-btn-handler", timeout=5000)
            if btn:
                await btn.click()
                print("Accepted cookies")
                await asyncio.sleep(1)
        except:
            print("No cookie popup")

        # Save initial view
        html = await page.content()
        (OUTPUT_DIR / "view_0_initial.html").write_text(html)
        print(f"Saved view 0 (initial) - {len(html)} chars")

        # Try clicking next button up to 5 times
        for i in range(1, 6):
            try:
                # Try various next button selectors
                next_btn = None
                for selector in [
                    "button.btn--chevron-next",
                    ".carousel__nav-btn--next",
                    "button[aria-label='Next']",
                    ".drawsheet-round-navigation__next",
                    "button.next",
                    # Generic next arrows
                    "button:has(svg.icon--chevron-right)",
                    ".btn--next",
                ]:
                    try:
                        next_btn = await page.wait_for_selector(selector, timeout=2000)
                        if next_btn and await next_btn.is_visible():
                            print(f"  Found next button with selector: {selector}")
                            break
                        next_btn = None
                    except:
                        continue

                if not next_btn:
                    print(f"  No next button found at step {i}")
                    break

                await next_btn.click()
                await asyncio.sleep(2)

                html = await page.content()
                (OUTPUT_DIR / f"view_{i}_next.html").write_text(html)
                print(f"Saved view {i} - {len(html)} chars")

            except Exception as e:
                print(f"  Error at step {i}: {e}")
                break

        # Also take a screenshot for visual reference
        await page.screenshot(path=str(OUTPUT_DIR / "draw_page.png"), full_page=True)
        print("Saved screenshot")

        await page.close()

    print(f"\nHTML files saved to {OUTPUT_DIR}/")
    print("Inspect these to understand the draw page structure.")


class ITFExplorer(BaseScraper):
    """Minimal scraper subclass just for exploring."""
    BASE_URL = "https://www.itftennis.com"

    async def get_tournament_list(self, year):
        return []

    async def scrape_tournament_results(self, tid, year):
        return
        yield

    async def scrape_fixtures(self, tid):
        return
        yield


if __name__ == "__main__":
    asyncio.run(main())
