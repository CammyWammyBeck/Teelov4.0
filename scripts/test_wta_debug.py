#!/usr/bin/env python3
"""
Debug script: verify WTA scores page structure with singles tab selected.
"""

import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bs4 import BeautifulSoup
from teelo.scrape.wta import WTAScraper

SCRATCHPAD = Path("/tmp/claude-1000/-home-cammybeck-Documents-programming-Teelov4-0/"
                  "9b75abc2-27bf-453a-9fd1-df117738f80d/scratchpad")
SCRATCHPAD.mkdir(parents=True, exist_ok=True)


async def debug_scrape():
    tournament_number = "901"
    tournament_id = "australian-open"
    year = 2024

    async with WTAScraper(headless=False) as scraper:
        page = await scraper.new_page()
        try:
            url = f"{scraper.BASE_URL}/tournaments/{tournament_number}/{tournament_id}/{year}/scores"
            print(f"Loading: {url}")
            await scraper.navigate(page, url, wait_for="domcontentloaded")
            await asyncio.sleep(5)
            await scraper._dismiss_cookies(page)

            # Click a day with lots of matches (day 5 = early main draw)
            buttons = await page.query_selector_all("button.day-navigation__button")
            day_idx = 5
            date_str = await buttons[day_idx].get_attribute("data-date")
            print(f"Clicking day {day_idx}: {date_str}")
            await buttons[day_idx].click()
            await asyncio.sleep(3)

            # Now click singles tab
            print("Clicking singles tab...")
            await scraper._select_singles_tab(page)
            await asyncio.sleep(3)

            html = await page.content()
            (SCRATCHPAD / f"wta_singles_{date_str}.html").write_text(html)

            soup = BeautifulSoup(html, "lxml")

            # Check active tab
            tabs = soup.select("li.js-type-filter")
            for t in tabs:
                dtype = t.get("data-type", "?")
                active = "is-active" in " ".join(t.get("class", []))
                print(f"  Tab data-type={dtype} active={active}")

            # Find top-level tennis-match divs (not child elements)
            all_tm = soup.find_all("div", class_=re.compile(r"^tennis-match\b"))
            # Filter to only top-level (class is exactly "tennis-match ...")
            top_level = [el for el in all_tm
                         if "tennis-match__" not in " ".join(el.get("class", []))]
            print(f"\nTop-level tennis-match divs: {len(top_level)}")

            ls_count = 0
            ld_count = 0
            neither = 0
            for el in top_level:
                cls = " ".join(el.get("class", []))
                if "-LS" in cls:
                    ls_count += 1
                elif "-LD" in cls:
                    ld_count += 1
                else:
                    neither += 1
                    print(f"  Neither LS nor LD: {cls[:100]}")

            print(f"  -LS (singles): {ls_count}")
            print(f"  -LD (doubles): {ld_count}")
            print(f"  Neither: {neither}")

            # Show first few matches
            for el in top_level[:3]:
                cls = " ".join(el.get("class", []))
                round_el = el.select_one(".tennis-match__round")
                rnd = round_el.get_text(strip=True) if round_el else "?"
                table = el.select_one("table.match-table")
                has_table = table is not None
                # Get player names
                links = el.select("a.match-table__player--link")
                names = [l.get_text(strip=True) for l in links[:2]]
                print(f"  {rnd} | {' vs '.join(names)} | has_table={has_table}")
                print(f"    class={cls[:100]}")

        finally:
            await page.close()


def main():
    asyncio.run(debug_scrape())


if __name__ == "__main__":
    main()
