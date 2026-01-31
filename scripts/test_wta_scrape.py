#!/usr/bin/env python3
"""
WTA Scraper Test Script.

Tests WTA tournament list and match scraping with detailed logging.
NO database writes — just prints parsed data for verification.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate

    # Test tournament list for 2024
    python scripts/test_wta_scrape.py --tournaments 2024

    # Test match scraping for Australian Open 2024
    python scripts/test_wta_scrape.py --matches 901 australian-open 2024

    # Both
    python scripts/test_wta_scrape.py --tournaments 2024 --matches 901 australian-open 2024
"""

import argparse
import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bs4 import BeautifulSoup
from teelo.scrape.base import BaseScraper, ScrapedMatch, ScrapedFixture
from teelo.scrape.parsers.score import parse_score, ScoreParseError
from teelo.scrape.parsers.player import extract_seed_from_name
from typing import AsyncGenerator


class WTATestScraper(BaseScraper):
    """
    WTA scraper for testing — implements the actual scraping logic
    discovered from website exploration.

    URL patterns:
    - Tournament calendar: /tournaments?year=YYYY&status=all
    - Tournament page: /tournaments/{number}/{slug}/{year}
    - Draws page: /tournaments/{number}/{slug}/{year}/draws

    Match data is in the draws page, structured as:
    - table.match-table with class match-table--winner-a or winner-b
    - Each table has 2 rows (tr.match-table__row)
    - Player links: a[href*='/players/{id}/{slug}']
    - Scores: td.match-table__score-cell with is-winner class
    - Tiebreaks: sup.match-table__tie-break inside score cell
    - Nationality: div.match-table__player-flag--{IOC}
    - Rounds: h2.tournament-draw__round-title
    """

    BASE_URL = "https://www.wtatennis.com"

    # Round title mapping from WTA display text to our standard codes
    ROUND_MAP = {
        "round of 128": "R128",
        "round of 64": "R64",
        "round of 32": "R32",
        "round of 16": "R16",
        "quarterfinals": "QF",
        "quarterfinal": "QF",
        "semifinals": "SF",
        "semifinal": "SF",
        "final": "F",
        "finals": "F",
        # Short codes already used in nav
        "r128": "R128",
        "r64": "R64",
        "r32": "R32",
        "r16": "R16",
        "qf": "QF",
        "sf": "SF",
        "f": "F",
    }

    async def get_tournament_list(self, year: int) -> list[dict]:
        """
        Scrape the WTA tournament calendar for a given year.

        URL: /tournaments?year={year}&status=all

        Each tournament is an <li class="tournament-list__item"> with:
        - data-fav-id: Tournament number (used in URLs)
        - data-fav-name: Official tournament name
        - Level image alt text: "Grand Slam", "WTA 1000", "WTA 500", "WTA 250"
        - Surface text element
        - Location text element
        - date-time attributes on <time> elements

        Returns list of tournament dicts with: id, number, name, slug, level,
        surface, location, start_date, year
        """
        page = await self.new_page()
        tournaments = []

        try:
            url = f"{self.BASE_URL}/tournaments?year={year}&status=all"
            print(f"  Loading tournament calendar: {url}")
            await self.navigate(page, url, wait_for="domcontentloaded")
            await asyncio.sleep(3)

            # Dismiss cookie consent
            await self._dismiss_cookies(page)
            await asyncio.sleep(2)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Each tournament is a list item in the calendar
            cards = soup.select("li.tournament-list__item")
            print(f"  Found {len(cards)} tournament cards")

            seen_keys = set()  # Deduplicate (same tournament can appear twice for Dec/Jan)

            for card in cards:
                try:
                    tournament = self._parse_tournament_card(card, year)
                    if tournament:
                        # Deduplicate by number+year combo
                        key = f"{tournament['number']}_{tournament['year']}"
                        if key not in seen_keys:
                            seen_keys.add(key)
                            tournaments.append(tournament)
                except Exception as e:
                    print(f"  Warning: Error parsing tournament card: {e}")
                    continue

        finally:
            await page.close()

        return tournaments

    def _parse_tournament_card(self, card, calendar_year: int) -> dict | None:
        """
        Parse a single tournament card from the calendar page.

        Args:
            card: BeautifulSoup <li> element
            calendar_year: The year we're browsing the calendar for

        Returns:
            Tournament dict or None if parsing fails
        """
        # Tournament number and name from data attributes
        number = card.get("data-fav-id", "")
        name = card.get("data-fav-name", "")
        if not number or not name:
            return None

        # Extract slug and year from the tournament link
        link = card.select_one("a[href*='/tournaments/']")
        if not link:
            return None

        href = link.get("href", "")
        match = re.search(r'/tournaments/(\d+)/([^/]+)/(\d+)', href)
        if not match:
            return None

        slug = match.group(2)
        link_year = int(match.group(3))

        # Only include tournaments for the requested calendar year
        # The link year is the "season year" which matches the calendar year
        # But some tournaments at year boundaries appear in adjacent years
        if link_year != calendar_year:
            return None

        # Level from the badge/tag image alt text
        level_img = card.select_one("img[alt]")
        level = "WTA 250"  # Default
        if level_img:
            alt = level_img.get("alt", "").strip()
            if alt:
                level = alt  # "Grand Slam", "WTA 1000", "WTA 500", "WTA 250"

        # Surface
        surface_elem = card.select_one("[class*='surface']")
        surface = surface_elem.get_text(strip=True) if surface_elem else "Hard"
        surface = self._normalize_surface(surface)

        # Location (city and country run together, e.g. "MELBOURNEAUSTRALIA")
        location = ""
        location_elem = card.select_one("[class*='location']")
        if location_elem:
            location = location_elem.get_text(strip=True)

        # Start date from first <time> element
        start_date = None
        time_elem = card.select_one("time[date-time]")
        if time_elem:
            start_date = time_elem.get("date-time", "")

        return {
            "id": slug,
            "number": number,
            "name": name,
            "slug": slug,
            "level": level,
            "surface": surface,
            "location": location,
            "start_date": start_date,
            "year": link_year,
        }

    async def scrape_tournament_results(
        self,
        tournament_id: str,
        year: int,
        tournament_number: str = None,
        include_qualifying: bool = False,
    ) -> AsyncGenerator[ScrapedMatch, None]:
        """
        Scrape all completed singles match results from the draws page.

        URL: /tournaments/{number}/{slug}/{year}/draws

        The draws page contains all rounds in a single HTML page.
        Matches are in <table class="match-table"> elements.
        Rounds are separated by <h2 class="tournament-draw__round-title">.

        Only scrapes singles (LS) draw, not doubles (LD).

        Args:
            tournament_id: Tournament slug (e.g. "australian-open")
            year: Season year
            tournament_number: WTA tournament number (e.g. "901")
            include_qualifying: Not yet supported for WTA
        """
        if not tournament_number:
            print(f"  ERROR: tournament_number required for WTA scraper")
            return

        page = await self.new_page()
        try:
            url = f"{self.BASE_URL}/tournaments/{tournament_number}/{tournament_id}/{year}/draws"
            print(f"  Loading draws page: {url}")
            await self.navigate(page, url, wait_for="domcontentloaded")
            await asyncio.sleep(5)

            await self._dismiss_cookies(page)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # The draws page has singles (data-event-type="LS") and doubles
            # (data-event-type="LD") in separate container divs.
            # We only scrape singles.
            singles_container = soup.select_one("[data-event-type='LS']")
            if not singles_container:
                print(f"  ERROR: Could not find singles draw container")
                return

            round_titles = singles_container.select("h2.tournament-draw__round-title")
            all_tables = singles_container.select("table.match-table")

            print(f"  Singles draw: {len(round_titles)} round headers, {len(all_tables)} match tables")

            # Build list of (round_title_element, round_code) pairs
            round_title_pairs = []
            for rt in round_titles:
                text = rt.get_text(strip=True).lower()
                round_code = self.ROUND_MAP.get(text, text.upper())
                round_title_pairs.append((rt, round_code))

            print(f"  Rounds: {[rc for _, rc in round_title_pairs]}")

            # For each round, find match tables between this round title and the next.
            match_number = 0

            for idx, (round_elem, round_code) in enumerate(round_title_pairs):
                next_round_elem = (
                    round_title_pairs[idx + 1][0]
                    if idx + 1 < len(round_title_pairs)
                    else None
                )

                round_tables = self._tables_between(
                    all_tables, round_elem, next_round_elem, singles_container
                )

                print(f"  {round_code}: {len(round_tables)} matches")

                for table in round_tables:
                    try:
                        match = self._parse_match_table(
                            table, tournament_id, tournament_number, year, round_code, match_number
                        )
                        if match:
                            match_number += 1
                            yield match
                    except Exception as e:
                        print(f"    Warning: Error parsing match: {e}")
                        continue

        finally:
            await page.close()

    def _tables_between(self, all_tables, start_elem, end_elem, soup) -> list:
        """
        Find all match tables that appear between two elements in the DOM.

        Uses source position comparison via the document's element order.
        A table belongs to a round if it comes after the round title and
        before the next round title in document order.
        """
        # Get positions of all elements by traversing the full document
        # This is O(n) but only done once per page
        all_elements = list(soup.descendants)

        try:
            start_pos = all_elements.index(start_elem)
        except ValueError:
            return []

        if end_elem:
            try:
                end_pos = all_elements.index(end_elem)
            except ValueError:
                end_pos = len(all_elements)
        else:
            end_pos = len(all_elements)

        result = []
        for table in all_tables:
            try:
                table_pos = all_elements.index(table)
                if start_pos < table_pos < end_pos:
                    result.append(table)
            except ValueError:
                continue

        return result

    def _parse_match_table(
        self,
        table,
        tournament_id: str,
        tournament_number: str,
        year: int,
        round_code: str,
        match_number: int,
    ) -> ScrapedMatch | None:
        """
        Parse a single <table class="match-table"> element into a ScrapedMatch.

        Table structure:
        - Class "match-table--winner-a" or "match-table--winner-b" indicates winner
        - Two <tr> rows: team A (row 0) and team B (row 1)
        - Player link: a.match-table__player--link with href /players/{id}/{slug}
        - Score cells: td.match-table__score-cell (3-5 per row for sets)
        - Tiebreak: <sup class="match-table__tie-break"> inside score cell
        - "is-winner" class on score cell = that player won the set
        - Dot placeholder: span.match-table__dot means set not played
        - Nationality: div.match-table__player-flag--{IOC}
        - Seed: div.match-table__seed or embedded in player text like "(1)"
        """
        table_classes = " ".join(table.get("class", []))

        rows = table.select("tr.match-table__row")
        if len(rows) < 2:
            return None

        row_a, row_b = rows[0], rows[1]

        # Extract player info from each row
        player_a = self._extract_player_from_row(row_a)
        player_b = self._extract_player_from_row(row_b)

        if not player_a or not player_b:
            return None

        # Skip byes
        if player_a["name"].lower() == "bye" or player_b["name"].lower() == "bye":
            return None

        # Extract scores from each row
        scores_a = self._extract_scores_from_row(row_a)
        scores_b = self._extract_scores_from_row(row_b)

        # Build score string in standard format (e.g. "7-6(2) 6-2")
        score_raw = self._build_score_string(scores_a, scores_b)

        # Clean player names — remove seed from display name
        name_a, seed_a = extract_seed_from_name(player_a["name"])
        name_b, seed_b = extract_seed_from_name(player_b["name"])

        # Determine winner from table class
        if "match-table--winner-a" in table_classes:
            winner_name = name_a
        elif "match-table--winner-b" in table_classes:
            winner_name = name_b
        else:
            winner_name = name_a  # Default to A

        # Determine match status
        status = "completed"
        if not score_raw or score_raw.strip() == "":
            status = "walkover"
            score_raw = "W/O"

        # Generate external ID for deduplication
        # Format: YYYY_TOURNEY_ROUND_WTAID1_WTAID2 (sorted for consistency)
        id_a = player_a["wta_id"] or name_a.lower().replace(" ", "-")
        id_b = player_b["wta_id"] or name_b.lower().replace(" ", "-")
        sorted_ids = sorted([id_a, id_b])
        external_id = f"{year}_{tournament_id}_{round_code}_{sorted_ids[0]}_{sorted_ids[1]}"

        # Get tournament info
        tournament_name = tournament_id.replace("-", " ").title()

        return ScrapedMatch(
            external_id=external_id,
            source="wta",
            tournament_name=tournament_name,
            tournament_id=tournament_id,
            tournament_year=year,
            tournament_level="",  # Will be set from tournament list data
            tournament_surface="",  # Will be set from tournament list data
            round=round_code,
            match_number=match_number,
            player_a_name=name_a,
            player_a_external_id=player_a["wta_id"],
            player_a_seed=seed_a,
            player_a_nationality=player_a["nationality"],
            player_b_name=name_b,
            player_b_external_id=player_b["wta_id"],
            player_b_seed=seed_b,
            player_b_nationality=player_b["nationality"],
            winner_name=winner_name,
            score_raw=score_raw,
            status=status,
        )

    def _extract_player_from_row(self, row) -> dict | None:
        """
        Extract player info from a match table row.

        Returns dict with: name, wta_id, nationality
        """
        # Player link: a.match-table__player--link or a[href*='/players/']
        link = row.select_one("a.match-table__player--link") or row.select_one("a[href*='/players/']")
        if not link:
            return None

        name = link.get_text(strip=True)
        if not name:
            return None

        # Extract WTA ID from href: /players/{id}/{slug}
        href = link.get("href", "")
        wta_id = None
        id_match = re.search(r"/players/(\d+)/", href)
        if id_match:
            wta_id = id_match.group(1)

        # Nationality from flag class: match-table__player-flag--{IOC}
        nationality = None
        flag = row.select_one("[class*='match-table__player-flag--']")
        if flag:
            flag_classes = flag.get("class", [])
            for cls in flag_classes:
                nat_match = re.search(r"match-table__player-flag--(\w+)", cls)
                if nat_match:
                    nationality = nat_match.group(1).upper()
                    break

        return {"name": name, "wta_id": wta_id, "nationality": nationality}

    def _extract_scores_from_row(self, row) -> list[dict]:
        """
        Extract set scores from a match table row.

        Each score cell is td.match-table__score-cell.
        Cells with span.match-table__dot are unplayed sets (skip).
        Tiebreak score in <sup class="match-table__tie-break">.
        "is-winner" class means this player won the set.

        Returns list of dicts: [{games: int, tiebreak: int|None, is_winner: bool}]
        """
        scores = []
        cells = row.select("td.match-table__score-cell")

        for cell in cells:
            # Skip dot placeholders (unplayed sets)
            dot = cell.select_one("span.match-table__dot")
            if dot:
                continue

            # Get the raw text (may include tiebreak number appended)
            cell_text = cell.get_text(strip=True)
            if not cell_text or cell_text == ".":
                continue

            # Check for tiebreak
            tiebreak = None
            tb_elem = cell.select_one("sup.match-table__tie-break")
            if tb_elem:
                try:
                    tiebreak = int(tb_elem.get_text(strip=True))
                except ValueError:
                    pass
                # Remove tiebreak from games count
                # The cell text looks like "62" where 6 is games and 2 is tiebreak
                games_text = cell_text.replace(tb_elem.get_text(strip=True), "").strip()
            else:
                games_text = cell_text

            try:
                games = int(games_text)
            except ValueError:
                continue

            is_winner = "is-winner" in " ".join(cell.get("class", []))

            scores.append({
                "games": games,
                "tiebreak": tiebreak,
                "is_winner": is_winner,
            })

        return scores

    def _build_score_string(self, scores_a: list[dict], scores_b: list[dict]) -> str:
        """
        Build a standard score string from parsed set scores.

        Format: "6-4 7-6(2) 6-3" (from winner's perspective, A is listed first)
        Tiebreak shown in parentheses with the loser's tiebreak score.
        """
        if not scores_a or not scores_b:
            return ""

        parts = []
        for sa, sb in zip(scores_a, scores_b):
            set_str = f"{sa['games']}-{sb['games']}"

            # Add tiebreak if present (show loser's tiebreak score)
            if sa["tiebreak"] is not None:
                set_str += f"({sa['tiebreak']})"
            elif sb["tiebreak"] is not None:
                set_str += f"({sb['tiebreak']})"

            parts.append(set_str)

        return " ".join(parts)

    async def scrape_fixtures(self, tournament_id: str) -> AsyncGenerator[ScrapedFixture, None]:
        """Not implemented for testing."""
        return
        yield

    async def _dismiss_cookies(self, page):
        """Dismiss cookie consent popup if present."""
        try:
            btn = await page.query_selector("#onetrust-accept-btn-handler")
            if btn and await btn.is_visible():
                await btn.click()
                await asyncio.sleep(1)
        except Exception:
            pass


# ==============================================================
# Test runner functions
# ==============================================================

async def test_tournament_list(year: int):
    """Test tournament list scraping and print results."""
    print("=" * 70)
    print(f"TEST: WTA Tournament List for {year}")
    print("=" * 70)

    async with WTATestScraper(headless=False) as scraper:
        tournaments = await scraper.get_tournament_list(year)

    print(f"\nFound {len(tournaments)} tournaments:\n")
    print(f"{'#':<6} {'Slug':<25} {'Name':<45} {'Level':<15} {'Surface':<10} {'Date':<12}")
    print("-" * 120)

    for t in tournaments:
        print(f"{t['number']:<6} {t['slug']:<25} {t['name'][:44]:<45} {t['level']:<15} {t['surface']:<10} {t.get('start_date', 'N/A'):<12}")

    # Summary by level
    print(f"\nSummary by level:")
    from collections import Counter
    levels = Counter(t["level"] for t in tournaments)
    for level, count in levels.most_common():
        print(f"  {level}: {count}")


async def test_match_scraping(tournament_number: str, tournament_slug: str, year: int):
    """Test match scraping for a single tournament and print results."""
    print("=" * 70)
    print(f"TEST: WTA Match Scraping - {tournament_slug} {year} (#{tournament_number})")
    print("=" * 70)

    matches = []
    score_errors = []

    async with WTATestScraper(headless=False) as scraper:
        async for match in scraper.scrape_tournament_results(
            tournament_slug, year, tournament_number=tournament_number
        ):
            matches.append(match)

    print(f"\nScraped {len(matches)} matches:\n")

    # Group by round for display
    from collections import defaultdict
    by_round = defaultdict(list)
    for m in matches:
        by_round[m.round].append(m)

    round_order = ["R128", "R64", "R32", "R16", "QF", "SF", "F"]
    for round_code in round_order:
        round_matches = by_round.get(round_code, [])
        if not round_matches:
            continue

        print(f"\n  --- {round_code} ({len(round_matches)} matches) ---")
        for m in round_matches:
            # Try parsing the score
            score_ok = ""
            try:
                parsed = parse_score(m.score_raw)
                score_ok = "OK"
            except ScoreParseError as e:
                score_ok = f"PARSE_ERR: {e}"
                score_errors.append((m.score_raw, str(e)))

            seed_a = f"({m.player_a_seed})" if m.player_a_seed else ""
            seed_b = f"({m.player_b_seed})" if m.player_b_seed else ""
            winner_marker = " <-W" if m.winner_name == m.player_a_name else ""
            loser_marker = " <-W" if m.winner_name == m.player_b_name else ""

            print(f"    {m.player_a_name}{seed_a} [{m.player_a_external_id}]{winner_marker}")
            print(f"      vs {m.player_b_name}{seed_b} [{m.player_b_external_id}]{loser_marker}")
            print(f"      Score: {m.score_raw}  [{score_ok}]  Status: {m.status}")
            print(f"      Nat: {m.player_a_nationality} vs {m.player_b_nationality}")
            print(f"      ExtID: {m.external_id}")

    # Summary
    print(f"\n{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"  Total matches: {len(matches)}")
    print(f"  By round: {dict(sorted(((k, len(v)) for k, v in by_round.items()), key=lambda x: round_order.index(x[0]) if x[0] in round_order else 99))}")
    print(f"  Score parse errors: {len(score_errors)}")
    for raw, err in score_errors:
        print(f"    '{raw}': {err}")

    # Spot check: AO 2024 final
    finals = by_round.get("F", [])
    if finals:
        f = finals[0]
        print(f"\n  Final: {f.player_a_name} vs {f.player_b_name} -> {f.score_raw}")
        print(f"  Winner: {f.winner_name}")


async def main():
    parser = argparse.ArgumentParser(description="Test WTA scraper")
    parser.add_argument("--tournaments", type=int, help="Test tournament list for given year")
    parser.add_argument("--matches", nargs=3, metavar=("NUMBER", "SLUG", "YEAR"),
                       help="Test match scraping: tournament_number slug year")
    args = parser.parse_args()

    if not args.tournaments and not args.matches:
        # Default: test both with AO 2024
        await test_tournament_list(2024)
        print("\n\n")
        await test_match_scraping("901", "australian-open", 2024)
    else:
        if args.tournaments:
            await test_tournament_list(args.tournaments)
        if args.matches:
            await test_match_scraping(args.matches[0], args.matches[1], int(args.matches[2]))


if __name__ == "__main__":
    asyncio.run(main())
