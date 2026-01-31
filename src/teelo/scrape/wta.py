"""
WTA Tour website scraper.

Scrapes match results and fixtures from wtatennis.com.

URL patterns:
- Tournament calendar: /tournaments?year=YYYY&status=all
- Tournament page: /tournaments/{number}/{slug}/{year}
- Draws page: /tournaments/{number}/{slug}/{year}/draws

The draws page loads all rounds in a single HTML page with separate containers
for singles (data-event-type="LS") and doubles (data-event-type="LD").

Match data structure:
- table.match-table with class match-table--winner-a or match-table--winner-b
- Two <tr> rows per match (team A and team B)
- Player links: a.match-table__player--link with href /players/{id}/{slug}
- Score cells: td.match-table__score-cell with "is-winner" class on set winners
- Tiebreaks: <sup class="match-table__tie-break"> inside the loser's score cell
- Nationality: div.match-table__player-flag--{IOC_CODE}
- Dot placeholders (span.match-table__dot) indicate unplayed sets
"""

import asyncio
import re
from typing import AsyncGenerator, Optional

from bs4 import BeautifulSoup
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout

from teelo.scrape.base import BaseScraper, ScrapedMatch, ScrapedFixture
from teelo.scrape.parsers.score import parse_score, ScoreParseError
from teelo.scrape.parsers.player import extract_seed_from_name


class WTAScraper(BaseScraper):
    """
    Scraper for WTA Tour website (wtatennis.com).

    Handles:
    - Grand Slams (women's draw)
    - WTA 1000 tournaments
    - WTA 500 tournaments
    - WTA 250 tournaments
    - WTA Finals

    Tournament list comes from the calendar page, which provides tournament
    numbers needed for constructing draw URLs. Each tournament card has
    data attributes with the official name, number, and a level badge image.

    Match results come from the draws page, which contains the full bracket
    with scores for all completed matches. Singles and doubles are in separate
    containers identified by data-event-type="LS" and "LD".

    Usage:
        async with WTAScraper() as scraper:
            tournaments = await scraper.get_tournament_list(2024)
            for tournament in tournaments:
                async for match in scraper.scrape_tournament_results(
                    tournament["id"], 2024,
                    tournament_number=tournament["number"]
                ):
                    print(f"{match.winner_name} d. opponent {match.score_raw}")
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
        # Short codes already used in round nav buttons
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
        - data-fav-id: Tournament number (used in draw URLs)
        - data-fav-name: Official tournament name
        - Level image alt text: "Grand Slam", "WTA 1000", "WTA 500", "WTA 250"
        - Surface text in a child element
        - Location text in a child element
        - date-time attributes on <time> elements for start date

        Args:
            year: Year to get tournaments for (e.g., 2024)

        Returns:
            List of tournament dicts with keys:
            id, number, name, slug, level, surface, location, start_date, year
        """
        page = await self.new_page()
        tournaments = []

        try:
            url = f"{self.BASE_URL}/tournaments?year={year}&status=all"
            print(f"Loading WTA tournament calendar: {url}")
            await self.navigate(page, url, wait_for="domcontentloaded")
            await asyncio.sleep(3)

            # Dismiss cookie consent (WTA uses OneTrust)
            await self._dismiss_cookies(page)
            await asyncio.sleep(2)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Each tournament is a list item in the calendar
            cards = soup.select("li.tournament-list__item")
            print(f"Found {len(cards)} tournament cards")

            # Deduplicate — the same tournament can appear twice when it spans
            # Dec/Jan boundaries (e.g., United Cup starting late December)
            seen_keys = set()

            for card in cards:
                try:
                    tournament = self._parse_tournament_card(card, year)
                    if tournament:
                        key = f"{tournament['number']}_{tournament['year']}"
                        if key not in seen_keys:
                            seen_keys.add(key)
                            tournaments.append(tournament)
                except Exception as e:
                    print(f"Warning: Error parsing tournament card: {e}")
                    continue

        finally:
            await page.close()

        print(f"Parsed {len(tournaments)} WTA tournaments for {year}")
        return tournaments

    def _parse_tournament_card(self, card, calendar_year: int) -> Optional[dict]:
        """
        Parse a single tournament card from the calendar page.

        The card is an <li> element with data attributes for the tournament
        number and name, child elements for surface/location/dates, and a
        link to the tournament page containing the slug and season year.

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
        # Link format: //www.wtatennis.com/tournaments/{number}/{slug}/{year}
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
        # (skip tournaments from adjacent years that appear on the page)
        if link_year != calendar_year:
            return None

        # Level from the badge/tag image alt text
        # Images are like: /resources/.../gs-tag.svg alt="Grand Slam"
        #                   /resources/.../1000k-tag.svg alt="WTA 1000"
        level_img = card.select_one("img[alt]")
        level = "WTA 250"  # Default
        if level_img:
            alt = level_img.get("alt", "").strip()
            if alt:
                level = alt

        # Surface (text inside an element with "surface" in its class)
        surface_elem = card.select_one("[class*='surface']")
        surface = surface_elem.get_text(strip=True) if surface_elem else "Hard"
        surface = self._normalize_surface(surface)

        # Location (city and country typically run together, e.g. "MELBOURNEAUSTRALIA")
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
        include_qualifying: bool = False,
        tournament_number: str = None,
    ) -> AsyncGenerator[ScrapedMatch, None]:
        """
        Scrape all completed singles match results from the draws page.

        URL: /tournaments/{number}/{slug}/{year}/draws

        The draws page loads all rounds in a single HTML page. The page has
        separate containers for singles (data-event-type="LS") and doubles
        (data-event-type="LD"). We only scrape the singles container.

        Within the singles container, rounds are separated by
        <h2 class="tournament-draw__round-title"> elements. Match tables
        between consecutive round titles belong to that round.

        Args:
            tournament_id: Tournament slug (e.g. "australian-open")
            year: Season year
            include_qualifying: Not yet supported for WTA (ignored)
            tournament_number: WTA tournament number (e.g. "901") — REQUIRED

        Yields:
            ScrapedMatch objects for each completed singles match
        """
        if not tournament_number:
            print(f"ERROR: tournament_number required for WTA scraper")
            return

        page = await self.new_page()
        try:
            url = f"{self.BASE_URL}/tournaments/{tournament_number}/{tournament_id}/{year}/draws"
            print(f"Loading WTA draws page: {url}")
            await self.navigate(page, url, wait_for="domcontentloaded")
            await asyncio.sleep(5)

            await self._dismiss_cookies(page)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # The draws page has singles and doubles in separate container divs.
            # We only scrape singles (data-event-type="LS").
            singles_container = soup.select_one("[data-event-type='LS']")
            if not singles_container:
                print(f"WARNING: Could not find singles draw container for {tournament_id} {year}")
                return

            round_titles = singles_container.select("h2.tournament-draw__round-title")
            all_tables = singles_container.select("table.match-table")

            print(f"Singles draw: {len(round_titles)} rounds, {len(all_tables)} match tables")

            # Build list of (round_title_element, round_code) pairs
            round_title_pairs = []
            for rt in round_titles:
                text = rt.get_text(strip=True).lower()
                round_code = self.ROUND_MAP.get(text, text.upper())
                round_title_pairs.append((rt, round_code))

            # For each round, find match tables between this title and the next
            match_number = 0

            for idx, (round_elem, round_code) in enumerate(round_title_pairs):
                next_round_elem = (
                    round_title_pairs[idx + 1][0]
                    if idx + 1 < len(round_title_pairs)
                    else None
                )

                # Find tables between this round title and the next
                round_tables = self._tables_between(
                    all_tables, round_elem, next_round_elem, singles_container
                )

                for table in round_tables:
                    try:
                        scraped = self._parse_match_table(
                            table, tournament_id, tournament_number, year, round_code, match_number
                        )
                        if scraped:
                            match_number += 1
                            yield scraped
                    except Exception as e:
                        print(f"Warning: Error parsing WTA match: {e}")
                        continue

            print(f"Scraped {match_number} singles matches from {tournament_id} {year}")

        finally:
            await page.close()

    def _tables_between(self, all_tables, start_elem, end_elem, container) -> list:
        """
        Find all match tables between two elements in the DOM.

        Uses document position comparison by walking the container's
        descendants list. A table belongs to a round if it appears after
        the round's title element and before the next round's title element.

        Args:
            all_tables: List of all match table elements in the container
            start_elem: Round title element (start boundary, inclusive)
            end_elem: Next round title element (end boundary, exclusive), or None
            container: Parent container element to walk

        Returns:
            List of table elements between start and end boundaries
        """
        all_elements = list(container.descendants)

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
    ) -> Optional[ScrapedMatch]:
        """
        Parse a single <table class="match-table"> element into a ScrapedMatch.

        Table structure:
        - Class "match-table--winner-a" or "match-table--winner-b" indicates winner
        - Two <tr> rows: team A (row 0) and team B (row 1)
        - Player link: a.match-table__player--link with href /players/{id}/{slug}
        - Score cells: td.match-table__score-cell (3-5 per row, one per set)
        - Tiebreak: <sup class="match-table__tie-break"> inside the loser's score cell
        - "is-winner" class on score cell = that player won the set
        - Dot placeholder (span.match-table__dot) = set not played
        - Nationality: div.match-table__player-flag--{IOC_CODE}
        - Seed embedded in player text like "(1)" — extracted via extract_seed_from_name

        Args:
            table: BeautifulSoup <table> element
            tournament_id: Tournament slug
            tournament_number: WTA tournament number
            year: Season year
            round_code: Round code (R128, R64, etc.)
            match_number: Sequential match number for ordering

        Returns:
            ScrapedMatch if parsed successfully, None for byes or incomplete data
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
            # No winner indicated — match might be incomplete
            winner_name = name_a

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

        # Tournament name from slug (will be overwritten by backfill with real name)
        tournament_name = tournament_id.replace("-", " ").title()

        return ScrapedMatch(
            external_id=external_id,
            source="wta",
            tournament_name=tournament_name,
            tournament_id=tournament_id,
            tournament_year=year,
            tournament_level="",  # Set by backfill from tournament list data
            tournament_surface="",  # Set by backfill from tournament list data
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

    def _extract_player_from_row(self, row) -> Optional[dict]:
        """
        Extract player info from a match table row.

        Looks for the player link (a.match-table__player--link) to get
        the player name and WTA ID, and the flag element to get nationality.

        Args:
            row: BeautifulSoup <tr> element

        Returns:
            Dict with name, wta_id, nationality — or None if no player found
        """
        # Player link
        link = (
            row.select_one("a.match-table__player--link")
            or row.select_one("a[href*='/players/']")
        )
        if not link:
            return None

        name = link.get_text(strip=True)
        if not name:
            return None

        # Extract WTA ID from href: /players/{numeric_id}/{slug}
        href = link.get("href", "")
        wta_id = None
        id_match = re.search(r"/players/(\d+)/", href)
        if id_match:
            wta_id = id_match.group(1)

        # Nationality from flag class: match-table__player-flag--{IOC_CODE}
        nationality = None
        flag = row.select_one("[class*='match-table__player-flag--']")
        if flag:
            for cls in flag.get("class", []):
                nat_match = re.search(r"match-table__player-flag--(\w+)", cls)
                if nat_match:
                    nationality = nat_match.group(1).upper()
                    break

        return {"name": name, "wta_id": wta_id, "nationality": nationality}

    def _extract_scores_from_row(self, row) -> list[dict]:
        """
        Extract set scores from a match table row.

        Each set is a td.match-table__score-cell. Unplayed sets contain a
        span.match-table__dot (these are skipped). Tiebreak scores are in
        a <sup class="match-table__tie-break"> element inside the cell —
        the cell text concatenates the games and tiebreak (e.g., "62" for
        6 games + tiebreak score 2).

        Args:
            row: BeautifulSoup <tr> element

        Returns:
            List of dicts: [{games: int, tiebreak: int|None, is_winner: bool}]
        """
        scores = []
        cells = row.select("td.match-table__score-cell")

        for cell in cells:
            # Skip dot placeholders (unplayed sets)
            if cell.select_one("span.match-table__dot"):
                continue

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
                # Remove tiebreak digits from games count
                # Cell text looks like "62" where 6=games, 2=tiebreak
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

        Format: "6-4 7-6(2) 6-3" where scores are from player A's perspective.
        Tiebreak is shown in parentheses with the loser's tiebreak score,
        matching standard tennis score notation.

        Args:
            scores_a: Set scores for player A
            scores_b: Set scores for player B

        Returns:
            Score string, or empty string if no scores available
        """
        if not scores_a or not scores_b:
            return ""

        parts = []
        for sa, sb in zip(scores_a, scores_b):
            set_str = f"{sa['games']}-{sb['games']}"

            # Add tiebreak — shown on the loser's side in standard notation
            if sa["tiebreak"] is not None:
                set_str += f"({sa['tiebreak']})"
            elif sb["tiebreak"] is not None:
                set_str += f"({sb['tiebreak']})"

            parts.append(set_str)

        return " ".join(parts)

    async def scrape_fixtures(
        self,
        tournament_id: str,
    ) -> AsyncGenerator[ScrapedFixture, None]:
        """
        Scrape upcoming fixtures for a WTA tournament.

        Not yet implemented — placeholder for future use.

        Args:
            tournament_id: Tournament URL slug

        Yields:
            ScrapedFixture objects for upcoming matches
        """
        # TODO: Implement fixture scraping from order-of-play page
        return
        yield  # Make this a generator

    async def _dismiss_cookies(self, page: Page) -> None:
        """
        Dismiss the OneTrust cookie consent popup if present.

        WTA uses OneTrust for cookie consent. The popup can block
        clicks on other elements if not dismissed.
        """
        try:
            btn = await page.query_selector("#onetrust-accept-btn-handler")
            if btn and await btn.is_visible():
                await btn.click()
                await asyncio.sleep(1)
        except Exception:
            pass


# Convenience functions

async def scrape_wta_tournament(
    tournament_id: str,
    year: int,
    tournament_number: str = None,
) -> list[ScrapedMatch]:
    """
    Convenience function to scrape a single WTA tournament.

    Args:
        tournament_id: Tournament URL slug
        year: Tournament year
        tournament_number: WTA tournament number (required)

    Returns:
        List of ScrapedMatch objects
    """
    matches = []
    async with WTAScraper() as scraper:
        async for match in scraper.scrape_tournament_results(
            tournament_id, year, tournament_number=tournament_number
        ):
            matches.append(match)
    return matches


async def get_wta_tournaments(year: int) -> list[dict]:
    """
    Convenience function to get WTA tournaments for a year.

    Args:
        year: Year to get tournaments for

    Returns:
        List of tournament dictionaries
    """
    async with WTAScraper() as scraper:
        return await scraper.get_tournament_list(year)
