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
import os
import re
from datetime import datetime, timedelta
from typing import AsyncGenerator, Optional

from bs4 import BeautifulSoup
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout

from teelo.scrape.base import BaseScraper, ScrapedMatch, ScrapedFixture, ScrapedDrawEntry
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

    def _generate_external_id(
        self,
        year: int,
        tournament_id: str,
        round_code: str,
        id_a: str,
        id_b: str
    ) -> str:
        """
        Generate a consistent external ID for a match.

        Format: YYYY_TOURNEY_ROUND_ID1_ID2
        IDs are sorted alphabetically to ensure A vs B and B vs A produce the same ID.
        """
        sorted_ids = sorted([str(id_a), str(id_b)])
        return f"{year}_{tournament_id}_{round_code}_{sorted_ids[0]}_{sorted_ids[1]}"

    async def get_tournament_list(self, year: int, tour_type: str = "main") -> list[dict]:
        """
        Scrape the WTA tournament calendar for a given year.

        Fetches from either the main WTA Tour page, the WTA 125 page, or both.
        URLs: 
        - Main: /tournaments?year={year}&status=all
        - 125: /tournaments/wta-125?year={year}&status=all

        Args:
            year: Year to get tournaments for (e.g., 2024)
            tour_type: "main", "125", or "all". Defaults to "main".

        Returns:
            List of tournament dicts with keys:
            id, number, name, slug, level, surface, location, start_date, year
        """
        page = await self.new_page()
        tournaments = []

        try:
            urls = []
            if tour_type in ["main", "all"]:
                urls.append(f"{self.BASE_URL}/tournaments?year={year}&status=all")
            if tour_type in ["125", "all"]:
                urls.append(f"{self.BASE_URL}/tournaments/wta-125?year={year}&status=all")
            
            # Deduplicate — the same tournament can appear twice when it spans
            # Dec/Jan boundaries (e.g., United Cup starting late December)
            seen_keys = set()

            for url in urls:
                print(f"Loading WTA tournament calendar: {url}")
                await self.navigate(page, url, wait_for="domcontentloaded")
                try:
                    await page.wait_for_selector("li.tournament-list__item", timeout=4000)
                except PlaywrightTimeout:
                    pass

                # Dismiss cookie consent (WTA uses OneTrust)
                await self._dismiss_cookies(page)
                try:
                    await page.wait_for_selector("li.tournament-list__item", timeout=4000)
                except PlaywrightTimeout:
                    pass

                html = await page.content()
                soup = BeautifulSoup(html, "lxml")

                # Each tournament is a list item in the calendar
                cards = soup.select("li.tournament-list__item")
                print(f"Found {len(cards)} tournament cards on {url}")

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

        print(f"Parsed {len(tournaments)} total WTA tournaments for {year}")
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
        Scrape all completed singles match results from the scores page.

        URL: /tournaments/{number}/{slug}/{year}/scores

        The scores page groups matches by day. Each day has a navigation button
        (button.day-navigation__button) with a data-date attribute. Clicking a
        button loads that day's matches dynamically via JavaScript.

        Each match is a div.tennis-match element. Singles matches have "-LS" in
        their class (e.g. "js-match-0901-2024-LS12345"), doubles have "-LD".
        The round is in div.tennis-match__round, and the actual match table is
        table.match-table inside the container.

        Falls back to the draws page if no day navigation buttons are found
        (e.g. very old tournaments without a scores page).

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
            # Try scores page first (has per-match dates)
            url = f"{self.BASE_URL}/tournaments/{tournament_number}/{tournament_id}/{year}/scores"
            print(f"Loading WTA scores page: {url}")
            await self.navigate(page, url, wait_for="domcontentloaded")
            try:
                await page.wait_for_selector(
                    "button.day-navigation__button, div.tennis-match",
                    timeout=4000,
                )
            except PlaywrightTimeout:
                pass

            await self._dismiss_cookies(page)
            await self._clear_onetrust_overlays(page)
            await self._clear_onetrust_overlays(page)

            # Find day navigation buttons
            day_buttons = await page.query_selector_all("button.day-navigation__button")
            print(f"Found {len(day_buttons)} day navigation buttons")

            if not day_buttons:
                # Fall back to draws page for older tournaments
                print(f"No day buttons found, falling back to draws page")
                async for match in self._scrape_draws_page(
                    page, tournament_id, year, tournament_number
                ):
                    yield match
                return

            # Collect day info: (date_str, button_index) for each day
            # We'll click each button, wait for content, then parse singles matches
            days = []
            for btn in day_buttons:
                date_str = await btn.get_attribute("data-date")
                title = await btn.get_attribute("title") or ""
                days.append({"date": date_str, "title": title})

            print(f"Days to scrape: {[d['date'] for d in days]}")

            match_number = 0
            seen_external_ids = set()

            # Click the Singles tab once before iterating days.
            # The scores page has a Singles/Doubles filter
            # (li.js-type-filter with data-type="singles" or "doubles").
            # Default may be doubles on some days, so we must select singles.
            await self._select_singles_tab(page)

            for day_idx, day_info in enumerate(days):
                date_str = day_info["date"]  # e.g. "2026-01-11"

                # Click the day button to load its matches
                # Re-query buttons each time since DOM may have changed
                buttons = await page.query_selector_all("button.day-navigation__button")
                if day_idx >= len(buttons):
                    print(f"Warning: day button {day_idx} disappeared, skipping")
                    continue

                await self._clear_onetrust_overlays(page)
                await buttons[day_idx].click(force=True)
                try:
                    await page.wait_for_selector("div.tennis-match", timeout=4000)
                except PlaywrightTimeout:
                    pass

                # Parse the current page content for this day
                html = await page.content()
                day_matches = self._parse_scores_day(
                    html, tournament_id, tournament_number, year, date_str, match_number
                )

                day_count = 0
                for scraped in day_matches:
                    # The DOM accumulates matches from all days, so skip
                    # any we've already yielded from a previous day
                    if scraped.external_id in seen_external_ids:
                        continue
                    seen_external_ids.add(scraped.external_id)
                    match_number += 1
                    scraped.match_number = match_number
                    day_count += 1
                    yield scraped

                print(f"  Day {date_str}: {day_count} singles matches")

            print(f"Scraped {match_number} singles matches from {tournament_id} {year}")

        finally:
            await page.close()

    async def scrape_tournament_draw(
        self,
        tournament_id: str,
        year: int,
        tournament_number: str,
        draw_type: str = "singles",
    ) -> list[ScrapedDrawEntry]:
        """
        Scrape the full draw bracket for a tournament.

        Returns ScrapedDrawEntry objects representing the draw structure, including
        byes and upcoming matches.

        Args:
            tournament_id: Tournament slug
            year: Season year
            tournament_number: WTA tournament number
            draw_type: "singles" (doubles not currently supported)

        Returns:
            List of ScrapedDrawEntry objects
        """
        if draw_type != "singles":
            print(f"Warning: Only singles draws supported for WTA currently")
            return []

        page = await self.new_page()
        entries = []

        try:
            url = f"{self.BASE_URL}/tournaments/{tournament_number}/{tournament_id}/{year}/draws"
            print(f"Loading WTA draw page: {url}")
            await self.navigate(page, url, wait_for="domcontentloaded")
            try:
                await page.wait_for_selector("[data-event-type='LS']", timeout=4000)
            except PlaywrightTimeout:
                pass

            await self._dismiss_cookies(page)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Singles container (data-event-type='LS')
            singles_container = soup.select_one("[data-event-type='LS']")
            if not singles_container:
                print(f"WARNING: Could not find singles draw container for {tournament_id} {year}")
                return []

            # Iterate over round containers to ensure correct grouping
            # Structure: .tournament-draw__round-container contains one round's header and matches
            round_containers = singles_container.select(".tournament-draw__round-container")

            if not round_containers:
                # Fallback to old logic if containers not found (structure changed?)
                print("Warning: No round containers found, falling back to flat parsing")
                round_containers = [singles_container] # Treat whole container as one, will fail round splitting

            print(f"Found {len(round_containers)} round containers")

            for container in round_containers:
                # Find round header
                header = container.select_one("h2.tournament-draw__round-title")
                if not header:
                    continue
                
                text = header.get_text(strip=True).lower()
                round_code = self.ROUND_MAP.get(text, text.upper())
                
                # Find tables in this container
                tables = container.select("table.match-table")
                
                print(f"  Round {round_code}: {len(tables)} matches")

                for pos_idx, table in enumerate(tables):
                    draw_position = pos_idx + 1

                    # Parse using helper
                    entry = self._parse_draw_entry_table(
                        table, tournament_id, year, round_code, draw_position
                    )
                    if entry:
                        entries.append(entry)

        finally:
            await page.close()

        return entries

    def _parse_draw_entry_table(
        self,
        table,
        tournament_id: str,
        year: int,
        round_code: str,
        draw_position: int,
    ) -> Optional[ScrapedDrawEntry]:
        """
        Parse a single match table into a ScrapedDrawEntry.

        Handles completed matches, upcoming matches, and byes.
        """
        rows = table.select("tr.match-table__row")
        if len(rows) < 2:
            return None

        row_a, row_b = rows[0], rows[1]

        # Extract players
        player_a = self._extract_player_from_row(row_a)
        player_b = self._extract_player_from_row(row_b)

        # Helper to check for Bye
        def is_bye(p):
            return p and p["name"].lower() == "bye"

        is_bye_match = False
        if (player_a and is_bye(player_a)) or (player_b and is_bye(player_b)):
            is_bye_match = True

        # Process player A
        name_a, wta_id_a, seed_a = None, None, None
        if player_a and not is_bye(player_a):
            name_a, seed_a = extract_seed_from_name(player_a["name"])
            wta_id_a = player_a["wta_id"]

        # Process player B
        name_b, wta_id_b, seed_b = None, None, None
        if player_b and not is_bye(player_b):
            name_b, seed_b = extract_seed_from_name(player_b["name"])
            wta_id_b = player_b["wta_id"]

        # Skip entries where both players are unknown (empty/placeholder tables)
        if not name_a and not name_b and not is_bye_match:
            return None

        # Treat missing opponent as a bye slot (common in WTA draws)
        if (name_a and not name_b) or (name_b and not name_a):
            is_bye_match = True

        # Scores
        scores_a = self._extract_scores_from_row(row_a)
        scores_b = self._extract_scores_from_row(row_b)
        score_raw = self._build_score_string(scores_a, scores_b)

        # Winner
        winner_name = None
        table_classes = " ".join(table.get("class", []))
        if "match-table--winner-a" in table_classes:
            winner_name = name_a
        elif "match-table--winner-b" in table_classes:
            winner_name = name_b

        return ScrapedDrawEntry(
            round=round_code,
            draw_position=draw_position,
            player_a_name=name_a,
            player_a_external_id=wta_id_a,
            player_a_seed=seed_a,
            player_b_name=name_b,
            player_b_external_id=wta_id_b,
            player_b_seed=seed_b,
            score_raw=score_raw if score_raw else None,
            winner_name=winner_name,
            is_bye=is_bye_match,
            source="wta",
            tournament_name=tournament_id.replace("-", " ").title(),
            tournament_id=tournament_id,
            tournament_year=year,
        )

    def _parse_scores_day(
        self,
        html: str,
        tournament_id: str,
        tournament_number: str,
        year: int,
        match_date: str,
        start_match_number: int,
    ) -> list[ScrapedMatch]:
        """
        Parse singles matches from the scores page for a single day.

        The scores page shows all matches for the selected day as a flat list
        of div.tennis-match elements. Singles matches have "-LS" in their class
        name (e.g. "js-match-0901-2024-LS12345"), doubles have "-LD".

        Each tennis-match contains:
        - div.tennis-match__round with the round text (e.g. "Round of 64")
        - table.match-table with the same structure as the draws page

        Args:
            html: Full page HTML after clicking a day button
            tournament_id: Tournament slug
            tournament_number: WTA tournament number
            year: Season year
            match_date: ISO date string for this day (e.g. "2026-01-11")
            start_match_number: Starting match number for ordering

        Returns:
            List of ScrapedMatch objects for singles matches on this day
        """
        soup = BeautifulSoup(html, "lxml")
        matches = []
        match_num = start_match_number

        # Find all tennis-match elements
        all_match_divs = soup.find_all(class_=re.compile(r"tennis-match"))

        for match_div in all_match_divs:
            classes = " ".join(match_div.get("class", []))

            # Only process top-level tennis-match containers (not child elements
            # like tennis-match__container, tennis-match__round, etc.)
            if "tennis-match__" in classes:
                continue

            # Filter: only singles (-LS), skip doubles (-LD)
            if "-LD" in classes:
                continue
            if "-LS" not in classes:
                # If neither -LS nor -LD, skip (shouldn't happen but be safe)
                continue

            # Extract round from tennis-match__round div
            round_elem = match_div.select_one(".tennis-match__round")
            round_code = ""
            if round_elem:
                round_text = round_elem.get_text(strip=True).lower()
                round_code = self.ROUND_MAP.get(round_text, round_text.upper())

            # Find the match-table inside this container
            table = match_div.select_one("table.match-table")
            if not table:
                continue

            try:
                scraped = self._parse_match_table(
                    table, tournament_id, tournament_number, year,
                    round_code, match_num
                )
                if scraped:
                    scraped.match_date = match_date
                    matches.append(scraped)
                    match_num += 1
            except Exception as e:
                print(f"Warning: Error parsing WTA scores match: {e}")
                continue

        return matches

    async def _scrape_draws_page(
        self,
        page: Page,
        tournament_id: str,
        year: int,
        tournament_number: str,
    ) -> AsyncGenerator[ScrapedMatch, None]:
        """
        Fallback: scrape results from the draws page (no per-match dates).

        Used for older tournaments that don't have a scores page with day
        navigation buttons. The draws page loads all rounds in a single HTML
        page with separate containers for singles and doubles.

        Args:
            page: Playwright Page object (already open)
            tournament_id: Tournament slug
            year: Season year
            tournament_number: WTA tournament number

        Yields:
            ScrapedMatch objects (without match_date set)
        """
        url = f"{self.BASE_URL}/tournaments/{tournament_number}/{tournament_id}/{year}/draws"
        print(f"Loading WTA draws page (fallback): {url}")
        await self.navigate(page, url, wait_for="domcontentloaded")
        try:
            await page.wait_for_selector("[data-event-type='LS']", timeout=4000)
        except PlaywrightTimeout:
            pass

        await self._dismiss_cookies(page)
        await self._clear_onetrust_overlays(page)

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # Singles container (data-event-type='LS')
        singles_container = soup.select_one("[data-event-type='LS']")
        if not singles_container:
            print(f"WARNING: Could not find singles draw container for {tournament_id} {year}")
            return

        # Use robust container-based parsing
        round_containers = singles_container.select(".tournament-draw__round-container")
        if not round_containers:
            # Try to infer based on older structure if needed, or just iterate flat
             round_containers = [singles_container]

        match_number = 0
        
        for container in round_containers:
            # Find round header
            header = container.select_one("h2.tournament-draw__round-title")
            if not header:
                continue # Skip if no header (or handle flat list if needed)
                
            text = header.get_text(strip=True).lower()
            round_code = self.ROUND_MAP.get(text, text.upper())
            
            # Find tables
            tables = container.select("table.match-table")

            for table in tables:
                try:
                    scraped = self._parse_match_table(
                        table, tournament_id, tournament_number, year,
                        round_code, match_number
                    )
                    if scraped:
                        match_number += 1
                        yield scraped
                except Exception as e:
                    print(f"Warning: Error parsing WTA match: {e}")
                    continue

        print(f"Scraped {match_number} singles matches from {tournament_id} {year} (draws fallback)")

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
        external_id = self._generate_external_id(year, tournament_id, round_code, id_a, id_b)

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
        Falls back to extracting text from .match-table__player if no link found.

        Args:
            row: BeautifulSoup <tr> element

        Returns:
            Dict with name, wta_id, nationality — or None if no player found
        """
        name = None
        wta_id = None

        # Player link
        link = (
            row.select_one("a.match-table__player--link")
            or row.select_one("a[href*='/players/']")
        )
        
        if link:
            name = link.get_text(strip=True)
            # Extract WTA ID from href: /players/{numeric_id}/{slug}
            href = link.get("href", "")
            id_match = re.search(r"/players/(\d+)/", href)
            if id_match:
                wta_id = id_match.group(1)
        else:
            # Fallback for Byes or players without profile links
            player_div = row.select_one(".match-table__player")
            if player_div:
                name = player_div.get_text(strip=True)
            
        if not name:
            return None

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
        year: int,
        tournament_number: str,
    ) -> AsyncGenerator[ScrapedFixture, None]:
        """
        Scrape upcoming fixtures from the order of play page.

        Iterates through all available days on the order of play page to capture
        scheduled matches. Matches are grouped by court.

        Args:
            tournament_id: Tournament slug
            year: Season year
            tournament_number: WTA tournament number

        Yields:
            ScrapedFixture objects
        """
        page = await self.new_page()
        try:
            url = f"{self.BASE_URL}/tournaments/{tournament_number}/{tournament_id}/{year}/order-of-play"
            print(f"Loading WTA schedule: {url}")
            await self.navigate(page, url, wait_for="domcontentloaded")
            try:
                await page.wait_for_selector("section.tournament-oop__day", timeout=4000)
            except PlaywrightTimeout:
                pass
            await self._dismiss_cookies(page)
            await self._clear_onetrust_overlays(page)

            # Check if we have day navigation buttons
            day_buttons = await page.query_selector_all("button.day-navigation__button")

            if not day_buttons:
                # Try parsing current content
                async for fixture in self._parse_fixtures_from_page(page, tournament_id, year):
                    yield fixture
                return

            # Collect available days
            days = []
            for btn in day_buttons:
                date_str = await btn.get_attribute("data-date")
                days.append(date_str)

            print(f"Found schedule days: {days}")

            # Iterate days
            for i, date_str in enumerate(days):
                # Re-query buttons to avoid stale element references
                buttons = await page.query_selector_all("button.day-navigation__button")
                if i < len(buttons):
                    await self._clear_onetrust_overlays(page)
                    await buttons[i].click(force=True)
                    await self._wait_for_oop_day(page, date_str)
                    
                    # Select Singles tab if available
                    await self._select_singles_tab(page)

                    async for fixture in self._parse_fixtures_from_page(
                        page, tournament_id, year, date_str
                    ):
                        yield fixture

        finally:
            await page.close()

    async def _parse_fixtures_from_page(
        self,
        page: Page,
        tournament_id: str,
        year: int,
        date_str: Optional[str] = None,
    ) -> AsyncGenerator[ScrapedFixture, None]:
        """Parse fixtures from the currently loaded schedule page."""
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        day_section = None
        if date_str:
            day_section = soup.select_one(f"section.tournament-oop__day[data-date='{date_str}']")
        if not day_section:
            day_section = soup.select_one("section.tournament-oop__day.is-active")
        if not day_section:
            day_section = soup

        # Derive date_str if not provided (e.g., no day navigation buttons)
        if not date_str:
            if day_section and day_section.has_attr("data-date"):
                date_str = day_section.get("data-date")
            if not date_str:
                active_btn = soup.select_one("button.day-navigation__button.is-active")
                if active_btn:
                    date_str = active_btn.get("data-date")

        utcoffset = None
        oop_root = soup.select_one("section.tournament-oop")
        if oop_root:
            utcoffset = oop_root.get("data-utcoffset")

        # Matches are grouped by court: div.tournament-oop__court
        court_divs = day_section.select("div.tournament-oop__court")

        # If no court divs, maybe flat list?
        if not court_divs:
            # Fallback to finding tennis-match directly
            court_divs = [day_section]  # Treat root as one "court"

        for court_div in court_divs:
            # Get court name
            court_name = "Unknown Court"
            if court_div != soup:
                header = court_div.select_one("h3.court-header__name")
                if header:
                    court_name = header.get_text(strip=True)

            court_start_time = self._extract_court_start_time(court_div)
            court_offset = self._extract_court_utc_offset(court_div) or utcoffset
            last_dt: Optional[datetime] = None
            last_match_live = False
            last_match_complete = False

            # Find matches in this court
            matches = court_div.select("div.tennis-match")
            
            for match_div in matches:
                # Only process top-level match divs
                classes = " ".join(match_div.get("class", []))
                if "tennis-match__" in classes:
                    continue
                    
                is_doubles = "-LD" in classes

                time_info = self._extract_match_time(match_div)
                is_live = self._is_live_match(match_div)
                is_complete = self._is_completed_match(match_div)
                self._debug_oop_match(
                    match_div,
                    court_name,
                    date_str,
                    time_info,
                    is_live,
                    last_dt,
                    court_start_time,
                )

                sched_date = time_info.get("date")
                sched_time = time_info.get("time")
                if sched_time and not sched_date and date_str:
                    sched_date = date_str

                if sched_date and sched_time:
                    try:
                        last_dt = datetime.strptime(
                            f"{sched_date} {sched_time}",
                            "%Y-%m-%d %H:%M",
                        )
                    except ValueError:
                        pass
                else:
                    base_dt = None
                    if date_str and court_start_time:
                        base_dt = self._combine_date_time(date_str, court_start_time)
                    if time_info["followed_by"]:
                        if last_dt:
                            est_dt = last_dt + timedelta(hours=2)
                            sched_date = est_dt.strftime("%Y-%m-%d")
                            sched_time = est_dt.strftime("%H:%M")
                            last_dt = est_dt
                        elif last_match_live or last_match_complete:
                            venue_now = self._venue_now(court_offset) if court_offset else None
                            if venue_now:
                                sched_date = venue_now.strftime("%Y-%m-%d")
                                sched_time = venue_now.strftime("%H:%M")
                                last_dt = venue_now
                        elif base_dt:
                            sched_date = base_dt.strftime("%Y-%m-%d")
                            sched_time = base_dt.strftime("%H:%M")
                            last_dt = base_dt
                    elif base_dt and not sched_time:
                        sched_date = base_dt.strftime("%Y-%m-%d")
                        sched_time = base_dt.strftime("%H:%M")
                        last_dt = base_dt

                last_match_live = is_live
                last_match_complete = is_complete

                if is_doubles:
                    continue

                fixture = self._parse_fixture_div(
                    match_div, tournament_id, year, court_name, date_str
                )
                if fixture:
                    if sched_time:
                        fixture.scheduled_time = sched_time
                    if sched_date:
                        fixture.scheduled_date = sched_date
                    yield fixture

    def _parse_fixture_div(
        self,
        div,
        tournament_id: str,
        year: int,
        court_name: str,
        date_str: Optional[str],
    ) -> Optional[ScrapedFixture]:
        """Parse a single fixture div."""
        # Round
        round_elem = div.select_one(".tennis-match__round")
        round_code = "R32" # Default
        if round_elem:
            text = round_elem.get_text(strip=True).lower()
            round_code = self.ROUND_MAP.get(text, text.upper())

        # Players (from match table inside)
        table = div.select_one("table.match-table")
        if not table:
            return None
            
        rows = table.select("tr.match-table__row")
        if len(rows) < 2:
            return None
            
        row_a, row_b = rows[0], rows[1]
        player_a = self._extract_player_from_row(row_a)
        player_b = self._extract_player_from_row(row_b)
        
        if not player_a or not player_b:
            return None
            
        # Skip byes
        if player_a["name"].lower() == "bye" or player_b["name"].lower() == "bye":
            return None
            
        name_a, seed_a = extract_seed_from_name(player_a["name"])
        name_b, seed_b = extract_seed_from_name(player_b["name"])
        
        return ScrapedFixture(
            tournament_name=tournament_id.replace("-", " ").title(),
            tournament_id=tournament_id,
            tournament_year=year,
            tournament_level="", # Backfilled
            tournament_surface="", # Backfilled
            round=round_code,
            scheduled_date=date_str,
            court=court_name,
            player_a_name=name_a,
            player_a_external_id=player_a["wta_id"],
            player_a_seed=seed_a,
            player_b_name=name_b,
            player_b_external_id=player_b["wta_id"],
            player_b_seed=seed_b,
            source="wta",
        )

    def _extract_court_start_time(self, court_div) -> Optional[str]:
        start = court_div.select_one(".court-header__start")
        if not start:
            return None
        # Prefer data-start-time which is in venue time (e.g., "03:30 PM")
        data_start = start.get("data-start-time")
        if data_start:
            return self._parse_time_str(data_start)
        # Fallback to visible time text
        time_span = start.select_one(".time")
        if time_span:
            return self._parse_time_str(time_span.get_text(strip=True))
        return None

    def _extract_court_utc_offset(self, court_div) -> Optional[str]:
        start = court_div.select_one(".court-header__start")
        if not start:
            return None
        offset = start.get("data-utc-offset")
        return offset or None

    def _extract_match_time(self, match_div) -> dict:
        """
        Extract time information from a match block.
        Returns dict with keys: time (HH:MM), date (YYYY-MM-DD or None), followed_by (bool).
        """
        status_text = ""
        time_text_el = match_div.select_one(".tennis-match__status-time-text")
        if time_text_el:
            status_text = time_text_el.get_text(" ", strip=True)
        if not status_text:
            status_el = match_div.select_one(".tennis-match__status-time")
            if status_el:
                status_text = status_el.get_text(" ", strip=True)

        status_text = " ".join(status_text.split())
        lower = status_text.lower()

        if not status_text:
            # Fallback: scan a smaller portion of the match card for follow-by text
            footer = match_div.select_one(".tennis-match__footer")
            if footer:
                status_text = footer.get_text(" ", strip=True)
                lower = status_text.lower()

        followed_by = "follows previous match" in lower or "followed by" in lower
        if "warmup" in lower:
            return {"time": None, "date": None, "followed_by": followed_by}

        # Treat "On Court" as immediate scheduling when no time is provided.
        if "on court" in lower:
            return {"time": None, "date": None, "followed_by": True}

        # Check data attributes on the match card for time hints
        for key, value in (match_div.attrs or {}).items():
            if "time" in key or "date" in key:
                if isinstance(value, str):
                    hhmm = re.search(r"(\d{1,2}):(\d{2})", value)
                    if hhmm:
                        return {"time": f"{int(hhmm.group(1)):02d}:{hhmm.group(2)}", "date": None, "followed_by": followed_by}

        # Prefer venue time if present
        venue_match = re.search(r"(\d{1,2}:\d{2})(?=\s*\(Venue\))", status_text)
        if venue_match:
            return {"time": venue_match.group(1), "date": None, "followed_by": followed_by}

        # If "Your time" is present, strip it to avoid wrong timezone
        if "your time" in lower:
            status_text = status_text.split("Your time")[0].strip()
            lower = status_text.lower()

        # Only parse explicit scheduling phrases
        has_explicit_time = any(
            key in lower
            for key in [
                "not before",
                "after rest",
                "starts at",
                "start at",
            ]
        )
        if not has_explicit_time and not followed_by:
            return {"time": None, "date": None, "followed_by": followed_by}

        # Parse AM/PM times
        ampm_match = re.search(r"(\d{1,2})(?::(\d{2}))?\s*([AP]M)", status_text, re.I)
        if ampm_match:
            hour = int(ampm_match.group(1))
            minute = int(ampm_match.group(2) or "00")
            ampm = ampm_match.group(3).upper()
            if ampm == "PM" and hour != 12:
                hour += 12
            if ampm == "AM" and hour == 12:
                hour = 0
            return {"time": f"{hour:02d}:{minute:02d}", "date": None, "followed_by": followed_by}

        # Parse 24h time (HH:MM)
        hhmm = re.search(r"(\d{1,2}):(\d{2})", status_text)
        if hhmm:
            return {"time": f"{int(hhmm.group(1)):02d}:{hhmm.group(2)}", "date": None, "followed_by": followed_by}

        return {"time": None, "date": None, "followed_by": followed_by}

    def _parse_time_str(self, text: str) -> Optional[str]:
        if not text:
            return None
        text = " ".join(text.split())
        ampm = re.search(r"(\d{1,2})(?::(\d{2}))?\s*([AP]M)", text, re.I)
        if ampm:
            hour = int(ampm.group(1))
            minute = int(ampm.group(2) or "00")
            mer = ampm.group(3).upper()
            if mer == "PM" and hour != 12:
                hour += 12
            if mer == "AM" and hour == 12:
                hour = 0
            return f"{hour:02d}:{minute:02d}"
        hhmm = re.search(r"(\d{1,2}):(\d{2})", text)
        if hhmm:
            return f"{int(hhmm.group(1)):02d}:{hhmm.group(2)}"
        return None

    def _is_live_match(self, match_div) -> bool:
        """
        Heuristic: detect if a match is currently live.
        """
        try:
            status = (match_div.get("data-status") or "").upper()
            status_el = match_div.select_one(".tennis-match__status-time-text")
            status_text = status_el.get_text(" ", strip=True).lower() if status_el else ""
            if not status_text:
                footer = match_div.select_one(".tennis-match__footer")
                status_text = footer.get_text(" ", strip=True).lower() if footer else ""

            if "finished" in status_text or status == "F":
                return False

            # Data status sometimes indicates live/in progress
            if status in {"L", "LIVE", "I", "IN PROGRESS", "P", "IP"}:
                return True

            # Status text contains "Live Match"
            if "live match" in status_text:
                return True

            # Some layouts add a live label
            if match_div.select_one(".tennis-match__live-label"):
                return True

            # Fallback: header text contains 'live'
            header = match_div.select_one(".tennis-match__header")
            if header and "live" in header.get_text(" ", strip=True).lower():
                return True
        except Exception:
            pass
        return False

    def _is_completed_match(self, match_div) -> bool:
        """
        Detect if a match is completed.
        """
        try:
            status = (match_div.get("data-status") or "").upper()
            if status == "F":
                return True
            status_el = match_div.select_one(".tennis-match__status-time-text")
            status_text = status_el.get_text(" ", strip=True).lower() if status_el else ""
            if "finished" in status_text:
                return True
        except Exception:
            pass
        return False

    def _debug_oop_match(
        self,
        match_div,
        court_name: str,
        date_str: Optional[str],
        time_info: dict,
        is_live: bool,
        last_dt: Optional[datetime],
        court_start_time: Optional[str],
    ) -> None:
        if os.getenv("WTA_OOP_DEBUG") != "1":
            return
        try:
            round_el = match_div.select_one(".tennis-match__round")
            round_txt = round_el.get_text(strip=True) if round_el else "?"
            status_el = match_div.select_one(".tennis-match__status-time-text")
            status_txt = status_el.get_text(" ", strip=True) if status_el else ""
            status_full_el = match_div.select_one(".tennis-match__status-time")
            status_full = status_full_el.get_text(" ", strip=True) if status_full_el else ""
            if not status_txt:
                footer = match_div.select_one(".tennis-match__footer")
                status_txt = footer.get_text(" ", strip=True) if footer else ""
            status_txt = " ".join(status_txt.split())
            status_attr = match_div.get("data-status") or ""

            time_attrs = {}
            for key, value in (match_div.attrs or {}).items():
                if "time" in key or "date" in key:
                    time_attrs[key] = value

            def player_name(sel: str) -> str:
                el = match_div.select_one(sel)
                return el.get_text(strip=True) if el else "?"

            p1 = player_name(".js-team-a .match-table__player-fullname")
            p2 = player_name(".js-team-b .match-table__player-fullname")

            print(
                "[WTA OOP]",
                f"date={date_str}",
                f"court={court_name}",
                f"round={round_txt}",
                f"players={p1} vs {p2}",
                f"status_attr={status_attr}",
                f"status_text='{status_txt}'",
                f"status_full='{status_full}'",
                f"time_attrs={time_attrs}",
                f"followed_by={time_info.get('followed_by')}",
                f"time={time_info.get('time')}",
                f"is_live={is_live}",
                f"last_dt={last_dt}",
                f"court_start={court_start_time}",
            )
        except Exception:
            pass

    def _combine_date_time(self, date_str: str, time_str: str) -> Optional[datetime]:
        try:
            return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            return None

    def _venue_now(self, utcoffset: Optional[str]) -> Optional[datetime]:
        """
        Convert current UTC time to venue local time using a +HHMM/-HHMM offset.
        """
        if not utcoffset:
            return None
        match = re.match(r"^([+-])(\d{2})(\d{2})$", utcoffset)
        if not match:
            return None
        sign = 1 if match.group(1) == "+" else -1
        hours = int(match.group(2))
        minutes = int(match.group(3))
        delta = timedelta(hours=hours, minutes=minutes) * sign
        return datetime.utcnow() + delta

    async def _wait_for_oop_day(self, page: Page, date_str: str) -> None:
        try:
            await page.wait_for_selector(
                f"section.tournament-oop__day.is-active[data-date='{date_str}']",
                timeout=4000,
            )
        except PlaywrightTimeout:
            pass

    async def _select_singles_tab(self, page: Page) -> None:
        """
        Click the Singles event-type filter tab on the scores page.

        The WTA scores page has a filter bar with Singles and Doubles tabs
        (li.js-type-filter with data-type="singles" or "doubles"). The page
        may default to Doubles on some days, so we always click Singles to
        ensure we're viewing the right event type.

        Does nothing if the singles tab is already active or not found.
        """
        try:
            # Check if singles tab is already active
            singles_tab = await page.query_selector(
                "li.js-type-filter[data-type='singles']"
            )
            if not singles_tab:
                return

            # Check if already active
            cls = await singles_tab.get_attribute("class") or ""
            if "is-active" in cls:
                return

            await self._clear_onetrust_overlays(page)
            await singles_tab.click(force=True)
            try:
                await page.wait_for_selector(
                    "li.js-type-filter[data-type='singles'].is-active",
                    timeout=4000,
                )
            except PlaywrightTimeout:
                pass
        except Exception:
            pass

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
                try:
                    await page.wait_for_selector(
                        "#onetrust-accept-btn-handler",
                        state="detached",
                        timeout=4000,
                    )
                except PlaywrightTimeout:
                    pass
            await self._clear_onetrust_overlays(page)
        except Exception:
            pass

    async def _clear_onetrust_overlays(self, page: Page) -> None:
        """
        Force-disable OneTrust overlays that intercept pointer events.
        Safe to call repeatedly.
        """
        try:
            await page.evaluate(
                """
                () => {
                    const overlay = document.querySelector('.onetrust-pc-dark-filter');
                    if (overlay) {
                        overlay.style.display = 'none';
                        overlay.style.pointerEvents = 'none';
                    }
                    const sdk = document.querySelector('#onetrust-consent-sdk');
                    if (sdk) {
                        sdk.style.display = 'none';
                        sdk.style.pointerEvents = 'none';
                    }
                }
                """
            )
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


async def scrape_wta_125_tournament(
    tournament_id: str,
    year: int,
    tournament_number: str = None,
) -> list[ScrapedMatch]:
    """
    Convenience function to scrape a single WTA 125 tournament.
    Note: Functionally identical to scrape_wta_tournament as the results page is same,
    but kept for API consistency.

    Args:
        tournament_id: Tournament URL slug
        year: Tournament year
        tournament_number: WTA tournament number (required)

    Returns:
        List of ScrapedMatch objects
    """
    return await scrape_wta_tournament(tournament_id, year, tournament_number)


async def get_wta_tournaments(year: int, tour_type: str = "main") -> list[dict]:
    """
    Convenience function to get WTA tournaments for a year.

    Args:
        year: Year to get tournaments for
        tour_type: "main", "125", or "all"

    Returns:
        List of tournament dictionaries
    """
    async with WTAScraper() as scraper:
        return await scraper.get_tournament_list(year, tour_type=tour_type)
