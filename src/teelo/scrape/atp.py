"""
ATP Tour website scraper.

Scrapes match results and fixtures from atptour.com.
The ATP website is JavaScript-heavy, so we use Playwright for rendering.

Data available:
- Tournament list by year
- Match results with scores
- Player IDs (ATP IDs like 'D0AG' for Djokovic)
- Optional detailed match statistics

URLs:
- Tournament list: https://www.atptour.com/en/scores/results-archive
- Tournament results: https://www.atptour.com/en/scores/archive/{tournament}/{year}/results
- Tournament draws: https://www.atptour.com/en/scores/archive/{tournament}/{year}/draws
"""

import asyncio
import re
from datetime import datetime
from typing import AsyncGenerator, Optional

from bs4 import BeautifulSoup
from playwright.async_api import Page

from teelo.scrape.base import BaseScraper, ScrapedDrawEntry, ScrapedMatch, ScrapedFixture
from teelo.scrape.parsers.score import parse_score, ScoreParseError
from teelo.scrape.parsers.player import extract_player_info, extract_seed_from_name
from teelo.scrape.atp_tournament_parser import parse_tournament_elements


class ATPScraper(BaseScraper):
    """
    Scraper for ATP Tour website (atptour.com).

    Handles:
    - Main tour (ATP 250, 500, Masters 1000, Grand Slams)
    - ATP Challenger Tour
    - Qualifying rounds

    Usage:
        async with ATPScraper() as scraper:
            # Get all ATP main tour tournaments for a year
            tournaments = await scraper.get_tournament_list(2024)

            # Get Challenger tournaments
            challenger_tournaments = await scraper.get_tournament_list(2024, tour_type="challenger")

            # Scrape results for a specific tournament
            async for match in scraper.scrape_tournament_results("australian-open", 2024):
                print(f"{match.player_a_name} def. {match.player_b_name}: {match.score_raw}")
    """

    BASE_URL = "https://www.atptour.com"

    # Tournament level mapping from banner image filenames and URLs
    # ATP uses banner images with level indicators in the filename
    LEVEL_MAPPING = {
        "grandslam": "Grand Slam",
        "1000": "Masters 1000",
        "500": "ATP 500",
        "250": "ATP 250",
        "challenger": "Challenger",
        "nextgen": "ATP Finals",
        "finals": "ATP Finals",
    }

    # Known ATP 500 tournaments (as of 2024) - used as fallback for level detection
    # These are identified by tournament slug
    ATP_500_TOURNAMENTS = {
        "rotterdam", "rio-de-janeiro", "acapulco", "dubai", "barcelona",
        "washington", "hamburg", "tokyo", "beijing", "vienna", "basel",
        "queen-s-club", "halle",
    }

    # Known ATP 250 tournaments that might be confused with higher levels
    # (most tournaments default to 250 anyway)
    GRAND_SLAM_TOURNAMENTS = {
        "australian-open", "roland-garros", "wimbledon", "us-open",
    }

    # Known Masters 1000 tournaments
    MASTERS_1000_TOURNAMENTS = {
        "indian-wells", "miami", "monte-carlo", "madrid", "rome",
        "canada", "cincinnati", "shanghai", "paris",
    }

    async def get_tournament_list(
        self,
        year: int,
        tour_type: str = "main",
    ) -> list[dict]:
        """
        Get list of ATP tournaments for a given year.

        Scrapes the ATP results archive page to find all tournaments.
        Can fetch main tour tournaments or Challenger tournaments.

        Args:
            year: Year to get tournaments for (e.g., 2024)
            tour_type: Type of tour to fetch:
                - "main": ATP main tour (Grand Slams, Masters, 500, 250)
                - "challenger": ATP Challenger Tour

        Returns:
            List of tournament dictionaries with:
            - id: Tournament URL slug (e.g., "australian-open")
            - name: Full tournament name
            - level: Tournament level
            - surface: Playing surface
            - location: City, Country
            - start_date: Tournament start date (YYYY-MM-DD)
            - end_date: Tournament end date
            - number: Tournament number (for ATP URLs)
        """
        page = await self.new_page()
        tournaments = []

        try:
            # Navigate to results archive
            # For Challenger tours, add tournamentType=ch parameter
            url = f"{self.BASE_URL}/en/scores/results-archive?year={year}"
            if tour_type == "challenger":
                url += "&tournamentType=ch"

            await self.navigate(page, url, wait_for="domcontentloaded")
            # Wait for JS to render the tournament list
            await asyncio.sleep(5)
            await self.random_delay()

            # Get page content
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Parse tournament entries using the dedicated parser
            # (handles the actual ATP HTML structure: div.tournament-list > ul.events > li)
            tournaments = parse_tournament_elements(soup, year)

            print(f"Parsed {len(tournaments)} tournaments from archive page")

            # If no tournaments found, try alternative approach
            if not tournaments:
                print("No tournaments found with primary parser, trying fallback...")
                tournaments = await self._get_tournaments_from_schedule(page, year, tour_type)

        finally:
            await page.close()

        return tournaments

    async def _get_tournaments_from_schedule(
        self,
        page: Page,
        year: int,
        tour_type: str = "main",
    ) -> list[dict]:
        """
        Alternative method to get tournaments from schedule page.

        Used as fallback if results archive structure changes.

        Args:
            page: Playwright Page object
            year: Year to get tournaments for
            tour_type: "main" or "challenger"

        Returns:
            List of tournament dictionaries
        """
        tournaments = []

        url = f"{self.BASE_URL}/en/tournaments"
        await self.navigate(page, url, wait_for="networkidle")
        await self.random_delay()

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # Find tournament links
        links = soup.select("a[href*='/tournaments/']")

        for link in links:
            href = link.get("href", "")
            # Extract tournament ID from URL
            match = re.search(r"/tournaments/([^/]+)/", href)
            if match:
                tourney_id = match.group(1)
                
                # Try to extract name specifically to avoid concatenation
                name_elem = link.select_one(".name, .tournament-name")
                if name_elem:
                    name = name_elem.get_text(strip=True)
                else:
                    # Fallback: get text but try to split by newlines if they exist in source
                    # (Playwright's inner_text would preserve newlines, BeautifulSoup's get_text strips them by default)
                    name = link.get_text(" ", strip=True)
                    
                if not name:
                    name = tourney_id.replace("-", " ").title()

                # Determine level using our known tournament sets
                level = self._detect_level_from_id(tourney_id, tour_type)

                tournaments.append({
                    "id": tourney_id,
                    "name": name,
                    "level": level,
                    "surface": "Hard",  # Default
                    "location": "",
                    "start_date": None,
                    "year": year,
                })

        return tournaments

    def _parse_tournament_element(
        self,
        elem,
        year: int,
        tour_type: str = "main",
    ) -> Optional[dict]:
        """
        Parse a tournament element from the results archive page.

        Uses multiple strategies for level detection:
        1. Banner image filename (most reliable) - contains level indicator
        2. Known tournament sets (fallback for known ATP 500, Masters, Grand Slams)
        3. URL/class hints
        4. Default based on tour_type

        Args:
            elem: BeautifulSoup element containing tournament info
            year: Tournament year
            tour_type: "main" or "challenger" - affects default level

        Returns:
            Tournament dictionary or None if parsing fails
        """
        # Try to find tournament link
        link = elem.select_one("a[href*='/scores/archive/'], a[href*='/tournaments/']")
        if not link:
            return None

        href = link.get("href", "")

        # Extract tournament ID and number from URL
        # Format: /en/scores/archive/tournament-name/580/2024/results
        full_match = re.search(r"/scores/archive/([^/]+)/(\d+)/\d+/results", href)
        if full_match:
            tourney_id = full_match.group(1)
            tourney_number = full_match.group(2)
        else:
            # Try simpler pattern
            id_match = re.search(r"/(?:scores/archive|tournaments)/([^/]+)", href)
            if not id_match:
                return None
            tourney_id = id_match.group(1)
            tourney_number = None

        # Get tournament name
        name_elem = elem.select_one(".tourney-title, .tournament-name, h3, h4")
        name = name_elem.get_text(strip=True) if name_elem else tourney_id.replace("-", " ").title()

        # Get location
        location_elem = elem.select_one(".tourney-location, .location")
        location = location_elem.get_text(strip=True) if location_elem else ""

        # Get surface
        surface = "Hard"  # Default
        surface_elem = elem.select_one(".tourney-details, .surface, [class*='surface']")
        if surface_elem:
            surface_text = surface_elem.get_text().lower()
            surface = self._normalize_surface(surface_text)

        # Detect level using multiple strategies
        level = self._detect_level(elem, tourney_id, href, tour_type)

        # Get dates
        # ATP archive shows dates like "2024.01.14 - 2024.01.28"
        date_elem = elem.select_one(".tourney-dates, .dates")
        start_date = None
        end_date = None
        if date_elem:
            date_text = date_elem.get_text(strip=True)
            # Find all YYYY.MM.DD / YYYY-MM-DD / YYYY/MM/DD dates in the text
            date_matches = re.findall(r"(\d{4}[-./]\d{2}[-./]\d{2})", date_text)
            if date_matches:
                start_date = date_matches[0].replace("/", "-").replace(".", "-")
                if len(date_matches) >= 2:
                    end_date = date_matches[1].replace("/", "-").replace(".", "-")

        return {
            "id": tourney_id,
            "name": name,
            "number": tourney_number,  # Include tournament number for direct URL construction
            "level": level,
            "surface": surface,
            "location": location,
            "start_date": start_date,
            "end_date": end_date,
            "year": year,
        }

    def _detect_level(
        self,
        elem,
        tourney_id: str,
        href: str,
        tour_type: str = "main",
    ) -> str:
        """
        Detect tournament level using multiple strategies.

        Strategy priority:
        1. Banner image filename (most reliable - ATP uses level indicators in filenames)
        2. Known tournament sets (reliable for major tournaments)
        3. URL/class-based hints
        4. Default based on tour_type

        Args:
            elem: BeautifulSoup element containing tournament info
            tourney_id: Tournament URL slug
            href: Tournament link href
            tour_type: "main" or "challenger"

        Returns:
            Tournament level string (e.g., "ATP 500", "Masters 1000", "Challenger")
        """
        # Strategy 1: Parse banner image filename
        # ATP uses images like "events_banner_1000.png", "events_banner_gs.png"
        banner = elem.select_one(".events_banner, .tourney-badge, img[src*='banner']")
        if banner:
            src = banner.get("src", "").lower()
            # Check for level indicators in the filename
            for level_key, level_name in self.LEVEL_MAPPING.items():
                if level_key in src:
                    return level_name

        # Strategy 2: Check known tournament sets
        level_from_id = self._detect_level_from_id(tourney_id, tour_type)
        if level_from_id != ("Challenger" if tour_type == "challenger" else "ATP 250"):
            # We got a more specific level, use it
            return level_from_id

        # Strategy 3: Check URL and element classes for hints
        href_lower = href.lower()
        elem_classes = " ".join(elem.get("class", [])).lower()

        for level_key, level_name in self.LEVEL_MAPPING.items():
            if level_key in href_lower or level_key in elem_classes:
                return level_name

        # Strategy 4: Default based on tour type
        return "Challenger" if tour_type == "challenger" else "ATP 250"

    def _detect_level_from_id(self, tourney_id: str, tour_type: str = "main") -> str:
        """
        Detect tournament level from known tournament ID sets.

        This is a reliable fallback because tournament identifiers don't change,
        even if website structure changes.

        Args:
            tourney_id: Tournament URL slug (e.g., "australian-open")
            tour_type: "main" or "challenger"

        Returns:
            Tournament level string
        """
        tourney_lower = tourney_id.lower()

        # Check Grand Slams first (highest priority)
        if tourney_lower in self.GRAND_SLAM_TOURNAMENTS:
            return "Grand Slam"

        # Check Masters 1000
        if tourney_lower in self.MASTERS_1000_TOURNAMENTS:
            return "Masters 1000"

        # Check ATP 500
        if tourney_lower in self.ATP_500_TOURNAMENTS:
            return "ATP 500"

        # Default based on tour type
        return "Challenger" if tour_type == "challenger" else "ATP 250"

    async def _get_tournament_number(
        self,
        page: Page,
        tournament_id: str,
        year: int,
        tour_type: str = "main",
    ) -> Optional[str]:
        """
        Look up the tournament number from the ATP results archive.

        ATP URLs require a tournament number (e.g., "580" for Australian Open).
        Full URL format: /en/scores/archive/{slug}/{number}/{year}/results

        Args:
            page: Playwright Page object
            tournament_id: Tournament URL slug (e.g., "australian-open")
            year: Year to look up
            tour_type: "main" or "challenger" - determines which archive to search

        Returns:
            Tournament number as string, or None if not found
        """
        url = f"{self.BASE_URL}/en/scores/results-archive?year={year}"
        if tour_type == "challenger":
            url += "&tournamentType=ch"

        await self.navigate(page, url, wait_for="domcontentloaded")
        await self.random_delay()

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # Find links matching this tournament
        # Format: /en/scores/archive/australian-open/580/2024/results
        pattern = rf"/en/scores/archive/{re.escape(tournament_id)}/(\d+)/{year}/results"

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            match = re.search(pattern, href)
            if match:
                return match.group(1)

        return None

    async def scrape_tournament_results(
        self,
        tournament_id: str,
        year: int,
        tournament_number: Optional[str] = None,
        tour_type: str = "main",
    ) -> AsyncGenerator[ScrapedMatch, None]:
        """
        Scrape all completed match results for a tournament.

        Navigates to the tournament results page and extracts all matches
        including qualifying rounds (ATP serves all rounds on the same page).

        Args:
            tournament_id: Tournament URL slug (e.g., "australian-open")
            year: Year of the tournament edition
            tournament_number: Optional tournament number (e.g., "580").
                              If not provided, will be looked up from archive.
            tour_type: "main" or "challenger" - affects level detection

        Yields:
            ScrapedMatch objects for each completed match
        """
        # Use a single page for all operations (navigating between URLs)
        # This is more efficient than opening multiple pages
        page = await self.new_page()

        try:
            # Look up tournament number if not provided
            # ATP URLs require: /en/scores/archive/{slug}/{number}/{year}/results
            if not tournament_number:
                print(f"Looking up tournament number for {tournament_id}...")
                tournament_number = await self._get_tournament_number(
                    page, tournament_id, year, tour_type
                )

                if not tournament_number:
                    print(f"Could not find tournament number for {tournament_id} {year}")
                    return

                print(f"Found tournament number: {tournament_number}")

            # Get tournament metadata (navigates to tournament overview page)
            tournament_info = await self._get_tournament_info(
                page, tournament_id, year, tour_type,
                tournament_number=tournament_number,
            )
            tournament_info["number"] = tournament_number

            # Scrape main draw results
            # Format: /en/scores/archive/{slug}/{number}/{year}/results
            # Note: Deduplication is handled via external_id (which includes player IDs)
            # at the calling layer (backfill script) and the database unique constraint
            results_url = f"{self.BASE_URL}/en/scores/archive/{tournament_id}/{tournament_number}/{year}/results"
            print(f"Scraping: {results_url}")

            await self.navigate(page, results_url, wait_for="domcontentloaded")

            # Wait longer for JavaScript to render
            await asyncio.sleep(3)

            # Wait for match elements to load (ATP uses JavaScript rendering)
            try:
                await page.wait_for_selector(".match", timeout=15000)
            except Exception:
                print(f"Warning: No .match elements found on page for {tournament_id}")

            await self.random_delay()

            html = await page.content()

            # Parse and yield matches
            async for match in self._parse_results_page(html, tournament_info, "main"):
                yield match


        finally:
            await page.close()

    async def scrape_tournament_draw(
        self,
        tournament_id: str,
        year: int,
        tournament_number: Optional[str] = None,
        tour_type: str = "main",
    ) -> list[ScrapedDrawEntry]:
        """
        Scrape the full tournament draw bracket.

        Navigates to the ATP draws page and extracts all draw entries
        with player names, seeds, scores, and draw positions. Draw
        positions are computed from the order of matches within each round.

        Args:
            tournament_id: Tournament URL slug (e.g., "australian-open")
            year: Year of the tournament edition
            tournament_number: Optional tournament number (e.g., "580").
                              If not provided, will be looked up from archive.
            tour_type: "main" or "challenger"

        Returns:
            List of ScrapedDrawEntry objects for all draw slots
        """
        page = await self.new_page()
        entries = []

        try:
            # Look up tournament number if not provided
            if not tournament_number:
                print(f"Looking up tournament number for {tournament_id}...")
                tournament_number = await self._get_tournament_number(
                    page, tournament_id, year, tour_type
                )
                if not tournament_number:
                    print(f"Could not find tournament number for {tournament_id} {year}")
                    return entries
                print(f"Found tournament number: {tournament_number}")

            # Get tournament metadata
            tournament_info = await self._get_tournament_info(
                page, tournament_id, year, tour_type,
                tournament_number=tournament_number,
            )

            # Navigate to draws page
            draws_url = (
                f"{self.BASE_URL}/en/scores/archive/"
                f"{tournament_id}/{tournament_number}/{year}/draws"
            )
            print(f"Scraping draw: {draws_url}")

            await self.navigate(page, draws_url, wait_for="domcontentloaded")
            await asyncio.sleep(5)

            # Wait for draw-item elements (draws page uses .draw-item, not .match)
            try:
                await page.wait_for_selector(".draw-item", timeout=15000)
            except Exception:
                print(f"Warning: No .draw-item elements found on draws page for {tournament_id}")

            await self.random_delay()
            html = await page.content()

            # Parse draw entries from the HTML
            entries = self._parse_draw_page(html, tournament_info)

        finally:
            await page.close()

        return entries

    def _parse_draw_page(
        self,
        html: str,
        tournament_info: dict,
    ) -> list[ScrapedDrawEntry]:
        """
        Parse the draws page HTML into ScrapedDrawEntry objects.

        ATP draws page structure (confirmed via test_atp_draw.py):
        - div.draw (one per round, 7 total for a 128 draw)
          - div.draw-header: round name (e.g., "Round of 128", "Quarter-Finals")
          - div.draw-content: contains the draw-items for this round
            - div.draw-item (one per match in the round)
              - div.draw-stats
                - div.stats-item (player A)
                  - div.player-info → div.name → a (player link) + span (seed)
                  - div.winner (present if this player won)
                  - div.scores → div.score-item (one per set, 5 slots)
                    - span (game count), span (tiebreak score, empty if no TB)
                - div.stats-item (player B) — same structure
                - div.stats-cta — H2H and Stats links

        Draw positions are assigned sequentially within each round,
        top-to-bottom in the bracket.

        Args:
            html: Raw HTML of the draws page
            tournament_info: Tournament metadata dict

        Returns:
            List of ScrapedDrawEntry objects
        """
        soup = BeautifulSoup(html, "lxml")
        entries = []

        # Iterate over each round's .draw container
        draw_rounds = soup.find_all(class_="draw")

        if not draw_rounds:
            print(f"Warning: No .draw containers found on draws page. HTML length: {len(html)}")
            return entries

        for draw_round in draw_rounds:
            # Get round name from header
            header = draw_round.find(class_="draw-header")
            if not header:
                continue
            round_text = header.get_text(strip=True)
            round_code = self._normalize_round(round_text)

            # Get all draw-items in this round
            draw_items = draw_round.find_all(class_="draw-item")

            for position, item in enumerate(draw_items, start=1):
                try:
                    entry = self._parse_draw_item(
                        item, round_code, position, tournament_info
                    )
                    if entry:
                        entries.append(entry)
                except Exception as e:
                    print(f"Error parsing draw item {round_code} #{position}: {e}")
                    continue

        # Print summary by round
        round_counts: dict[str, int] = {}
        for entry in entries:
            round_counts[entry.round] = round_counts.get(entry.round, 0) + 1
        print(f"Draw entries by round: {round_counts}")

        return entries

    def _parse_draw_item(
        self,
        item,
        round_code: str,
        draw_position: int,
        tournament_info: dict,
    ) -> Optional[ScrapedDrawEntry]:
        """
        Parse a single .draw-item element from the draws page.

        Each draw-item contains two .stats-item divs (one per player).
        Each stats-item has:
        - .player-info → .name → a (player link) + span (seed like "(1)")
        - .winner div (present only for the winning player)
        - .scores → .score-item spans (game count + tiebreak per set)

        Args:
            item: BeautifulSoup element with class="draw-item"
            round_code: Normalized round code (e.g., "R128", "QF")
            draw_position: 1-indexed position within this round
            tournament_info: Tournament metadata dict

        Returns:
            ScrapedDrawEntry or None if parsing fails
        """
        # Common tournament fields for the entry
        tourney_kwargs = dict(
            source="atp",
            tournament_name=tournament_info["name"],
            tournament_id=tournament_info["id"],
            tournament_year=tournament_info["year"],
            tournament_level=tournament_info["level"],
            tournament_surface=tournament_info["surface"],
        )

        # Find the two stats-item divs (player A and player B)
        stats_items = item.find_all(class_="stats-item")
        if len(stats_items) < 2:
            return None

        # Parse each player's info
        player_a = self._parse_draw_stats_item(stats_items[0])
        player_b = self._parse_draw_stats_item(stats_items[1])

        # Handle byes
        if player_a and player_a["name"] and player_a["name"].lower() == "bye":
            return ScrapedDrawEntry(
                round=round_code,
                draw_position=draw_position,
                player_a_name=player_b["name"] if player_b else None,
                player_a_external_id=player_b["atp_id"] if player_b else None,
                player_a_seed=player_b["seed"] if player_b else None,
                is_bye=True,
                **tourney_kwargs,
            )
        if player_b and player_b["name"] and player_b["name"].lower() == "bye":
            return ScrapedDrawEntry(
                round=round_code,
                draw_position=draw_position,
                player_a_name=player_a["name"] if player_a else None,
                player_a_external_id=player_a["atp_id"] if player_a else None,
                player_a_seed=player_a["seed"] if player_a else None,
                is_bye=True,
                **tourney_kwargs,
            )

        # Skip if no players found
        p_a_name = player_a["name"] if player_a else None
        p_b_name = player_b["name"] if player_b else None
        if not p_a_name and not p_b_name:
            return None

        # Build score from per-player score items
        score_raw = None
        if player_a and player_b and player_a["scores"] and player_b["scores"]:
            score_raw = self._build_draw_score(player_a["scores"], player_b["scores"])

        # Determine winner from the .winner div presence
        winner_name = None
        if player_a and player_a["is_winner"]:
            winner_name = p_a_name
        elif player_b and player_b["is_winner"]:
            winner_name = p_b_name

        return ScrapedDrawEntry(
            round=round_code,
            draw_position=draw_position,
            player_a_name=p_a_name,
            player_a_external_id=player_a["atp_id"] if player_a else None,
            player_a_seed=player_a["seed"] if player_a else None,
            player_b_name=p_b_name,
            player_b_external_id=player_b["atp_id"] if player_b else None,
            player_b_seed=player_b["seed"] if player_b else None,
            score_raw=score_raw,
            winner_name=winner_name,
            **tourney_kwargs,
        )

    def _parse_draw_stats_item(self, stats_item) -> Optional[dict]:
        """
        Parse a single .stats-item element for one player in a draw match.

        Structure:
        - .player-info → .name → a (href has ATP ID), span (seed)
        - .winner div (present if this player won)
        - .scores → .score-item (5 slots, each with 2 spans: games + tiebreak)

        Args:
            stats_item: BeautifulSoup element with class="stats-item"

        Returns:
            Dict with keys: name, atp_id, seed, is_winner, scores
            or None if no player info found
        """
        player_info = stats_item.find(class_="player-info")
        if not player_info:
            return None

        # Extract name and ATP ID from the link
        name_div = player_info.find(class_="name")
        if not name_div:
            return None

        link = name_div.find("a")
        if not link:
            # No link — could be "Bye", "Qualifier / Lucky Loser", or other placeholder
            raw_text = name_div.get_text(strip=True).lower()
            if "bye" in raw_text:
                return {"name": "Bye", "atp_id": None, "seed": None, "is_winner": False, "scores": []}
            # Qualifier/LL or other TBD placeholder — no usable player data
            return None

        name = link.get_text(strip=True).title()

        # ATP ID from href: /en/players/jannik-sinner/s0ag/overview
        atp_id = None
        href = link.get("href", "")
        id_match = re.search(r"/players/[^/]+/([a-zA-Z0-9]+)/", href)
        if id_match:
            atp_id = id_match.group(1).upper()

        # Seed from the span sibling of the link (e.g., "(1)")
        seed = None
        seed_span = name_div.find("span")
        if seed_span:
            seed_text = seed_span.get_text(strip=True)
            seed_match = re.search(r"\((\d+)\)", seed_text)
            if seed_match:
                seed = int(seed_match.group(1))

        # Check if this player is the winner (has .winner div)
        is_winner = player_info.find(class_="winner") is not None

        # Extract per-set scores from .scores → .score-item
        # Each score-item has 2 spans: [game_count, tiebreak_score]
        scores = []
        scores_div = stats_item.find(class_="scores")
        if scores_div:
            for score_item in scores_div.find_all(class_="score-item"):
                spans = score_item.find_all("span")
                games = spans[0].get_text(strip=True) if len(spans) >= 1 else ""
                tiebreak = spans[1].get_text(strip=True) if len(spans) >= 2 else ""
                if games:  # Only include sets that have been played
                    scores.append({"games": games, "tiebreak": tiebreak})

        return {
            "name": name,
            "atp_id": atp_id,
            "seed": seed,
            "is_winner": is_winner,
            "scores": scores,
        }

    def _build_draw_score(
        self,
        scores_a: list[dict],
        scores_b: list[dict],
    ) -> Optional[str]:
        """
        Build a standard score string from per-player set scores.

        The draws page shows scores per player (unlike the results page which
        interleaves them). Each player has a list of {games, tiebreak} dicts.

        Args:
            scores_a: Player A's per-set scores [{games: "6", tiebreak: ""}, ...]
            scores_b: Player B's per-set scores [{games: "3", tiebreak: ""}, ...]

        Returns:
            Score string like "6-3 7-6(4) 6-3" or None if no scores

        Example:
            scores_a = [{"games": "6", "tiebreak": ""}, {"games": "7", "tiebreak": ""}]
            scores_b = [{"games": "3", "tiebreak": ""}, {"games": "6", "tiebreak": "4"}]
            → "6-3 7-6(4)"
        """
        if not scores_a and not scores_b:
            return None

        num_sets = max(len(scores_a), len(scores_b))
        if num_sets == 0:
            return None

        sets = []
        for i in range(num_sets):
            a_games = scores_a[i]["games"] if i < len(scores_a) else "0"
            b_games = scores_b[i]["games"] if i < len(scores_b) else "0"

            set_str = f"{a_games}-{b_games}"

            # Add tiebreak score (shown on the losing side)
            a_tb = scores_a[i].get("tiebreak", "") if i < len(scores_a) else ""
            b_tb = scores_b[i].get("tiebreak", "") if i < len(scores_b) else ""
            tb = a_tb or b_tb
            if tb:
                set_str += f"({tb})"

            sets.append(set_str)

        result = " ".join(sets)
        return result if result.strip() else None

    async def _get_tournament_info(
        self,
        page: Page,
        tournament_id: str,
        year: int,
        tour_type: str = "main",
        tournament_number: Optional[str] = None,
    ) -> dict:
        """
        Get tournament metadata (level, surface, location).

        Uses the improved level detection system with multiple fallback strategies.

        Args:
            page: Playwright Page object
            tournament_id: Tournament URL slug
            year: Tournament year
            tour_type: "main" or "challenger" - affects default level
            tournament_number: Tournament number for URL (e.g., "339" for Brisbane)

        Returns:
            Dictionary with tournament metadata
        """
        # Get initial level from known tournament sets
        initial_level = self._detect_level_from_id(tournament_id, tour_type)

        # Default info
        info = {
            "id": tournament_id,
            "name": tournament_id.replace("-", " ").title(),
            "year": year,
            "level": initial_level,
            "surface": "Hard",
            "location": "",
            "country_ioc": None,
            "start_date": None,
            "end_date": None,
        }

        # Try to get from tournament page
        # URL requires tournament number: /en/tournaments/{slug}/{number}/overview
        if tournament_number:
            url = f"{self.BASE_URL}/en/tournaments/{tournament_id}/{tournament_number}/overview"
        else:
            url = f"{self.BASE_URL}/en/tournaments/{tournament_id}/overview"

        try:
            await self.navigate(page, url, wait_for="domcontentloaded")
            await self.random_delay()

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Get tournament name
            title = soup.select_one("h1, .tournament-title, .hero-title")
            if title:
                info["name"] = title.get_text(strip=True)

            # Get surface - try multiple selectors
            surface_elem = soup.select_one(".surface, [class*='surface'], .info-area span")
            if surface_elem:
                info["surface"] = self._normalize_surface(surface_elem.get_text())
            else:
                # Fallback for new layout (2025/2026)
                # Structure: <ul class="td_left"><li><span>Surface</span><span>Hard</span></li></ul>
                for li in soup.select("ul.td_left li, ul.td_right li"):
                    spans = li.find_all("span")
                    if len(spans) >= 2:
                        label = spans[0].get_text(strip=True).lower()
                        value = spans[1].get_text(strip=True)
                        
                        if "surface" in label:
                            info["surface"] = self._normalize_surface(value)
                        elif "location" in label or "city" in label:
                            # Sometimes location is here too?
                            info["location"] = value

            # Get location
            location_elem = soup.select_one(".location, .tournament-location, .info-area")
            if location_elem:
                info["location"] = location_elem.get_text(strip=True)

            # Note: Tournament dates are NOT available on the overview page.
            # Dates are scraped from the archive page (get_tournament_list) instead,
            # and flow through task_params to the edition creation in backfill.

            # Try to improve level detection from page content if we only have default
            # Only override if we find more specific information
            page_text = soup.get_text().lower()

            # Check banner image first (most reliable)
            banner = soup.select_one(".events_banner, .tourney-badge, img[src*='banner']")
            if banner:
                src = banner.get("src", "").lower()
                for level_key, level_name in self.LEVEL_MAPPING.items():
                    if level_key in src:
                        info["level"] = level_name
                        break
            else:
                # Fall back to page text analysis
                if "grand slam" in page_text:
                    info["level"] = "Grand Slam"
                elif "masters 1000" in page_text or "atp masters" in page_text:
                    info["level"] = "Masters 1000"
                elif "atp 500" in page_text:
                    info["level"] = "ATP 500"
                elif "challenger" in page_text and info["level"] == "ATP 250":
                    info["level"] = "Challenger"

        except Exception as e:
            print(f"Could not get tournament info for {tournament_id}: {e}")

        return info

    async def _parse_results_page(
        self,
        html: str,
        tournament_info: dict,
        draw_type: str,
    ) -> AsyncGenerator[ScrapedMatch, None]:
        """
        Parse a results page and yield matches.

        The ATP results page groups matches by day using accordion sections:
        - Each day has a "tournament-day" header inside "atp_accordion-header"
        - The header's <h4> contains the date (e.g., "Sun, 11 January, 2026")
          or just the round name on older pages (e.g., "Final")
        - Matches for that day are in the sibling "atp_accordion-content" div

        If the page doesn't use the accordion/day structure (e.g., older pages
        or challengers), falls back to iterating all class="match" elements
        without dates.

        Args:
            html: HTML content of the results page
            tournament_info: Tournament metadata
            draw_type: 'main' or 'qualifying'

        Yields:
            ScrapedMatch objects
        """
        soup = BeautifulSoup(html, "lxml")
        match_number = 0

        # Try day-based parsing first (2025+ ATP layout)
        day_elems = soup.find_all(class_="tournament-day")

        if day_elems:
            for day_elem in day_elems:
                # Extract date from the <h4> inside tournament-day
                # Format: "Sun, 11 January, 2026Day (9)" or just "Final"
                match_date = self._extract_date_from_day_header(
                    day_elem, tournament_info["year"]
                )

                # Find matches in the accordion content (sibling of the header)
                header = day_elem.parent  # atp_accordion-header
                if not header:
                    continue
                content = header.find_next_sibling(class_="atp_accordion-content")
                if not content:
                    continue

                match_containers = content.find_all(class_="match")
                for match_elem in match_containers:
                    try:
                        round_header = match_elem.find(class_="match-header")
                        current_round = ""
                        if round_header:
                            span = round_header.find("span")
                            if span:
                                round_text = span.get_text(strip=True)
                                current_round = self._normalize_round(round_text)

                        match = self._parse_match_element(
                            match_elem,
                            tournament_info,
                            current_round,
                            draw_type,
                            match_number,
                        )

                        if match:
                            match.match_date = match_date
                            match_number += 1
                            match.match_number = match_number
                            yield match

                    except Exception as e:
                        print(f"Error parsing match element: {e}")
                        continue

            if match_number > 0:
                return

        # Fallback: iterate all match elements without day grouping
        # (used for older pages, challengers, or if day parsing yielded nothing)
        match_containers = soup.find_all(class_="match")

        if not match_containers:
            print(f"Warning: No matches found on page. HTML length: {len(html)}")
            return

        for match_elem in match_containers:
            try:
                round_header = match_elem.find(class_="match-header")
                current_round = ""
                if round_header:
                    span = round_header.find("span")
                    if span:
                        round_text = span.get_text(strip=True)
                        current_round = self._normalize_round(round_text)

                match = self._parse_match_element(
                    match_elem,
                    tournament_info,
                    current_round,
                    draw_type,
                    match_number,
                )

                if match:
                    match_number += 1
                    match.match_number = match_number
                    yield match

            except Exception as e:
                print(f"Error parsing match element: {e}")
                continue

    def _extract_date_from_day_header(self, day_elem, year: int) -> Optional[str]:
        """
        Extract an ISO date string from an ATP tournament-day <h4> element.

        The <h4> text can be:
        - "Sun, 11 January, 2026Day (9)" → "2026-01-11"
        - "Final" → None (no date available on older pages)

        Args:
            day_elem: BeautifulSoup element with class="tournament-day"
            year: Tournament year (used as fallback for parsing)

        Returns:
            ISO date string "YYYY-MM-DD" or None if no date found
        """
        h4 = day_elem.find("h4")
        if not h4:
            return None

        text = h4.get_text(strip=True)

        # Try to match a date pattern like "Sun, 11 January, 2026"
        # The "Day (N)" suffix gets concatenated but we can ignore it
        date_match = re.search(
            r"(\d{1,2})\s+(January|February|March|April|May|June|"
            r"July|August|September|October|November|December)"
            r"(?:,?\s*(\d{4}))?",
            text
        )
        if not date_match:
            return None

        day = int(date_match.group(1))
        month_name = date_match.group(2)
        match_year = int(date_match.group(3)) if date_match.group(3) else year

        try:
            dt = datetime.strptime(f"{day} {month_name} {match_year}", "%d %B %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    def _parse_match_element(
        self,
        match_elem,
        tournament_info: dict,
        current_round: str,
        draw_type: str,
        match_number: int,
    ) -> Optional[ScrapedMatch]:
        """
        Parse a single match element using v3.0's working selectors.

        ATP HTML structure (from v3.0 analysis):
        - Player names: class="name" → a tag with text and href
        - Player ATP ID: from a href /en/players/{name}/{ATP_ID}/...
        - Scores: class="score-item" → span elements

        Args:
            match_elem: BeautifulSoup element for the match container
            tournament_info: Tournament metadata
            current_round: Current round (from header)
            draw_type: 'main' or 'qualifying'
            match_number: Match number for external ID

        Returns:
            ScrapedMatch if successfully parsed, None otherwise
        """
        # Find player name elements (v3.0 pattern: class="name" → a tag)
        names = match_elem.find_all(class_="name")

        if len(names) < 2:
            return None

        # Extract player A (winner - listed first in ATP results)
        player_a_link = names[0].find("a")
        if not player_a_link:
            return None

        # Extract player B (loser)
        player_b_link = names[1].find("a")
        if not player_b_link:
            return None

        # Get player names
        name_a = player_a_link.get_text(strip=True).title()
        name_b = player_b_link.get_text(strip=True).title()

        # Skip byes
        if name_a.lower() == "bye" or name_b.lower() == "bye":
            return None

        # Skip walkovers shown in score
        # (Some matches have W/O or walkover text)

        # Extract ATP IDs from href
        # Format: /en/players/jannik-sinner/s0ag/overview
        # Note: IDs can be lowercase (s0ag, mm58) or uppercase (D0AG)
        href_a = player_a_link.get("href", "")
        href_b = player_b_link.get("href", "")

        id_match_a = re.search(r"/players/[^/]+/([a-zA-Z0-9]+)/", href_a)
        id_match_b = re.search(r"/players/[^/]+/([a-zA-Z0-9]+)/", href_b)

        atp_id_a = id_match_a.group(1).upper() if id_match_a else None
        atp_id_b = id_match_b.group(1).upper() if id_match_b else None

        # Clean names and extract seeds
        name_a, seed_a = extract_seed_from_name(name_a)
        name_b, seed_b = extract_seed_from_name(name_b)

        # Get scores using v3.0 pattern (class="score-item")
        score_items = match_elem.find_all(class_="score-item")
        score_raw = self._parse_score_items(score_items)

        # Determine match status (completed, retired, walkover)
        status = "completed"

        # Strategy 1: Check match-cta text for explicit indicators
        match_cta = match_elem.find(class_="match-cta")
        if match_cta:
            cta_text = match_cta.get_text().lower()
            if "w/o" in cta_text or "walkover" in cta_text:
                status = "walkover"
            elif "ret" in cta_text:
                status = "retired"

        # Strategy 2: Check score text for explicit indicators
        if status == "completed" and score_raw:
            score_lower = score_raw.lower()
            if "w/o" in score_lower or "walkover" in score_lower:
                status = "walkover"
            elif "ret" in score_lower:
                status = "retired"

        # Strategy 3: Detect retirement from incomplete final set
        # ATP doesn't always mark retirements explicitly — the score just
        # shows an incomplete set (e.g. "7-5 2-1" where neither player
        # reached 6 games in the last set)
        if status == "completed" and score_raw:
            status = _detect_retirement_from_score(score_raw, status)

        # Strategy 4: No score at all but match exists -> walkover
        if not score_raw or score_raw.strip() == "":
            status = "walkover"
            score_raw = "W/O"

        # Generate external ID using player IDs for reliable deduplication
        # Format: YYYY_TOURNEY_ROUND_PLAYERID1_PLAYERID2 (sorted for consistency)
        # This ensures the same match always gets the same external_id regardless
        # of parsing order or if it appears multiple times in the HTML
        round_code = current_round if current_round else "R128"
        if draw_type == "qualifying" and current_round:
            round_code = f"Q{current_round[-1] if current_round[-1].isdigit() else '1'}"

        # Use player IDs if available, otherwise fall back to normalized names
        # Sort to ensure consistent ordering (A vs B == B vs A)
        player_id_a = atp_id_a or name_a.lower().replace(" ", "-")
        player_id_b = atp_id_b or name_b.lower().replace(" ", "-")
        sorted_ids = sorted([player_id_a, player_id_b])

        external_id = f"{tournament_info['year']}_{tournament_info['id']}_{round_code}_{sorted_ids[0]}_{sorted_ids[1]}"

        return ScrapedMatch(
            external_id=external_id,
            source="atp",
            tournament_name=tournament_info["name"],
            tournament_id=tournament_info["id"],
            tournament_year=tournament_info["year"],
            tournament_level=tournament_info["level"],
            tournament_surface=tournament_info["surface"],
            round=round_code,
            tournament_location=tournament_info.get("location"),
            tournament_country_ioc=tournament_info.get("country_ioc"),
            player_a_name=name_a,
            player_a_external_id=atp_id_a,
            player_a_seed=seed_a,
            player_b_name=name_b,
            player_b_external_id=atp_id_b,
            player_b_seed=seed_b,
            winner_name=name_a,  # Winner is listed first in ATP results
            score_raw=score_raw,
            status=status,
        )

    def _parse_score_items(self, score_items) -> str:
        """
        Parse ATP score items into standard format.

        ATP HTML shows scores as individual cells for each set, with each player's
        games in separate rows. The score-item elements contain:
        - First span: main game count (e.g., "7")
        - Additional spans: tiebreak score if any (e.g., "(5)")

        The items are arranged: [A_set1, A_set2, ..., B_set1, B_set2, ...]

        Args:
            score_items: List of BeautifulSoup elements with class="score-item"

        Returns:
            Score string in standard format like "7-6(5) 6-4"
        """
        if not score_items:
            return ""

        scores = []
        for item in score_items:
            span = item.find("span")
            if not span:
                continue

            score_text = span.get_text(strip=True)

            # Include tiebreak scores from additional spans
            # (sometimes shown as separate spans within the item)
            extra_spans = item.find_all("span")[1:]
            for extra in extra_spans:
                extra_text = extra.get_text(strip=True)
                if extra_text:
                    score_text += extra_text

            if score_text:
                scores.append(score_text)

        # Need even number of scores (half for each player)
        if not scores or len(scores) % 2 != 0:
            return ""

        # Combine into standard format: "7-6(5) 6-4"
        half = len(scores) // 2
        sets = []

        for i in range(half):
            a_score = scores[i]
            b_score = scores[i + half]

            # Extract main game count (first digit)
            a_games = a_score[0] if a_score else "0"
            b_games = b_score[0] if b_score else "0"

            set_str = f"{a_games}-{b_games}"

            # Add tiebreak if present (in parentheses)
            # Tiebreak shown after the game count, e.g., "7(5)" means 7 games with tiebreak 5
            if len(a_score) > 1 and "(" in a_score:
                # Extract tiebreak portion
                tb_match = re.search(r"\((\d+)\)", a_score)
                if tb_match:
                    set_str += f"({tb_match.group(1)})"
            elif len(b_score) > 1 and "(" in b_score:
                tb_match = re.search(r"\((\d+)\)", b_score)
                if tb_match:
                    set_str += f"({tb_match.group(1)})"

            sets.append(set_str)

        return " ".join(sets)

    async def scrape_fixtures(
        self,
        tournament_id: str,
    ) -> AsyncGenerator[ScrapedFixture, None]:
        """
        Scrape upcoming fixtures (order of play) for a tournament.

        Args:
            tournament_id: Tournament URL slug

        Yields:
            ScrapedFixture objects for upcoming matches
        """
        page = await self.new_page()

        try:
            # Get current year
            year = datetime.now().year

            # Navigate to tournament schedule/order of play
            url = f"{self.BASE_URL}/en/scores/current/{tournament_id}/daily-schedule"
            await self.navigate(page, url, wait_for="networkidle")
            await self.random_delay()

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Get tournament info
            tournament_info = {
                "id": tournament_id,
                "name": tournament_id.replace("-", " ").title(),
                "year": year,
                "level": "ATP 250",
                "surface": "Hard",
            }

            # Find match entries
            match_elements = soup.select(
                ".match-card, .schedule-match, [class*='match-item']"
            )

            for elem in match_elements:
                fixture = self._parse_fixture_element(elem, tournament_info)
                if fixture:
                    yield fixture

        finally:
            await page.close()

    def _parse_fixture_element(
        self,
        elem,
        tournament_info: dict,
    ) -> Optional[ScrapedFixture]:
        """
        Parse a fixture element from the daily schedule.

        Args:
            elem: BeautifulSoup element
            tournament_info: Tournament metadata

        Returns:
            ScrapedFixture if successfully parsed
        """
        # Find players
        player_elems = elem.select("a[href*='/players/'], .player-name")
        if len(player_elems) < 2:
            return None

        player_a = extract_player_info(player_elems[0], source="atp")
        player_b = extract_player_info(player_elems[1], source="atp")

        if not player_a or not player_b:
            return None

        # Get scheduled time
        time_elem = elem.select_one(".time, .schedule-time")
        scheduled_time = time_elem.get_text(strip=True) if time_elem else None

        # Get court
        court_elem = elem.select_one(".court, .venue")
        court = court_elem.get_text(strip=True) if court_elem else None

        # Get round
        round_elem = elem.select_one(".round, .match-round")
        round_str = round_elem.get_text(strip=True) if round_elem else "R64"

        return ScrapedFixture(
            tournament_name=tournament_info["name"],
            tournament_id=tournament_info["id"],
            tournament_year=tournament_info["year"],
            tournament_level=tournament_info["level"],
            tournament_surface=tournament_info["surface"],
            round=self._normalize_round(round_str),
            scheduled_time=scheduled_time,
            court=court,
            player_a_name=player_a.name,
            player_a_external_id=player_a.external_id,
            player_b_name=player_b.name,
            player_b_external_id=player_b.external_id,
            source="atp",
        )


def _detect_retirement_from_score(score_raw: str, current_status: str) -> str:
    """
    Detect retirement from an incomplete final set in the score.

    ATP and WTA websites don't always explicitly mark retirements.
    The only indicator is an incomplete final set where neither player
    reached 6 games (e.g. "7-5 2-1" means someone retired at 2-1 in set 2).

    A set is "incomplete" if both players have fewer than 6 games AND
    it's not a tiebreak situation (both at 6 or 7).

    Args:
        score_raw: Score string like "6-4 2-1" or "7-6(5) 3-6 4-2"
        current_status: Current status (only overrides if "completed")

    Returns:
        Updated status string
    """
    if current_status != "completed":
        return current_status

    if not score_raw:
        return current_status

    # Split into sets and check the last one
    sets = score_raw.strip().split()
    if not sets:
        return current_status

    last_set = sets[-1]

    # Match standard set score pattern: "6-4", "7-6(5)", etc.
    match = re.match(r"^(\d+)-(\d+)", last_set)
    if not match:
        return current_status

    a_games = int(match.group(1))
    b_games = int(match.group(2))

    # A set is complete if:
    # - A player reached 6+ and leads by 2+ (e.g. 6-4, 6-3, 6-0)
    # - Both reached 6 (goes to tiebreak, shown as 7-6)
    # - Both reached 6 and one won 7 (7-5 is valid but means tiebreak wasn't needed)
    # A set is incomplete (retirement) if both players are under 6
    if a_games < 6 and b_games < 6:
        return "retired"

    return current_status


# Convenience functions for simple usage
async def scrape_atp_tournament(
    tournament_id: str,
    year: int,
    tour_type: str = "main",
) -> list[ScrapedMatch]:
    """
    Convenience function to scrape a single ATP tournament.

    Args:
        tournament_id: Tournament URL slug (e.g., "australian-open")
        year: Tournament year
        tour_type: "main" for ATP main tour, "challenger" for Challenger tour

    Returns:
        List of ScrapedMatch objects
    """
    matches = []
    async with ATPScraper() as scraper:
        async for match in scraper.scrape_tournament_results(
            tournament_id, year, tour_type=tour_type
        ):
            matches.append(match)
    return matches


async def scrape_challenger_tournament(
    tournament_id: str,
    year: int,
) -> list[ScrapedMatch]:
    """
    Convenience function to scrape a single Challenger tournament.

    Args:
        tournament_id: Tournament URL slug
        year: Tournament year

    Returns:
        List of ScrapedMatch objects
    """
    return await scrape_atp_tournament(tournament_id, year, tour_type="challenger")
