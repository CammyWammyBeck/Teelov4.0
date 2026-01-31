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

from teelo.scrape.base import BaseScraper, ScrapedMatch, ScrapedFixture
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
        include_qualifying: bool = True,
        tournament_number: Optional[str] = None,
        tour_type: str = "main",
    ) -> AsyncGenerator[ScrapedMatch, None]:
        """
        Scrape all completed match results for a tournament.

        Navigates to the tournament results page and extracts all matches
        from main draw and optionally qualifying.

        Args:
            tournament_id: Tournament URL slug (e.g., "australian-open")
            year: Year of the tournament edition
            include_qualifying: Whether to include qualifying matches
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

            # Scrape qualifying if requested (navigating in same page)
            if include_qualifying:
                qual_url = f"{self.BASE_URL}/en/scores/archive/{tournament_id}/{tournament_number}/{year}/results?matchType=qualifying"

                try:
                    await self.navigate(page, qual_url, wait_for="domcontentloaded")

                    try:
                        await page.wait_for_selector(".match", timeout=10000)
                    except Exception:
                        pass  # Qualifying might not exist

                    await self.random_delay()

                    qual_html = await page.content()

                    async for match in self._parse_results_page(qual_html, tournament_info, "qualifying"):
                        yield match
                except Exception as e:
                    print(f"No qualifying results for {tournament_id}: {e}")

        finally:
            await page.close()

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

        Uses v3.0's proven selectors:
        - Match containers: class="match"
        - Round headers: class="match-header" → span
        - Player names: class="name" → a tag
        - Scores: class="score-item" → span

        Note: Deduplication is handled via external_id (which includes player IDs)
        at the calling layer (backfill script's seen_external_ids set) and
        the database unique constraint. This method simply parses and yields
        all matches found on the page.

        Args:
            html: HTML content of the results page
            tournament_info: Tournament metadata
            draw_type: 'main' or 'qualifying'

        Yields:
            ScrapedMatch objects
        """
        soup = BeautifulSoup(html, "lxml")

        # Find match containers using v3.0's working selector
        # Each match is wrapped in an element with class="match"
        match_containers = soup.find_all(class_="match")

        if not match_containers:
            print(f"Warning: No matches found on page. HTML length: {len(html)}")
            return

        match_number = 0

        for match_elem in match_containers:
            try:
                # Get round from match-header
                round_header = match_elem.find(class_="match-header")
                current_round = ""
                if round_header:
                    span = round_header.find("span")
                    if span:
                        round_text = span.get_text(strip=True)
                        current_round = self._normalize_round(round_text)

                # Parse the match
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

        # Check for walkover/retirement in the match
        status = "completed"
        match_cta = match_elem.find(class_="match-cta")
        if match_cta:
            cta_text = match_cta.get_text().lower()
            if "w/o" in cta_text or "walkover" in cta_text:
                status = "walkover"
            elif "ret" in cta_text:
                status = "retired"

        # Also check score for retirement indicator
        if score_raw and ("ret" in score_raw.lower() or "w/o" in score_raw.lower()):
            if "w/o" in score_raw.lower():
                status = "walkover"
            else:
                status = "retired"

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
