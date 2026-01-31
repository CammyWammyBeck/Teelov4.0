"""
ITF Tennis website scraper.

Scrapes match results from itftennis.com for:
- Men's World Tennis Tour (ITF Men's)
- Women's World Tennis Tour (ITF Women's)

The ITF website has some specific challenges:
1. Cookie consent popup must be dismissed
2. Tournament list uses "More Matches" pagination
3. Player names are split into first/last name elements
4. Draw navigation uses prev/next buttons instead of single page

URLs:
- Men's calendar: https://www.itftennis.com/en/tournament-calendar/mens-world-tennis-tour-calendar/
- Women's calendar: https://www.itftennis.com/en/tournament-calendar/womens-world-tennis-tour-calendar/
- Tournament draws: {tournament_url}draws-and-results/
"""

import asyncio
import re
from datetime import datetime
from typing import AsyncGenerator, Optional

from bs4 import BeautifulSoup
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout

from teelo.scrape.base import BaseScraper, ScrapedMatch, ScrapedFixture
from teelo.scrape.parsers.score import parse_score, ScoreParseError
from teelo.scrape.parsers.player import extract_seed_from_name


class ITFScraper(BaseScraper):
    """
    Scraper for ITF Tennis website (itftennis.com).

    Handles:
    - Men's World Tennis Tour (ITF M15, M25, etc.)
    - Women's World Tennis Tour (ITF W15, W25, etc.)

    ITF tournaments are categorized by prize money level:
    - M15/W15: $15,000 prize money
    - M25/W25: $25,000 prize money
    - M50/W50: $50,000 prize money (rare)
    - M75/W75: $75,000 prize money (rare)
    - M100/W100: $100,000 prize money (rare, transitional)

    Usage:
        async with ITFScraper() as scraper:
            # Get men's tournaments for 2024
            tournaments = await scraper.get_tournament_list(2024, gender="men")

            # Get women's tournaments
            tournaments = await scraper.get_tournament_list(2024, gender="women")

            # Scrape results for a specific tournament
            async for match in scraper.scrape_tournament_results(tournament_url, tournament_info):
                print(f"{match.player_a_name} def. {match.player_b_name}")
    """

    BASE_URL = "https://www.itftennis.com"

    # ITF tournament level mapping based on prize money
    LEVEL_MAPPING = {
        "15": "ITF $15K",
        "25": "ITF $25K",
        "50": "ITF $50K",
        "75": "ITF $75K",
        "100": "ITF $100K",
    }

    # XPath for "More Matches" button (from v3.0)
    MORE_MATCHES_XPATH = '//*[@id="whatson-hero"]/div[3]/section/div/div/button'

    # Cookie consent button ID
    COOKIE_CONSENT_ID = "onetrust-accept-btn-handler"

    async def get_tournament_list(
        self,
        year: int,
        gender: str = "men",
    ) -> list[dict]:
        """
        Get list of ITF tournaments for a given year.

        Scrapes the ITF tournament calendar page. Uses "More Matches" pagination
        to load all tournaments (the page loads incrementally).

        Args:
            year: Year to get tournaments for (e.g., 2024)
            gender: "men" or "women" - determines which tour calendar to scrape

        Returns:
            List of tournament dictionaries with:
            - id: Tournament ID (from URL)
            - name: Tournament name
            - level: Prize money level (ITF $15K, $25K, etc.)
            - surface: Playing surface
            - location: City, Country
            - start_date: Tournament start date
            - url: Full URL to tournament page
            - gender: "men" or "women"
        """
        page = await self.new_page()
        tournaments = []

        try:
            # Build calendar URL based on gender
            if gender == "women":
                calendar_path = "womens-world-tennis-tour-calendar"
            else:
                calendar_path = "mens-world-tennis-tour-calendar"

            url = f"{self.BASE_URL}/en/tournament-calendar/{calendar_path}/?categories=All&startdate={year}"

            print(f"Loading ITF {gender}'s calendar for {year}...")
            await self.navigate(page, url, wait_for="domcontentloaded")
            await asyncio.sleep(2)  # Wait for JS to initialize

            # Accept cookies if prompted
            await self._accept_cookies(page)

            # Load all tournaments by clicking "More Matches" repeatedly
            await self._load_all_tournaments(page)

            # Get page content after all tournaments loaded
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Parse tournament elements
            # ITF uses tournament cards with class "tournament-card" or similar
            tourney_elements = soup.select(
                ".tournament-card, .result-item, [class*='tournament']"
            )

            print(f"Found {len(tourney_elements)} tournament elements")

            for elem in tourney_elements:
                try:
                    tournament = self._parse_tournament_element(elem, year, gender)
                    if tournament:
                        tournaments.append(tournament)
                except Exception as e:
                    print(f"Error parsing tournament element: {e}")
                    continue

        finally:
            await page.close()

        print(f"Parsed {len(tournaments)} ITF {gender}'s tournaments for {year}")
        return tournaments

    async def _accept_cookies(self, page: Page) -> None:
        """
        Accept the cookie consent popup if present.

        ITF uses OneTrust for cookie management. The popup blocks interaction
        until dismissed.
        """
        try:
            # Wait a bit for popup to appear
            await asyncio.sleep(1)

            # Try to find and click the accept button
            accept_btn = await page.wait_for_selector(
                f"#{self.COOKIE_CONSENT_ID}",
                timeout=5000,
            )
            if accept_btn:
                await accept_btn.click()
                print("Cookie consent accepted")
                await asyncio.sleep(0.5)  # Wait for popup to close
        except PlaywrightTimeout:
            # No cookie popup, continue
            pass
        except Exception as e:
            print(f"Cookie consent handling failed (non-critical): {e}")

    async def _load_all_tournaments(self, page: Page) -> None:
        """
        Load all tournaments by repeatedly clicking "More Matches" button.

        The ITF calendar page uses lazy loading - only shows ~20 tournaments
        initially and requires clicking "More Matches" to load more.

        Uses a failure counter to detect when all tournaments are loaded
        (button disappears or stops working after 3 consecutive failures).
        """
        consecutive_failures = 0
        max_failures = 3
        total_clicks = 0

        print("Loading all tournaments...")

        while consecutive_failures < max_failures:
            try:
                # Find the "More Matches" button
                more_button = await page.wait_for_selector(
                    f"xpath={self.MORE_MATCHES_XPATH}",
                    timeout=3000,
                )

                if not more_button:
                    consecutive_failures += 1
                    continue

                # Check if button is visible and enabled
                is_visible = await more_button.is_visible()
                if not is_visible:
                    consecutive_failures += 1
                    continue

                # Click the button
                await more_button.click()
                total_clicks += 1
                consecutive_failures = 0  # Reset on success

                # Wait for new content to load
                await asyncio.sleep(1.5)

                if total_clicks % 10 == 0:
                    print(f"  Clicked 'More Matches' {total_clicks} times...")

            except PlaywrightTimeout:
                consecutive_failures += 1
            except Exception as e:
                consecutive_failures += 1
                if consecutive_failures >= max_failures:
                    print(f"Stopped loading: {e}")

        print(f"Finished loading tournaments (clicked {total_clicks} times)")

    def _parse_tournament_element(
        self,
        elem,
        year: int,
        gender: str,
    ) -> Optional[dict]:
        """
        Parse a tournament element from the ITF calendar page.

        ITF tournament elements typically contain:
        - Tournament name and link
        - Location (city, country)
        - Dates
        - Surface
        - Prize money level

        Args:
            elem: BeautifulSoup element containing tournament info
            year: Tournament year
            gender: "men" or "women"

        Returns:
            Tournament dictionary or None if parsing fails
        """
        # Try to find tournament link
        link = elem.select_one("a[href*='/tournament/']")
        if not link:
            return None

        href = link.get("href", "")

        # Extract tournament ID from URL
        # Format: /en/tournament/M-ITF-USA-01A-2024/
        id_match = re.search(r"/tournament/([^/]+)", href)
        if not id_match:
            return None

        tourney_id = id_match.group(1)

        # Verify it matches the expected year
        if str(year) not in tourney_id:
            return None

        # Get tournament name
        name_elem = elem.select_one(".tournament-name, h3, h4, a")
        name = name_elem.get_text(strip=True) if name_elem else tourney_id

        # Get location
        location = ""
        location_elem = elem.select_one(".location, .tournament-location, .venue")
        if location_elem:
            location = location_elem.get_text(strip=True)

        # Get surface
        surface = "Hard"  # Default
        surface_elem = elem.select_one("[class*='surface'], .court-type")
        if surface_elem:
            surface_text = surface_elem.get_text().lower()
            surface = self._normalize_surface(surface_text)

        # Detect level from tournament name or ID
        # ITF tournaments are named like "M15 Monastir" or "W25 Trnava"
        level = "ITF"  # Default
        level_match = re.search(r"[MW](\d+)", name) or re.search(r"[MW](\d+)", tourney_id)
        if level_match:
            prize_level = level_match.group(1)
            if prize_level in self.LEVEL_MAPPING:
                level = self.LEVEL_MAPPING[prize_level]
            else:
                level = f"ITF ${prize_level}K"

        # Get dates
        start_date = None
        date_elem = elem.select_one(".dates, .tournament-dates, [class*='date']")
        if date_elem:
            date_text = date_elem.get_text(strip=True)
            # Try to parse date (various formats)
            date_match = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", date_text)
            if date_match:
                # Normalize date format
                date_str = date_match.group(1)
                try:
                    # Try different formats
                    for fmt in ["%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y-%m-%d"]:
                        try:
                            parsed_date = datetime.strptime(date_str, fmt)
                            start_date = parsed_date.strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue
                except Exception:
                    pass

        # Build full URL
        full_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"

        return {
            "id": tourney_id,
            "name": name,
            "level": level,
            "surface": surface,
            "location": location,
            "start_date": start_date,
            "year": year,
            "url": full_url,
            "gender": gender,
        }

    async def scrape_tournament_results(
        self,
        tournament_url: str,
        tournament_info: dict,
    ) -> AsyncGenerator[ScrapedMatch, None]:
        """
        Scrape all completed match results for an ITF tournament.

        Navigates through all rounds of the draw using prev/next buttons
        to collect all matches.

        Args:
            tournament_url: Full URL to tournament page
            tournament_info: Tournament metadata dict with id, name, level, etc.

        Yields:
            ScrapedMatch objects for each completed match
        """
        page = await self.new_page()

        try:
            # Navigate to draws and results page
            # Format: {tournament_url}draws-and-results/
            draws_url = tournament_url.rstrip("/") + "/draws-and-results/"
            print(f"Scraping ITF tournament: {draws_url}")

            await self.navigate(page, draws_url, wait_for="domcontentloaded")
            await asyncio.sleep(2)

            # Accept cookies if needed
            await self._accept_cookies(page)

            # Navigate through all rounds and collect HTML
            round_htmls = await self._navigate_all_rounds(page)

            # Parse matches from all rounds
            match_number = 0
            for round_html in round_htmls:
                async for match in self._parse_draw_page(
                    round_html, tournament_info, match_number
                ):
                    match_number += 1
                    match.match_number = match_number
                    yield match

        finally:
            await page.close()

    async def _navigate_all_rounds(self, page: Page) -> list[str]:
        """
        Navigate through all rounds of the draw and collect HTML.

        ITF uses prev/next buttons to navigate between rounds rather than
        showing all rounds on a single page.

        Returns:
            List of HTML strings for each round
        """
        round_htmls = []
        visited_rounds = set()

        # First, get current page HTML
        html = await page.content()
        round_htmls.append(html)

        # Detect current round
        current_round = await self._get_current_round(page)
        if current_round:
            visited_rounds.add(current_round)

        # Navigate backwards (to earlier rounds) using "prev" button
        while True:
            try:
                # Find prev round button
                prev_btn = await page.wait_for_selector(
                    "button.prev-round, [class*='prev'], a[aria-label*='previous']",
                    timeout=2000,
                )

                if not prev_btn or not await prev_btn.is_visible():
                    break

                await prev_btn.click()
                await asyncio.sleep(1.5)

                # Check if we've visited this round
                current_round = await self._get_current_round(page)
                if current_round in visited_rounds:
                    break
                if current_round:
                    visited_rounds.add(current_round)

                html = await page.content()
                round_htmls.insert(0, html)  # Insert at beginning for chronological order

            except PlaywrightTimeout:
                break
            except Exception:
                break

        # Navigate forwards (to later rounds) using "next" button
        # First, go back to the original position
        for _ in range(len(round_htmls) - 1):
            try:
                next_btn = await page.wait_for_selector(
                    "button.next-round, [class*='next'], a[aria-label*='next']",
                    timeout=2000,
                )
                if next_btn and await next_btn.is_visible():
                    await next_btn.click()
                    await asyncio.sleep(1)
            except Exception:
                break

        # Now continue forward to get remaining rounds
        while True:
            try:
                next_btn = await page.wait_for_selector(
                    "button.next-round, [class*='next'], a[aria-label*='next']",
                    timeout=2000,
                )

                if not next_btn or not await next_btn.is_visible():
                    break

                await next_btn.click()
                await asyncio.sleep(1.5)

                current_round = await self._get_current_round(page)
                if current_round in visited_rounds:
                    break
                if current_round:
                    visited_rounds.add(current_round)

                html = await page.content()
                round_htmls.append(html)

            except PlaywrightTimeout:
                break
            except Exception:
                break

        print(f"Collected HTML from {len(round_htmls)} rounds")
        return round_htmls

    async def _get_current_round(self, page: Page) -> Optional[str]:
        """
        Get the current round name from the page.

        Returns:
            Round name or None
        """
        try:
            round_elem = await page.query_selector(
                ".round-name, .current-round, [class*='round-title']"
            )
            if round_elem:
                return await round_elem.inner_text()
        except Exception:
            pass
        return None

    async def _parse_draw_page(
        self,
        html: str,
        tournament_info: dict,
        start_match_number: int,
    ) -> AsyncGenerator[ScrapedMatch, None]:
        """
        Parse matches from a draw page HTML.

        ITF draw pages show matches in a drawsheet format with:
        - Player first and last names in separate elements
        - Score shown between players
        - Winner indicated by highlighting or position

        Args:
            html: HTML content of the draw page
            tournament_info: Tournament metadata
            start_match_number: Starting match number for external ID generation

        Yields:
            ScrapedMatch objects
        """
        soup = BeautifulSoup(html, "lxml")

        # Find match containers
        # ITF uses drawsheet-widget for individual matches
        match_containers = soup.select(
            ".drawsheet-widget__match, .match-container, [class*='match-row']"
        )

        if not match_containers:
            # Try alternative selectors
            match_containers = soup.select(".match, [class*='drawsheet']")

        # Try to determine current round from page
        round_elem = soup.select_one(".round-name, .current-round, [class*='round-title']")
        current_round = "R32"  # Default
        if round_elem:
            current_round = self._normalize_round(round_elem.get_text(strip=True))

        match_number = start_match_number

        for match_elem in match_containers:
            try:
                match = self._parse_itf_match_element(
                    match_elem,
                    tournament_info,
                    current_round,
                    match_number,
                )

                if match:
                    match_number += 1
                    yield match

            except Exception as e:
                print(f"Error parsing ITF match element: {e}")
                continue

    def _parse_itf_match_element(
        self,
        match_elem,
        tournament_info: dict,
        current_round: str,
        match_number: int,
    ) -> Optional[ScrapedMatch]:
        """
        Parse a single ITF match element.

        ITF HTML structure (typical):
        - Player container with first-name and last-name spans
        - Score shown separately
        - Winner may be indicated by class or position

        Args:
            match_elem: BeautifulSoup element for the match
            tournament_info: Tournament metadata
            current_round: Current round string
            match_number: Match number for external ID

        Returns:
            ScrapedMatch if successfully parsed, None otherwise
        """
        # Find player elements
        # ITF often uses separate first/last name elements
        player_elems = match_elem.select(
            ".drawsheet-widget__player, .player, [class*='player-name']"
        )

        if len(player_elems) < 2:
            # Try to find players by first/last name pattern
            first_names = match_elem.select(".drawsheet-widget__first-name, .first-name")
            last_names = match_elem.select(".drawsheet-widget__last-name, .last-name")

            if len(first_names) >= 2 and len(last_names) >= 2:
                # Reconstruct player names
                name_a = f"{first_names[0].get_text(strip=True)} {last_names[0].get_text(strip=True)}"
                name_b = f"{first_names[1].get_text(strip=True)} {last_names[1].get_text(strip=True)}"
            else:
                return None
        else:
            # Extract names from player elements
            name_a = self._extract_itf_player_name(player_elems[0])
            name_b = self._extract_itf_player_name(player_elems[1])

        if not name_a or not name_b:
            return None

        # Skip byes
        if name_a.lower() == "bye" or name_b.lower() == "bye":
            return None

        # Extract seeds if present
        name_a, seed_a = extract_seed_from_name(name_a)
        name_b, seed_b = extract_seed_from_name(name_b)

        # Extract ITF player IDs from links
        itf_id_a = None
        itf_id_b = None

        links = match_elem.select("a[href*='/player/']")
        if len(links) >= 2:
            href_a = links[0].get("href", "")
            href_b = links[1].get("href", "")

            id_match_a = re.search(r"/player/(\d+)/", href_a)
            id_match_b = re.search(r"/player/(\d+)/", href_b)

            itf_id_a = id_match_a.group(1) if id_match_a else None
            itf_id_b = id_match_b.group(1) if id_match_b else None

        # Get score
        score_elem = match_elem.select_one(
            ".drawsheet-widget__score, .score, [class*='match-score']"
        )
        score_raw = ""
        if score_elem:
            score_raw = score_elem.get_text(strip=True)
            # Clean up score (remove extra whitespace)
            score_raw = " ".join(score_raw.split())

        # Determine winner
        # ITF often highlights winner or shows them first
        winner_name = name_a  # Default: assume first player won

        # Check for winner class
        winner_elem = match_elem.select_one(
            ".winner, [class*='winner'], .drawsheet-widget__winner"
        )
        if winner_elem:
            winner_text = winner_elem.get_text(strip=True).lower()
            if name_b.lower() in winner_text:
                winner_name = name_b

        # Determine match status
        status = "completed"
        if score_raw:
            score_lower = score_raw.lower()
            if "w/o" in score_lower or "walkover" in score_lower:
                status = "walkover"
            elif "ret" in score_lower:
                status = "retired"
            elif "def" in score_lower:
                status = "default"

        # Generate external ID using player IDs for reliable deduplication
        # Format: YYYY_TOURNEY_ROUND_PLAYERID1_PLAYERID2 (sorted for consistency)
        # Use ITF IDs if available, otherwise fall back to normalized names
        player_id_a = itf_id_a or name_a.lower().replace(" ", "-")
        player_id_b = itf_id_b or name_b.lower().replace(" ", "-")
        sorted_ids = sorted([player_id_a, player_id_b])

        external_id = f"{tournament_info['year']}_{tournament_info['id']}_{current_round}_{sorted_ids[0]}_{sorted_ids[1]}"

        return ScrapedMatch(
            external_id=external_id,
            source="itf",
            tournament_name=tournament_info["name"],
            tournament_id=tournament_info["id"],
            tournament_year=tournament_info["year"],
            tournament_level=tournament_info.get("level", "ITF"),
            tournament_surface=tournament_info.get("surface", "Hard"),
            round=current_round,
            tournament_location=tournament_info.get("location"),
            player_a_name=name_a,
            player_a_external_id=itf_id_a,
            player_a_seed=seed_a,
            player_b_name=name_b,
            player_b_external_id=itf_id_b,
            player_b_seed=seed_b,
            winner_name=winner_name,
            score_raw=score_raw,
            status=status,
        )

    def _extract_itf_player_name(self, player_elem) -> Optional[str]:
        """
        Extract player name from an ITF player element.

        Handles both combined names and separate first/last name elements.

        Args:
            player_elem: BeautifulSoup element containing player info

        Returns:
            Player name or None
        """
        # Try separate first/last name elements
        first_elem = player_elem.select_one(
            ".drawsheet-widget__first-name, .first-name"
        )
        last_elem = player_elem.select_one(
            ".drawsheet-widget__last-name, .last-name"
        )

        if first_elem and last_elem:
            first = first_elem.get_text(strip=True)
            last = last_elem.get_text(strip=True)
            return f"{first} {last}".title()

        # Try combined name
        name_elem = player_elem.select_one(".player-name, a")
        if name_elem:
            name = name_elem.get_text(strip=True)
            # ITF sometimes uses LASTNAME, Firstname format
            if "," in name:
                parts = name.split(",", 1)
                return f"{parts[1].strip()} {parts[0].strip()}".title()
            return name.title()

        # Fallback to element text
        text = player_elem.get_text(strip=True)
        if text and text.lower() != "bye":
            if "," in text:
                parts = text.split(",", 1)
                return f"{parts[1].strip()} {parts[0].strip()}".title()
            return text.title()

        return None

    async def scrape_fixtures(
        self,
        tournament_url: str,
    ) -> AsyncGenerator[ScrapedFixture, None]:
        """
        Scrape upcoming fixtures for an ITF tournament.

        ITF doesn't typically show detailed schedules far in advance,
        so this may return limited results.

        Args:
            tournament_url: Full URL to tournament page

        Yields:
            ScrapedFixture objects for upcoming matches
        """
        # ITF fixture scraping is less developed than results
        # For now, yield nothing - can be implemented later if needed
        return
        yield  # Make this a generator


# Convenience functions
async def scrape_itf_tournament(
    tournament_url: str,
    tournament_info: dict,
) -> list[ScrapedMatch]:
    """
    Convenience function to scrape a single ITF tournament.

    Args:
        tournament_url: Full URL to tournament page
        tournament_info: Dict with id, name, level, surface, year, gender

    Returns:
        List of ScrapedMatch objects
    """
    matches = []
    async with ITFScraper() as scraper:
        async for match in scraper.scrape_tournament_results(tournament_url, tournament_info):
            matches.append(match)
    return matches


async def get_itf_tournaments(
    year: int,
    gender: str = "men",
) -> list[dict]:
    """
    Convenience function to get ITF tournaments for a year.

    Args:
        year: Year to get tournaments for
        gender: "men" or "women"

    Returns:
        List of tournament dictionaries
    """
    async with ITFScraper() as scraper:
        return await scraper.get_tournament_list(year, gender)
