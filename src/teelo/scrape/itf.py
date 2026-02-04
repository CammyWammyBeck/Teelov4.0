"""
ITF Tennis website scraper.

Scrapes match results from itftennis.com for:
- Men's World Tennis Tour (ITF Men's)
- Women's World Tennis Tour (ITF Women's)

ITF draw pages show 3 rounds at a time in a carousel. A 32-draw tournament
has 5 rounds (1st Round, 2nd Round, Quarter-finals, Semi-finals, Final)
totalling 31 matches (16+8+4+2+1). We scrape by:
1. Capturing the initial view (rounds 1-3)
2. Clicking next to reveal round 4
3. Clicking next again to reveal round 5

HTML structure per match (.drawsheet-widget):
- .drawsheet-widget__team-info--team-1 / --team-2 (with .is-winner class on winner)
- .player-wrapper a (contains first/last name spans and ITF ID in href)
- .drawsheet-widget__score spans (one per set, per player)
- .drawsheet-widget__seeding (e.g. "[1]")
- .drawsheet-widget__winner-status-desc (e.g. "Retired", "Walkover")
- .drawsheet-widget__nationality .itf-flags--XXX (country IOC code)

URLs:
- Men's calendar: https://www.itftennis.com/en/tournament-calendar/mens-world-tennis-tour-calendar/
- Women's calendar: https://www.itftennis.com/en/tournament-calendar/womens-world-tennis-tour-calendar/
- Tournament draws: {tournament_url}draws-and-results/
"""

import asyncio
import re
from datetime import datetime, timedelta
from typing import AsyncGenerator, Optional

from bs4 import BeautifulSoup
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout

from teelo.scrape.base import BaseScraper, ScrapedMatch, ScrapedFixture, ScrapedDrawEntry

# Round name normalization for ITF 32-draw tournaments.
# ITF uses "1st Round", "2nd Round" etc. which map to standard codes.
ITF_ROUND_MAP = {
    "1st round": "R32",
    "2nd round": "R16",
    "quarter-finals": "QF",
    "quarterfinals": "QF",
    "semi-finals": "SF",
    "semifinals": "SF",
    "final": "F",
}

# ITF tournament level mapping based on prize money
LEVEL_MAPPING = {
    "15": "ITF $15K",
    "25": "ITF $25K",
    "50": "ITF $50K",
    "75": "ITF $75K",
    "100": "ITF $100K",
}


class ITFScraper(BaseScraper):
    """
    Scraper for ITF Tennis website (itftennis.com).

    Handles Men's and Women's World Tennis Tour tournaments.
    ITF tournaments are 32-draw with 5 rounds and 31 total matches.

    Usage:
        async with ITFScraper() as scraper:
            tournaments = await scraper.get_tournament_list(2024, gender="men")
            async for match in scraper.scrape_tournament_results(tournament_url, tournament_info):
                print(f"{match.winner_name} d. opponent {match.score_raw}")
    """

    BASE_URL = "https://www.itftennis.com"

    # XPath for "More Matches" button on calendar page
    MORE_MATCHES_XPATH = '//*[@id="whatson-hero"]/div[3]/section/div/div/button'

    async def _accept_cookies(self, page: Page) -> None:
        """Dismiss the OneTrust cookie consent popup if present."""
        try:
            btn = await page.wait_for_selector(
                "#onetrust-accept-btn-handler", timeout=4000
            )
            if btn:
                await btn.click()
                try:
                    await page.wait_for_selector(
                        "#onetrust-accept-btn-handler",
                        state="detached",
                        timeout=4000,
                    )
                except PlaywrightTimeout:
                    pass
        except PlaywrightTimeout:
            pass
        except Exception:
            pass

    # =========================================================================
    # Tournament results scraping
    # =========================================================================

    async def scrape_tournament_results(
        self,
        tournament_url: str,
        tournament_info: dict,
    ) -> AsyncGenerator[ScrapedMatch, None]:
        """
        Scrape all completed match results for an ITF tournament.

        Navigates through the draw carousel (3 views for a 32-draw tournament)
        and parses matches from each visible round container.

        Args:
            tournament_url: Full URL to tournament page
            tournament_info: Dict with id, name, level, surface, year, gender, etc.

        Yields:
            ScrapedMatch for each completed match (including walkovers/retirements)
        """
        page = await self.new_page()

        try:
            draws_url = tournament_url.rstrip("/") + "/draws-and-results/"
            print(f"Scraping ITF tournament: {draws_url}")

            await self.navigate(page, draws_url, wait_for="domcontentloaded")
            try:
                await page.wait_for_selector(
                    ".drawsheet-round-container, .drawsheet-widget",
                    timeout=4000,
                )
            except PlaywrightTimeout:
                pass
            await self._accept_cookies(page)

            all_matches = []
            seen_rounds = set()
            match_number = 0

            # Collect matches from up to 3 carousel views
            # View 0: rounds 1-3 (R32, R16, QF)
            # View 1: rounds 2-4 (R16, QF, SF) - we skip already-seen rounds
            # View 2: rounds 3-5 (QF, SF, F) - we skip already-seen rounds
            for view_idx in range(3):
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")

                for container in soup.select(".drawsheet-round-container"):
                    title_elem = container.select_one(
                        ".drawsheet-round-container__round-title"
                    )
                    if not title_elem:
                        continue

                    round_name = _normalize_round(title_elem.get_text(strip=True))
                    if round_name in seen_rounds:
                        continue
                    seen_rounds.add(round_name)

                    # Parse all match widgets in this round container
                    for widget in container.select(".drawsheet-widget"):
                        match = _parse_match_widget(
                            widget, round_name, tournament_info, match_number
                        )
                        if match:
                            match_number += 1
                            match.match_number = match_number
                            yield match

                # Click next to advance the carousel
                if view_idx < 2:
                    try:
                        next_btn = await page.wait_for_selector(
                            "button.btn--chevron-next", timeout=4000
                        )
                        if next_btn and await next_btn.is_visible():
                            await next_btn.click()
                            try:
                                await page.wait_for_load_state("networkidle", timeout=4000)
                            except PlaywrightTimeout:
                                pass
                        else:
                            break
                    except PlaywrightTimeout:
                        break

            print(f"Scraped {match_number} matches from {len(seen_rounds)} rounds")

        finally:
            await page.close()

    # =========================================================================
    # Tournament list scraping (calendar page)
    # =========================================================================

    async def get_tournament_list(
        self,
        year: int,
        gender: str = "men",
    ) -> list[dict]:
        """
        Get list of ITF tournaments for a given year.

        Scrapes the ITF tournament calendar page, clicking "More Matches"
        to load all tournaments.

        Args:
            year: Year to get tournaments for (e.g., 2024)
            gender: "men" or "women"

        Returns:
            List of tournament dicts with id, name, level, surface, url, etc.
        """
        page = await self.new_page()
        tournaments = []

        try:
            if gender == "women":
                calendar_path = "womens-world-tennis-tour-calendar"
            else:
                calendar_path = "mens-world-tennis-tour-calendar"

            url = f"{self.BASE_URL}/en/tournament-calendar/{calendar_path}/?categories=All&startdate={year}"
            print(f"Loading ITF {gender}'s calendar for {year}...")
            await self.navigate(page, url, wait_for="domcontentloaded")
            try:
                await page.wait_for_selector("a[href*='/tournament/']", timeout=4000)
            except PlaywrightTimeout:
                pass
            await self._accept_cookies(page)

            # Load all tournaments by clicking "More Matches" repeatedly
            await self._load_all_tournaments(page)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Find tournament links in the calendar
            for link in soup.select("a[href*='/tournament/']"):
                href = link.get("href", "")
                # Skip non-tournament links (media, etc.)
                if "draws-and-results" in href or "media" in href:
                    continue
                if f"/{year}/" not in href and str(year) not in href:
                    continue

                tournament = self._parse_tournament_link(link, href, year, gender)
                if tournament:
                    tournaments.append(tournament)

        finally:
            await page.close()

        print(f"Parsed {len(tournaments)} ITF {gender}'s tournaments for {year}")
        return tournaments

    async def _load_all_tournaments(self, page: Page) -> None:
        """Click "More Matches" button repeatedly to load all tournaments."""
        consecutive_failures = 0
        total_clicks = 0

        while consecutive_failures < 3:
            try:
                more_button = await page.wait_for_selector(
                    f"xpath={self.MORE_MATCHES_XPATH}", timeout=4000
                )
                if not more_button or not await more_button.is_visible():
                    consecutive_failures += 1
                    continue

                await more_button.click()
                total_clicks += 1
                consecutive_failures = 0
                try:
                    await page.wait_for_load_state("networkidle", timeout=4000)
                except PlaywrightTimeout:
                    pass

                if total_clicks % 10 == 0:
                    print(f"  Clicked 'More Matches' {total_clicks} times...")

            except PlaywrightTimeout:
                consecutive_failures += 1
            except Exception:
                consecutive_failures += 1

        print(f"Finished loading tournaments (clicked {total_clicks} times)")

    def _parse_tournament_link(
        self, link, href: str, year: int, gender: str
    ) -> Optional[dict]:
        """Parse a tournament link element from the calendar page."""
        # Extract tournament ID from URL path
        # e.g. /en/tournament/m25-monastir/tun/2024/m-itf-tun-2024-064/
        parts = href.strip("/").split("/")
        if len(parts) < 2:
            return None
        tourney_id = parts[-1]

        # Get tournament name from link text
        # ITF links often have .short and .long child spans — prefer .short
        short_el = link.select_one(".short")
        if short_el:
            name = short_el.get_text(strip=True)
        else:
            name = link.get_text(strip=True)
        if not name:
            name = tourney_id

        # Detect level from name (e.g. "M15 Monastir" -> ITF $15K)
        level = "ITF"
        level_match = re.search(r"[MW](\d+)", name) or re.search(
            r"[MW](\d+)", tourney_id
        )
        if level_match:
            prize_level = level_match.group(1)
            level = LEVEL_MAPPING.get(prize_level, f"ITF ${prize_level}K")

        full_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"

        # Extract start date from table row
        start_date = None
        try:
            tr = link.find_parent("tr")
            if tr:
                date_span = tr.select_one("td.date span.date")
                if date_span:
                    # Format: "01 Jan to 07 Jan 2024"
                    full_text = date_span.get_text(strip=True)
                    parts = full_text.split(" to ")
                    
                    if parts:
                        start_part = parts[0].strip()  # "01 Jan"
                        
                        # Infer year from the end of the full string
                        year_match = re.search(r"(\d{4})$", full_text)
                        year_to_use = int(year_match.group(1)) if year_match else year
                        
                        # Check if start_part already has year (rare but possible)
                        if re.search(r"\d{4}", start_part):
                            dt = datetime.strptime(start_part, "%d %b %Y")
                        else:
                            dt = datetime.strptime(f"{start_part} {year_to_use}", "%d %b %Y")
                            
                            # Handle Dec-Jan rollover
                            # If start is Dec and end is Jan, start year should be year-1
                            if dt.month == 12 and len(parts) > 1 and "Jan" in parts[1]:
                                dt = dt.replace(year=year_to_use - 1)
                        
                        start_date = dt.strftime("%Y-%m-%d")
        except Exception:
            # Ignore date parsing errors, just leave start_date as None
            pass

        return {
            "id": tourney_id,
            "name": name,
            "level": level,
            "surface": "Hard",  # Default, can be enriched later
            "location": "",
            "start_date": start_date,
            "year": year,
            "url": full_url,
            "gender": gender,
        }

    # =========================================================================
    # Tournament draw scraping
    # =========================================================================

    async def scrape_tournament_draw(
        self,
        tournament_url: str,
        tournament_info: dict,
    ) -> list[ScrapedDrawEntry]:
        """
        Scrape the full draw bracket for an ITF tournament.

        Navigates through the draw carousel to capture all match slots, including
        byes and upcoming matches.

        Args:
            tournament_url: Full URL to tournament page
            tournament_info: Dict with id, name, level, surface, year, gender

        Returns:
            List of ScrapedDrawEntry objects
        """
        page = await self.new_page()
        entries = []

        try:
            draws_url = tournament_url.rstrip("/") + "/draws-and-results/"
            print(f"Scraping ITF draw: {draws_url}")

            await self.navigate(page, draws_url, wait_for="domcontentloaded")
            try:
                await page.wait_for_selector(
                    ".drawsheet-round-container, .drawsheet-widget",
                    timeout=4000,
                )
            except PlaywrightTimeout:
                pass
            await self._accept_cookies(page)

            seen_rounds = set()
            
            # Carousel iteration (similar to results scraping)
            for view_idx in range(3):
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")

                for container in soup.select(".drawsheet-round-container"):
                    title_elem = container.select_one(
                        ".drawsheet-round-container__round-title"
                    )
                    if not title_elem:
                        continue

                    round_name = _normalize_round(title_elem.get_text(strip=True))
                    if round_name in seen_rounds:
                        continue
                    seen_rounds.add(round_name)

                    # Parse all widgets in this round
                    # ITF displays matches in vertical order, so index+1 is the draw position
                    widgets = container.select(".drawsheet-widget")
                    print(f"  Round {round_name}: {len(widgets)} slots")

                    for i, widget in enumerate(widgets):
                        draw_position = i + 1
                        entry = _parse_draw_entry_widget(
                            widget, round_name, draw_position, tournament_info
                        )
                        if entry:
                            entries.append(entry)

                # Click next
                if view_idx < 2:
                    try:
                        next_btn = await page.wait_for_selector(
                            "button.btn--chevron-next", timeout=4000
                        )
                        if next_btn and await next_btn.is_visible():
                            await next_btn.click()
                            try:
                                await page.wait_for_load_state("networkidle", timeout=4000)
                            except PlaywrightTimeout:
                                pass
                        else:
                            break
                    except PlaywrightTimeout:
                        break

        finally:
            await page.close()

        print(f"Scraped {len(entries)} draw entries from {tournament_info['id']}")
        return entries

    # =========================================================================
    # Fixtures (Order of Play)
    # =========================================================================

    async def scrape_fixtures(
        self,
        tournament_url: str,
    ) -> AsyncGenerator[ScrapedFixture, None]:
        """
        Scrape upcoming fixtures from the ITF Order of Play page.

        URL: {tournament_url}/order-of-play/

        Matches are grouped by court in .orderop-widget-container elements.
        Each match is an .orderop-widget.
        """
        page = await self.new_page()
        
        # Construct OOP URL
        # tournament_url usually ends with / e.g. .../m-itf-gbr-2026-001/
        oop_url = tournament_url.rstrip("/") + "/order-of-play/"
        
        try:
            print(f"Scraping ITF schedule: {oop_url}")
            await self.navigate(page, oop_url, wait_for="domcontentloaded")
            try:
                await page.wait_for_selector(".orderop-widget-container", timeout=4000)
            except PlaywrightTimeout:
                pass
            await self._accept_cookies(page)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Iterate over courts
            court_containers = soup.select(".orderop-widget-container")
            print(f"Found {len(court_containers)} courts with scheduled matches")

            for container in court_containers:
                # Court Name
                court_name = "Unknown Court"
                court_header = container.select_one(".orderop-widget-container__court-name")
                if court_header:
                    court_name = court_header.get_text(strip=True)

                # Matches on this court
                widgets = container.select(".orderop-widget")
                last_dt: Optional[datetime] = None
                
                for widget in widgets:
                    time_info = self._extract_oop_datetime(widget)

                    is_doubles = widget.select_one(".orderop-widget__team-doubles") is not None

                    effective_date = time_info.get("date")
                    effective_time = time_info.get("time")
                    if time_info.get("followed_by") and last_dt:
                        est_dt = last_dt + timedelta(hours=2)
                        effective_date = est_dt.strftime("%Y-%m-%d")
                        effective_time = est_dt.strftime("%H:%M")

                    if effective_date and effective_time:
                        try:
                            last_dt = datetime.strptime(
                                f"{effective_date} {effective_time}",
                                "%Y-%m-%d %H:%M",
                            )
                        except ValueError:
                            pass

                    if is_doubles:
                        continue

                    fixture = self._parse_fixture_widget(
                        widget,
                        court_name,
                        tournament_url,
                        date_str=effective_date,
                        time_str=effective_time,
                    )
                    if fixture:
                        if effective_date:
                            fixture.scheduled_date = effective_date
                        if effective_time:
                            fixture.scheduled_time = effective_time
                        yield fixture

        finally:
            await page.close()

    def _parse_fixture_widget(
        self,
        widget,
        court_name: str,
        tournament_url: str,
        date_str: Optional[str] = None,
        time_str: Optional[str] = None,
    ) -> Optional[ScrapedFixture]:
        """Parse a single match widget from the schedule."""
        # Date & Time (optionally provided by caller)
        if date_str is None or time_str is None:
            extracted = self._extract_oop_datetime(widget)
            if date_str is None:
                date_str = extracted["date"]
            if time_str is None:
                time_str = extracted["time"]

        # Round
        round_code = "R32" # Default
        round_elem = widget.select_one(".orderop-widget__round-details")
        if round_elem:
            raw_round = round_elem.get_text(strip=True)
            # Remove "Men's Singles" etc.
            raw_round = raw_round.replace("Men's Singles", "").replace("Women's Singles", "").strip()
            round_code = _normalize_round(raw_round)

        # Players
        team1 = widget.select_one(".orderop-widget__team-info--team-1")
        team2 = widget.select_one(".orderop-widget__team-info--team-2")
        
        if not team1 or not team2:
            return None

        def extract_oop_player(team_div):
            name_el = team_div.select_one(".orderop-widget__first-name")
            last_el = team_div.select_one(".orderop-widget__last-name")
            if name_el and last_el:
                name = f"{name_el.get_text(strip=True)} {last_el.get_text(strip=True)}"
            else:
                # Fallback
                name = team_div.get_text(strip=True)
            
            # Seed
            seed = None
            seed_el = team_div.select_one(".orderop-widget__seeding")
            if seed_el:
                s_match = re.search(r"\[(\d+)\]", seed_el.get_text(strip=True))
                if s_match:
                    seed = int(s_match.group(1))
            
            # Nationality is text in nationality div
            # No clear way to get ITF ID from OOP page (usually no links)
            return name, seed

        name_a, seed_a = extract_oop_player(team1)
        name_b, seed_b = extract_oop_player(team2)

        # Extract tournament info from URL as fallback
        # .../tournament/m25-sheffield/gbr/2026/m-itf-gbr-2026-001/
        parts = tournament_url.strip("/").split("/")
        tourney_id = parts[-1] if parts else ""
        year = 2026 # Fallback if not in URL
        for p in parts:
            if p.isdigit() and len(p) == 4:
                year = int(p)
                break

        return ScrapedFixture(
            tournament_name=tourney_id, # Placeholder
            tournament_id=tourney_id,
            tournament_year=year,
            tournament_level="ITF",
            tournament_surface="", # Unknown from OOP
            round=round_code,
            scheduled_date=date_str,
            scheduled_time=time_str,
            court=court_name,
            player_a_name=name_a,
            player_a_seed=seed_a,
            player_b_name=name_b,
            player_b_seed=seed_b,
            source="itf",
        )

    def _extract_oop_datetime(self, widget) -> dict:
        """
        Extract date/time and followed-by status from an OOP widget.

        Returns:
            dict with keys: date (YYYY-MM-DD), time (HH:MM), followed_by (bool)
        """
        date_str = None
        time_str = None
        followed_by = False

        date_elem = widget.select_one(".orderop-widget__date")
        if date_elem:
            try:
                raw_date = date_elem.get_text(strip=True)
                dt = datetime.strptime(raw_date, "%A %d %B %Y")
                date_str = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        time_elem = widget.select_one(".orderop-widget__start-time")
        raw_time = ""
        if time_elem:
            raw_time = time_elem.get_text(" ", strip=True)
            lower = raw_time.lower()
            if "followed by" in lower:
                followed_by = True
            time_match = re.search(r"(\d{1,2}:\d{2})", raw_time)
            if time_match:
                time_str = time_match.group(1)

        return {"date": date_str, "time": time_str, "followed_by": followed_by}


# =============================================================================
# Module-level parsing functions (pure functions, no async)
# =============================================================================


def _normalize_round(raw: str) -> str:
    """Normalize ITF round name to standard code (R32, R16, QF, SF, F)."""
    return ITF_ROUND_MAP.get(raw.lower().strip(), raw.upper())


def _parse_match_widget(
    widget,
    round_name: str,
    tournament_info: dict,
    match_number: int,
) -> Optional[ScrapedMatch]:
    """
    Parse a single match from a .drawsheet-widget element.

    Each widget contains two team-info sections (team-1 and team-2),
    each with player name, nationality, seed, and set scores.
    The winner has the .is-winner class on their team-info element.

    Args:
        widget: BeautifulSoup element for the match widget
        round_name: Normalized round code (R32, R16, etc.)
        tournament_info: Tournament metadata dict
        match_number: Sequential match number

    Returns:
        ScrapedMatch if parseable, None for BYEs or unparseable matches
    """
    team1 = widget.select_one(".drawsheet-widget__team-info--team-1")
    team2 = widget.select_one(".drawsheet-widget__team-info--team-2")

    if not team1 or not team2:
        return None

    player_a = _extract_player(team1)
    player_b = _extract_player(team2)

    if not player_a or not player_b:
        return None

    # Skip BYEs
    if player_a["name"].lower() == "bye" or player_b["name"].lower() == "bye":
        return None

    # Determine winner from is-winner class
    winner_name = None
    if "is-winner" in (team1.get("class") or []):
        winner_name = player_a["name"]
    elif "is-winner" in (team2.get("class") or []):
        winner_name = player_b["name"]

    # Extract set scores from each team's score spans
    scores_a = [s.get_text(strip=True) for s in team1.select(".drawsheet-widget__score")]
    scores_b = [s.get_text(strip=True) for s in team2.select(".drawsheet-widget__score")]

    # Check if all scores are empty (ITF shows empty spans for walkovers)
    has_scores = any(s for s in scores_a) or any(s for s in scores_b)

    # Build score string
    score_raw = _build_score(scores_a, scores_b) if has_scores else ""

    # Check for retirement/walkover status
    # The status description appears as .drawsheet-widget__winner-status-desc
    status = "completed"
    status_elem = widget.select_one(".drawsheet-widget__winner-status-desc")
    if status_elem:
        status_text = status_elem.get_text(strip=True).lower()
        if "retired" in status_text or "ret" in status_text:
            status = "retired"
        elif "walkover" in status_text or "w/o" in status_text:
            status = "walkover"
        elif "default" in status_text:
            status = "default"
    elif not has_scores and winner_name:
        # No real scores but a winner -> walkover
        status = "walkover"
        score_raw = "W/O"

    # Generate external ID for deduplication
    # Use sorted ITF IDs (or name slugs as fallback) for consistency
    id_a = player_a["itf_id"] or player_a["name"].lower().replace(" ", "-")
    id_b = player_b["itf_id"] or player_b["name"].lower().replace(" ", "-")
    sorted_ids = sorted([id_a, id_b])
    external_id = (
        f"{tournament_info['year']}_{tournament_info['id']}"
        f"_{round_name}_{sorted_ids[0]}_{sorted_ids[1]}"
    )

    return ScrapedMatch(
        external_id=external_id,
        source="itf",
        tournament_name=tournament_info["name"],
        tournament_id=tournament_info["id"],
        tournament_year=tournament_info["year"],
        tournament_level=tournament_info.get("level", "ITF"),
        tournament_surface=tournament_info.get("surface", "Hard"),
        round=round_name,
        tournament_location=tournament_info.get("location"),
        player_a_name=player_a["name"],
        player_a_external_id=player_a["itf_id"],
        player_a_nationality=player_a["nationality"],
        player_a_seed=player_a["seed"],
        player_b_name=player_b["name"],
        player_b_external_id=player_b["itf_id"],
        player_b_nationality=player_b["nationality"],
        player_b_seed=player_b["seed"],
        winner_name=winner_name,
        score_raw=score_raw,
        status=status,
        match_number=match_number,
    )


def _extract_player(team_info) -> Optional[dict]:
    """
    Extract player info from a .drawsheet-widget__team-info element.

    Returns dict with name, itf_id, nationality, seed, or None if no player found.
    """
    player_link = team_info.select_one(".player-wrapper a")
    if not player_link:
        return None

    # Name from first/last name spans
    first = player_link.select_one(".drawsheet-widget__first-name")
    last = player_link.select_one(".drawsheet-widget__last-name")
    if first and last:
        name = f"{first.get_text(strip=True)} {last.get_text(strip=True)}"
    else:
        name = player_link.get_text(strip=True)

    if not name:
        return None

    # ITF ID from player link href: /en/players/name/800399810/country/mt/s/
    itf_id = None
    href = player_link.get("href", "")
    id_match = re.search(r"/players/[^/]+/(\d+)/", href)
    if id_match:
        itf_id = id_match.group(1)

    # Nationality from flag class: itf-flags--RUS -> RUS
    nationality = None
    flag = team_info.select_one(".drawsheet-widget__nationality .itf-flags")
    if flag:
        for cls in flag.get("class", []):
            if cls.startswith("itf-flags--"):
                nationality = cls.replace("itf-flags--", "")

    # Seed from [N] in seeding span
    seed = None
    seed_elem = team_info.select_one(".drawsheet-widget__seeding")
    if seed_elem:
        seed_match = re.search(r"\[(\d+)\]", seed_elem.get_text(strip=True))
        if seed_match:
            seed = int(seed_match.group(1))

    return {"name": name, "itf_id": itf_id, "nationality": nationality, "seed": seed}


def _build_score(scores_a: list[str], scores_b: list[str]) -> str:
    """
    Build a score string like "6-4 3-6 7-6(5)" from per-set score lists.

    Handles ITF's tiebreak compression where e.g. player A has "64" and
    player B has "7", meaning the set was 6-7 with a tiebreak lost 4.
    """
    if not scores_a and not scores_b:
        return ""

    sets = []
    for sa, sb in zip(scores_a, scores_b):
        da = "".join(filter(str.isdigit, sa))
        db = "".join(filter(str.isdigit, sb))

        if not da or not db:
            sets.append(f"{sa}-{sb}")
            continue

        # Tiebreak: "64" vs "7" means 6-7(4), "76" vs "6" means 7-6(6) etc.
        if len(da) > 1 and da[0] == "6" and db == "7":
            sets.append(f"6-7({da[1:]})")
        elif len(db) > 1 and db[0] == "6" and da == "7":
            sets.append(f"7-6({db[1:]})")
        else:
            sets.append(f"{da}-{db}")

    return " ".join(sets)


def _parse_draw_entry_widget(
    widget,
    round_name: str,
    draw_position: int,
    tournament_info: dict,
) -> Optional[ScrapedDrawEntry]:
    """
    Parse a draw widget into a ScrapedDrawEntry.
    """
    team1 = widget.select_one(".drawsheet-widget__team-info--team-1")
    team2 = widget.select_one(".drawsheet-widget__team-info--team-2")
    
    if not team1 or not team2:
        return None

    # Check for BYEs
    player_a = _extract_player(team1)
    player_b = _extract_player(team2)
    
    is_bye = False
    if (player_a and player_a["name"].lower() == "bye") or \
       (player_b and player_b["name"].lower() == "bye"):
        is_bye = True
        
    # If not a bye, and both players missing, might be empty slot (TBD vs TBD)
    if not is_bye and not player_a and not player_b:
        # Keep it as TBD entry
        pass

    # Extract score if completed
    score_raw = None
    winner_name = None
    
    if not is_bye:
        scores_a = [s.get_text(strip=True) for s in team1.select(".drawsheet-widget__score")]
        scores_b = [s.get_text(strip=True) for s in team2.select(".drawsheet-widget__score")]
        has_scores = any(s for s in scores_a) or any(s for s in scores_b)
        
        if has_scores:
            score_raw = _build_score(scores_a, scores_b)
            
        # Winner
        if "is-winner" in (team1.get("class") or []):
            winner_name = player_a["name"] if player_a else None
        elif "is-winner" in (team2.get("class") or []):
            winner_name = player_b["name"] if player_b else None

    return ScrapedDrawEntry(
        round=round_name,
        draw_position=draw_position,
        player_a_name=player_a["name"] if player_a else None,
        player_a_external_id=player_a["itf_id"] if player_a else None,
        player_a_seed=player_a["seed"] if player_a else None,
        player_b_name=player_b["name"] if player_b else None,
        player_b_external_id=player_b["itf_id"] if player_b else None,
        player_b_seed=player_b["seed"] if player_b else None,
        score_raw=score_raw,
        winner_name=winner_name,
        is_bye=is_bye,
        source="itf",
        tournament_name=tournament_info["name"],
        tournament_id=tournament_info["id"],
        tournament_year=tournament_info["year"],
        tournament_level=tournament_info.get("level", "ITF"),
        tournament_surface=tournament_info.get("surface", "Hard"),
    )



# =============================================================================
# Convenience functions
# =============================================================================


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
        async for match in scraper.scrape_tournament_results(
            tournament_url, tournament_info
        ):
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
