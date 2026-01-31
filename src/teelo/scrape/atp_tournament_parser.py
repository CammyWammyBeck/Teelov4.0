"""
ATP Archive Page Tournament Parser.

Parses tournament list data from the ATP results archive page
(https://www.atptour.com/en/scores/results-archive?year=YYYY).

The archive page lists all tournaments for a given year with:
- Tournament name, location, dates
- Level badge image (Grand Slam, Masters, 500, 250, etc.)
- Links to results pages (containing tournament number)

HTML structure (as of 2025):
    <div class="tournament-list">
      <ul class="events">
        <li>
          <div class="tournament-info">
            <div class="event-badge_container">
              <img class="events_banner" src="...categorystamps_250.png"/>
            </div>
            <a class="tournament__profile" href="/en/tournaments/{slug}/{number}/overview">
              <span class="name">Brisbane</span>
              <span class="venue">Brisbane, Australia | </span>
              <span class="Date">31 December, 2023 - 7 January, 2024</span>
            </a>
          </div>
          <div class="non-live-cta">
            <a class="results" href="/en/scores/archive/{slug}/{number}/{year}/results">
          </div>
        </li>
      </ul>
    </div>
"""

import re
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup


# Maps banner image filename keywords to tournament levels.
# ATP uses images like "categorystamps_250.png", "categorystamps_grandslam.png".
BANNER_LEVEL_MAP: dict[str, str] = {
    "grandslam": "Grand Slam",
    "1000": "Masters 1000",
    "500": "ATP 500",
    "250": "ATP 250",
    "challenger": "Challenger",
    "nextgen": "ATP Finals",
    "finals": "ATP Finals",
}


def parse_atp_date_range(date_text: str, year: int) -> tuple[Optional[str], Optional[str]]:
    """
    Parse an ATP archive date range string into ISO date strings.

    ATP date formats (all real examples from the archive page):
      - "31 December, 2023 - 7 January, 2024"  (cross-year)
      - "1 - 7 January, 2024"                   (same month, short form)
      - "14 - 28 January, 2024"                  (same month)
      - "29 January - 4 February, 2024"           (cross-month)

    Args:
        date_text: Raw date string from the archive page
        year: The archive year (used as fallback, but we prefer parsed year)

    Returns:
        Tuple of (start_date, end_date) as ISO strings "YYYY-MM-DD",
        or (None, None) if parsing fails.
    """
    try:
        date_text = date_text.strip()
        if not date_text:
            return None, None

        # Split on " - " (with spaces around the dash/hyphen)
        # Some pages may use en-dash (–) instead of hyphen
        parts = re.split(r"\s*[-–]\s*", date_text, maxsplit=1)
        if len(parts) != 2:
            return None, None

        left = parts[0].strip()   # e.g., "31 December, 2023" or "1" or "29 January"
        right = parts[1].strip()  # e.g., "7 January, 2024"

        # Parse the right side first — it always has "DD Month, YYYY"
        end_date = datetime.strptime(right, "%d %B, %Y")

        # Parse the left side — format varies:
        #   "31 December, 2023" — full date with year
        #   "29 January"        — day + month, no year
        #   "1"                 — day only, inherits month+year from right

        if "," in left:
            # Full date with year: "31 December, 2023"
            start_date = datetime.strptime(left, "%d %B, %Y")
        elif re.search(r"[a-zA-Z]", left):
            # Day + month without year: "29 January"
            start_parsed = datetime.strptime(left, "%d %B")
            # Determine year: if start month > end month, it's the previous year
            # (e.g., December start, January end means December of year-1)
            start_year = end_date.year
            if start_parsed.month > end_date.month:
                start_year -= 1
            start_date = start_parsed.replace(year=start_year)
        else:
            # Day only: "1" — use month and year from end_date
            start_day = int(left)
            # If start day > end day in same month, it must be the previous month
            if start_day > end_date.day:
                # Go to previous month
                if end_date.month == 1:
                    start_date = end_date.replace(year=end_date.year - 1, month=12, day=start_day)
                else:
                    start_date = end_date.replace(month=end_date.month - 1, day=start_day)
            else:
                start_date = end_date.replace(day=start_day)

        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    except (ValueError, AttributeError, TypeError):
        return None, None


def _detect_level_from_banner(src: str) -> Optional[str]:
    """
    Detect tournament level from the banner image filename.

    ATP uses images like "categorystamps_250.png", "categorystamps_grandslam.png".

    Args:
        src: Image src attribute value

    Returns:
        Level string (e.g., "ATP 250") or None if not recognized
    """
    src_lower = src.lower()
    for keyword, level in BANNER_LEVEL_MAP.items():
        if keyword in src_lower:
            return level
    return None


def parse_tournament_elements(soup: BeautifulSoup, year: int) -> list[dict]:
    """
    Parse tournament entries from an ATP archive page.

    Extracts tournament metadata including name, dates, level, and
    tournament number from the archive page HTML.

    Args:
        soup: Parsed HTML of the ATP results archive page
        year: The year being scraped

    Returns:
        List of tournament dicts with keys:
        id, name, number, level, surface, location, start_date, end_date, year
    """
    tournaments: list[dict] = []

    # The tournament list is inside <div class="tournament-list">
    tournament_list = soup.select_one(".tournament-list")
    if not tournament_list:
        return tournaments

    # Each tournament is an <li> inside <ul class="events">
    entries = tournament_list.select("ul.events > li")

    for entry in entries:
        try:
            tournament = _parse_single_tournament(entry, year)
            if tournament:
                tournaments.append(tournament)
        except Exception:
            # Don't let one bad entry stop the whole list
            continue

    return tournaments


def _parse_single_tournament(entry, year: int) -> Optional[dict]:
    """
    Parse a single tournament <li> element from the archive page.

    Args:
        entry: BeautifulSoup element for one tournament <li>
        year: The archive year

    Returns:
        Tournament dict or None if essential data is missing
    """
    # --- Tournament ID and number from results link ---
    # The results link href contains both slug and number:
    # /en/scores/archive/{slug}/{number}/{year}/results
    tournament_id = None
    tournament_number = None

    results_link = entry.select_one("a.results")
    if results_link:
        href = results_link.get("href", "")
        match = re.search(r"/scores/archive/([^/]+)/(\d+)/", href)
        if match:
            tournament_id = match.group(1)
            tournament_number = match.group(2)

    # Fallback: try the tournament profile link
    # /en/tournaments/{slug}/{number}/overview
    if not tournament_id:
        profile_link = entry.select_one("a.tournament__profile")
        if profile_link:
            href = profile_link.get("href", "")
            match = re.search(r"/tournaments/([^/]+)/(\d+)/", href)
            if match:
                tournament_id = match.group(1)
                tournament_number = match.group(2)

    # Skip entries with no tournament ID (e.g., Davis Cup with empty href)
    if not tournament_id:
        return None

    # --- Tournament name ---
    name_elem = entry.select_one("span.name")
    name = name_elem.get_text(strip=True) if name_elem else tournament_id.replace("-", " ").title()

    # --- Location ---
    # <span class="venue">Brisbane, Australia | </span>
    venue_elem = entry.select_one("span.venue")
    location = ""
    if venue_elem:
        location = venue_elem.get_text(strip=True).rstrip("| ").strip()

    # --- Dates ---
    # <span class="Date">31 December, 2023 - 7 January, 2024</span>
    # Note: capital "D" in class name
    date_elem = entry.select_one("span.Date")
    start_date = None
    end_date = None
    if date_elem:
        date_text = date_elem.get_text(strip=True)
        start_date, end_date = parse_atp_date_range(date_text, year)

    # --- Level from banner image ---
    # <img class="events_banner" src="...categorystamps_250.png"/>
    level = "ATP 250"  # Default
    banner = entry.select_one("img.events_banner")
    if banner:
        src = banner.get("src", "")
        detected = _detect_level_from_banner(src)
        if detected:
            level = detected

    return {
        "id": tournament_id,
        "name": name,
        "number": tournament_number,
        "level": level,
        "surface": "Hard",  # Can't determine from archive page, will be set later
        "location": location,
        "start_date": start_date,
        "end_date": end_date,
        "year": year,
    }
