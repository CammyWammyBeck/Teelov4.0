"""
Player information extraction utilities.

Extracts player data from various HTML structures found on
tennis websites (ATP, WTA, ITF, betting sites).
"""

import re
from dataclasses import dataclass
from typing import Optional

from bs4 import BeautifulSoup, Tag


@dataclass
class ExtractedPlayer:
    """
    Player information extracted from HTML.

    Contains all available fields - some may be None depending
    on what information was available in the source.
    """
    name: str
    external_id: Optional[str] = None
    nationality_ioc: Optional[str] = None
    seed: Optional[int] = None
    ranking: Optional[int] = None
    profile_url: Optional[str] = None

    def __repr__(self) -> str:
        return f"<ExtractedPlayer(name='{self.name}', id={self.external_id})>"


def extract_player_info(
    element: Tag,
    source: str = "atp",
) -> Optional[ExtractedPlayer]:
    """
    Extract player information from an HTML element.

    Different sources use different HTML structures, so this function
    dispatches to source-specific extractors.

    Args:
        element: BeautifulSoup Tag containing player info
        source: Data source ('atp', 'wta', 'itf', 'sportsbet')

    Returns:
        ExtractedPlayer if extraction successful, None otherwise
    """
    extractors = {
        "atp": _extract_atp_player,
        "wta": _extract_wta_player,
        "itf": _extract_itf_player,
        "sportsbet": _extract_sportsbet_player,
    }

    extractor = extractors.get(source, _extract_generic_player)
    return extractor(element)


def _extract_atp_player(element: Tag) -> Optional[ExtractedPlayer]:
    """
    Extract player info from ATP website HTML structure.

    ATP player elements typically look like:
    <a href="/en/players/novak-djokovic/D0AG/overview" class="player-name">
        Novak Djokovic
    </a>

    The player ID is in the URL path (D0AG in this example).
    """
    # Try to find player link
    link = element.find("a", class_=re.compile(r"player|name", re.I))
    if not link:
        link = element.find("a", href=re.compile(r"/players/"))

    if link:
        name = link.get_text(strip=True)
        href = link.get("href", "")

        # Extract player ID from URL
        # Format: /en/players/player-name/ABCD/overview
        id_match = re.search(r"/players/[^/]+/([A-Z0-9]+)", href)
        external_id = id_match.group(1) if id_match else None

        # Try to find nationality (often as flag icon)
        flag = element.find("img", class_=re.compile(r"flag|country", re.I))
        nationality = None
        if flag:
            # Nationality often in class name or alt text
            flag_class = " ".join(flag.get("class", []))
            flag_match = re.search(r"flag-([A-Z]{3})", flag_class, re.I)
            if flag_match:
                nationality = flag_match.group(1).upper()
            elif flag.get("alt"):
                nationality = flag.get("alt")[:3].upper()

        # Try to find seed
        seed = None
        seed_elem = element.find(class_=re.compile(r"seed", re.I))
        if seed_elem:
            seed_text = seed_elem.get_text(strip=True)
            seed_match = re.search(r"\d+", seed_text)
            if seed_match:
                seed = int(seed_match.group())

        return ExtractedPlayer(
            name=name,
            external_id=external_id,
            nationality_ioc=nationality,
            seed=seed,
            profile_url=href if href.startswith("http") else f"https://www.atptour.com{href}",
        )

    # Fallback: just get text content
    text = element.get_text(strip=True)
    if text:
        return ExtractedPlayer(name=text)

    return None


def _extract_wta_player(element: Tag) -> Optional[ExtractedPlayer]:
    """
    Extract player info from WTA website HTML structure.

    WTA uses similar structure to ATP but with different class names.
    """
    # Try to find player link
    link = element.find("a", href=re.compile(r"/players/"))

    if link:
        name = link.get_text(strip=True)
        href = link.get("href", "")

        # Extract player ID from URL
        # Format varies - try to find numeric or alphanumeric ID
        id_match = re.search(r"/players/[^/]+/(\d+)", href)
        external_id = id_match.group(1) if id_match else None

        return ExtractedPlayer(
            name=name,
            external_id=external_id,
            profile_url=href if href.startswith("http") else f"https://www.wtatennis.com{href}",
        )

    # Fallback
    text = element.get_text(strip=True)
    if text:
        return ExtractedPlayer(name=text)

    return None


def _extract_itf_player(element: Tag) -> Optional[ExtractedPlayer]:
    """
    Extract player info from ITF website HTML structure.

    ITF often uses LASTNAME, Firstname format.
    """
    link = element.find("a", href=re.compile(r"/player/"))

    if link:
        name = link.get_text(strip=True)
        href = link.get("href", "")

        # Convert LASTNAME, Firstname to Firstname Lastname
        if "," in name:
            parts = name.split(",", 1)
            name = f"{parts[1].strip()} {parts[0].strip()}"

        # Extract player ID
        id_match = re.search(r"/player/(\d+)/", href)
        external_id = id_match.group(1) if id_match else None

        # ITF often shows nationality as 3-letter code
        nat_elem = element.find(class_=re.compile(r"nationality|country", re.I))
        nationality = None
        if nat_elem:
            nat_text = nat_elem.get_text(strip=True)
            if len(nat_text) == 3 and nat_text.isalpha():
                nationality = nat_text.upper()

        return ExtractedPlayer(
            name=name,
            external_id=external_id,
            nationality_ioc=nationality,
            profile_url=href if href.startswith("http") else f"https://www.itftennis.com{href}",
        )

    text = element.get_text(strip=True)
    if text:
        # Handle LASTNAME, Firstname format
        if "," in text:
            parts = text.split(",", 1)
            text = f"{parts[1].strip()} {parts[0].strip()}"
        return ExtractedPlayer(name=text)

    return None


def _extract_sportsbet_player(element: Tag) -> Optional[ExtractedPlayer]:
    """
    Extract player info from Sportsbet betting site.

    Betting sites often use abbreviated names: "N. Djokovic"
    """
    # Betting sites typically have simpler structure
    text = element.get_text(strip=True)

    if text:
        # Clean up common betting site formatting
        name = text.strip()

        # Remove odds if accidentally included
        name = re.sub(r"\s*\d+\.\d+\s*$", "", name)

        return ExtractedPlayer(name=name)

    return None


def _extract_generic_player(element: Tag) -> Optional[ExtractedPlayer]:
    """
    Generic player extraction for unknown sources.

    Tries common patterns and falls back to text extraction.
    """
    # Try common link patterns
    link = element.find("a")
    if link:
        name = link.get_text(strip=True)
        if name:
            return ExtractedPlayer(name=name, profile_url=link.get("href"))

    # Just get text
    text = element.get_text(strip=True)
    if text:
        return ExtractedPlayer(name=text)

    return None


def extract_seed_from_name(name: str) -> tuple[str, Optional[int]]:
    """
    Extract seed number if embedded in player name.

    Some sources format names as "(1) N. Djokovic" or "N. Djokovic [1]"

    Args:
        name: Player name potentially containing seed

    Returns:
        Tuple of (clean_name, seed_number or None)

    Examples:
        >>> extract_seed_from_name("(1) Novak Djokovic")
        ('Novak Djokovic', 1)
        >>> extract_seed_from_name("Novak Djokovic [WC]")
        ('Novak Djokovic', None)
    """
    clean_name = name

    # Pattern: (1) Name or [1] Name
    prefix_match = re.match(r"^[\(\[](\d+)[\)\]]\s*(.+)$", name)
    if prefix_match:
        return prefix_match.group(2).strip(), int(prefix_match.group(1))

    # Pattern: Name (1) or Name [1]
    suffix_match = re.match(r"^(.+?)\s*[\(\[](\d+)[\)\]]$", name)
    if suffix_match:
        return suffix_match.group(1).strip(), int(suffix_match.group(2))

    # Remove non-numeric qualifiers like [WC], [Q], [LL]
    clean_name = re.sub(r"\s*[\(\[](?:WC|Q|LL|PR|SE|ALT)[\)\]]", "", name, flags=re.I)

    return clean_name.strip(), None
