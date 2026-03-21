"""
Geographic lookup utilities for tennis data.

This module provides mappings and helper functions to normalize country names,
convert cities to countries, and look up IOC (International Olympic Committee)
country codes. This is essential for standardizing location data from different
sources (ATP, WTA, ITF, betting sites) which may use different naming conventions.

City→country lookups are backed by scripts/city_country_cache.json, which contains
~2600 tennis city mappings built via Nominatim geocoding. To add new cities, either:
  1. Add them manually to the cache JSON file
  2. Re-run scripts/backfill_tournament_country.py which will geocode unknowns
"""

import json
from pathlib import Path
from typing import Optional

# Comprehensive mapping of country names to 3-letter IOC codes.
# Includes common variations and alternate spellings found in tennis data.
COUNTRY_TO_IOC: dict[str, str] = {
    "Albania": "ALB",
    "Algeria": "ALG",
    "Andorra": "AND",
    "Angola": "ANG",
    "Argentina": "ARG",
    "Armenia": "ARM",
    "Australia": "AUS",
    "Austria": "AUT",
    "Azerbaijan": "AZE",
    "Bahamas": "BAH",
    "Bahrain": "BRN",
    "Bangladesh": "BAN",
    "Barbados": "BAR",
    "Belarus": "BLR",
    "Belgium": "BEL",
    "Benin": "BEN",
    "Bolivia": "BOL",
    "Bosnia": "BIH",
    "Bosnia and Herzegovina": "BIH",
    "Brazil": "BRA",
    "Britain": "GBR",
    "Bulgaria": "BUL",
    "Burkina Faso": "BUR",
    "Burundi": "BDI",
    "Cambodia": "CAM",
    "Cameroon": "CMR",
    "Canada": "CAN",
    "Chad": "CHA",
    "Chile": "CHI",
    "China": "CHN",
    "Chinese Taipei": "TPE",
    "Colombia": "COL",
    "Congo": "CGO",
    "Costa Rica": "CRC",
    "Cote d'Ivoire": "CIV",
    "Croatia": "CRO",
    "Cuba": "CUB",
    "Cyprus": "CYP",
    "Czech Republic": "CZE",
    "Czechia": "CZE",
    "DR Congo": "COD",
    "Democratic Republic of the Congo": "COD",
    "Denmark": "DEN",
    "Djibouti": "DJI",
    "Dominican Republic": "DOM",
    "Ecuador": "ECU",
    "Egypt": "EGY",
    "El Salvador": "ESA",
    "England": "GBR",
    "Estonia": "EST",
    "Ethiopia": "ETH",
    "Finland": "FIN",
    "France": "FRA",
    "Gabon": "GAB",
    "Georgia": "GEO",
    "Germany": "GER",
    "Ghana": "GHA",
    "Great Britain": "GBR",
    "Greece": "GRE",
    "Guam": "GUM",
    "Guatemala": "GUA",
    "Haiti": "HAI",
    "Honduras": "HON",
    "Hong Kong": "HKG",
    "Hong-Kong, China": "HKG",
    "Hungary": "HUN",
    "Iceland": "ISL",
    "India": "IND",
    "Indonesia": "INA",
    "Iran": "IRI",
    "Iraq": "IRQ",
    "Ireland": "IRL",
    "Israel": "ISR",
    "Italy": "ITA",
    "Ivory Coast": "CIV",
    "Côte d'Ivoire": "CIV",
    "Jamaica": "JAM",
    "Japan": "JPN",
    "Jordan": "JOR",
    "Kazakhstan": "KAZ",
    "Kenya": "KEN",
    "Kosovo": "KOS",
    "Kuwait": "KUW",
    "Laos": "LAO",
    "Latvia": "LAT",
    "Lebanon": "LBN",
    "Libya": "LBA",
    "Liechtenstein": "LIE",
    "Lithuania": "LTU",
    "Luxembourg": "LUX",
    "Madagascar": "MAD",
    "Malaysia": "MAS",
    "Mali": "MLI",
    "Malta": "MLT",
    "Mauritius": "MRI",
    "Mexico": "MEX",
    "Moldova": "MDA",
    "Monaco": "MON",
    "Mongolia": "MGL",
    "Montenegro": "MNE",
    "Morocco": "MAR",
    "Mozambique": "MOZ",
    "Myanmar": "MYA",
    "Nepal": "NEP",
    "Netherlands": "NED",
    "Nicaragua": "NCA",
    "New Zealand": "NZL",
    "Niger": "NIG",
    "Nigeria": "NGR",
    "North Korea": "PRK",
    "North Macedonia": "MKD",
    "Norway": "NOR",
    "Oman": "OMA",
    "Pakistan": "PAK",
    "Panama": "PAN",
    "Paraguay": "PAR",
    "Peru": "PER",
    "Philippines": "PHI",
    "Poland": "POL",
    "Portugal": "POR",
    "Puerto Rico": "PUR",
    "Qatar": "QAT",
    "Romania": "ROU",
    "Russia": "RUS",
    "Rwanda": "RWA",
    "San Marino": "SMR",
    "Saudi Arabia": "KSA",
    "Scotland": "GBR",
    "Senegal": "SEN",
    "Serbia": "SRB",
    "Singapore": "SGP",
    "Slovakia": "SVK",
    "Slovenia": "SLO",
    "South Africa": "RSA",
    "South Korea": "KOR",
    "Spain": "ESP",
    "Sri Lanka": "SRI",
    "Sweden": "SWE",
    "Switzerland": "SUI",
    "Taiwan": "TPE",
    "Tanzania": "TAN",
    "Thailand": "THA",
    "Togo": "TOG",
    "Trinidad and Tobago": "TTO",
    "Tunisia": "TUN",
    "Turkey": "TUR",
    "UAE": "UAE",
    "Uganda": "UGA",
    "Ukraine": "UKR",
    "United Arab Emirates": "UAE",
    "United Kingdom": "GBR",
    "United States": "USA",
    "Uruguay": "URU",
    "USA": "USA",
    "Uzbekistan": "UZB",
    "Venezuela": "VEN",
    "Vietnam": "VIE",
    "Wales": "GBR",
    "Zimbabwe": "ZIM",
}

# ── Lazy-loaded lookup caches ───────────────────────────────────────────────

_LOWER_COUNTRY_TO_IOC: Optional[dict[str, str]] = None
_CITY_TO_COUNTRY: Optional[dict[str, str]] = None

_CITY_CACHE_PATH = Path(__file__).parent.parent.parent.parent / "scripts" / "city_country_cache.json"


def _get_city_lookup() -> dict[str, str]:
    """
    Load city→country lookup from the geocoding cache (lazy, once).

    Returns a dict of lowercase city name → country name.
    Source: scripts/city_country_cache.json (~2600 entries).
    """
    global _CITY_TO_COUNTRY
    if _CITY_TO_COUNTRY is not None:
        return _CITY_TO_COUNTRY

    _CITY_TO_COUNTRY = {}
    if _CITY_CACHE_PATH.exists():
        try:
            data = json.loads(_CITY_CACHE_PATH.read_text())
            for city_upper, (country, ioc) in data.items():
                if country and ioc:
                    _CITY_TO_COUNTRY[city_upper.lower()] = country
        except Exception:
            pass

    return _CITY_TO_COUNTRY


# ── Public API ──────────────────────────────────────────────────────────────


def country_to_ioc(country_name: str) -> Optional[str]:
    """
    Look up IOC code for a country name. Case-insensitive.

    Args:
        country_name: Name of the country (e.g., "United States", "spain")

    Returns:
        3-letter IOC code (e.g., "USA", "ESP") or None if not found.
    """
    if not country_name:
        return None

    # Check exact match first (fastest path)
    if country_name in COUNTRY_TO_IOC:
        return COUNTRY_TO_IOC[country_name]

    global _LOWER_COUNTRY_TO_IOC
    if _LOWER_COUNTRY_TO_IOC is None:
        _LOWER_COUNTRY_TO_IOC = {k.lower(): v for k, v in COUNTRY_TO_IOC.items()}

    normalized = country_name.strip().lower()
    return _LOWER_COUNTRY_TO_IOC.get(normalized)


def city_to_country(city: str) -> Optional[str]:
    """
    Look up country name for a tennis city. Case-insensitive.

    Backed by scripts/city_country_cache.json which contains ~2600 tennis
    city→country mappings built via Nominatim geocoding.

    Args:
        city: Name of the city (e.g., "Paris", "london")

    Returns:
        Country name matching a key in COUNTRY_TO_IOC, or None if not found.
    """
    if not city:
        return None

    lookup = _get_city_lookup()
    return lookup.get(city.strip().lower())


def city_to_ioc(city: str) -> Optional[str]:
    """
    Convenience: look up IOC code for a tennis city.

    Combines city_to_country() and country_to_ioc().

    Args:
        city: Name of the city

    Returns:
        3-letter IOC code for the city's country, or None if not found.
    """
    country = city_to_country(city)
    if country:
        return country_to_ioc(country)
    return None


def city_country_to_timezone(city: str | None, country: str | None) -> str | None:
    """
    Look up IANA timezone for a city/country pair using geocoding.

    Uses geopy Nominatim to geocode city+country, then timezonefinder
    to get the timezone from coordinates. Returns None if lookup fails.

    Results are cached in-process to avoid repeated geocoding calls.
    """
    if not city or not country:
        return None

    cache_key = (city.strip().lower(), country.strip().lower())

    if not hasattr(city_country_to_timezone, "_cache"):
        city_country_to_timezone._cache = {}

    cached = city_country_to_timezone._cache.get(cache_key)
    if cached is not None:
        return cached if cached != "" else None

    try:
        from geopy.geocoders import Nominatim
        from timezonefinder import TimezoneFinder

        if not hasattr(city_country_to_timezone, "_geocoder"):
            city_country_to_timezone._geocoder = Nominatim(
                user_agent="teelo-timezone-lookup", timeout=10,
            )
            city_country_to_timezone._tf = TimezoneFinder()

        location = city_country_to_timezone._geocoder.geocode(
            f"{city}, {country}", exactly_one=True,
        )
        if location is None:
            city_country_to_timezone._cache[cache_key] = ""
            return None

        tz = city_country_to_timezone._tf.timezone_at(
            lat=location.latitude, lng=location.longitude,
        )
        city_country_to_timezone._cache[cache_key] = tz or ""
        return tz
    except Exception:
        city_country_to_timezone._cache[cache_key] = ""
        return None


# ---------------------------------------------------------------------------
# Region mapping (IOC code → region)
# ---------------------------------------------------------------------------

REGION_MEMBERS: dict[str, set[str]] = {
    "Europe": {
        "ALB", "AND", "ARM", "AUT", "AZE", "BEL", "BIH", "BLR", "BUL",
        "CRO", "CYP", "CZE", "DEN", "ESP", "EST", "FIN", "FRA", "GBR",
        "GEO", "GER", "GRE", "HUN", "IRL", "ISL", "ISR", "ITA", "LAT",
        "LTU", "LUX", "MDA", "MKD", "MLT", "MNE", "MON", "NED", "NOR",
        "POL", "POR", "ROU", "RUS", "SLO", "SRB", "SVK", "SWE", "SUI",
        "UKR",
    },
    "Asia-Pacific": {
        "AUS", "BAN", "CHN", "HKG", "INA", "IND", "JPN", "KAZ", "KGZ",
        "KOR", "MAS", "MYA", "NZL", "PAK", "PHI", "SGP", "SRI", "THA",
        "TJK", "TKM", "TPE", "UZB", "VIE",
    },
    "Americas": {
        "ARG", "BAH", "BAR", "BER", "BOL", "BRA", "CAN", "CHI", "COL",
        "CRC", "CUB", "DOM", "ECU", "ESA", "GUA", "HAI", "HON", "JAM",
        "MEX", "NCA", "PAN", "PAR", "PER", "PUR", "TTO", "URU", "USA",
        "VEN",
    },
    "Middle East & Africa": {
        "ALG", "ANG", "BDI", "BEN", "BOT", "BRN", "BUR", "CAM", "CGO",
        "CHA", "CIV", "CMR", "COD", "COM", "DJI", "EGY", "ERI", "ETH",
        "GAB", "GAM", "GHA", "GUI", "GNB", "IRQ", "JOR", "KEN", "KSA",
        "KUW", "LBA", "LBN", "LBR", "LES", "MAD", "MAR", "MAW", "MLI",
        "MOZ", "MRI", "MTN", "NAM", "NGR", "NIG", "OMA", "QAT", "RSA",
        "RWA", "SEN", "SEY", "SLE", "SOM", "SSD", "SUD", "SWZ", "SYR",
        "TAN", "TOG", "TUN", "TUR", "UAE", "UGA", "YEM", "ZAM", "ZIM",
    },
}

IOC_TO_REGION: dict[str, str] = {
    ioc: region for region, members in REGION_MEMBERS.items() for ioc in members
}


def ioc_to_region(ioc: str) -> str | None:
    """Return the region name for an IOC country code, or None if unknown."""
    return IOC_TO_REGION.get(ioc)
