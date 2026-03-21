"""
Backfill tournament country and country_ioc for all tournaments missing them.

Strategy:
1. Extract city names from tournament names using level-specific patterns
2. First try the existing geo.py city→country lookup (no API calls)
3. Geocode remaining cities via Nominatim (rate-limited to 1 req/1.1sec)
4. Cache geocoding results to JSON so re-runs don't re-geocode
5. Apply mappings to the database

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/backfill_tournament_country.py --dry-run
    python scripts/backfill_tournament_country.py --geocode-only
    python scripts/backfill_tournament_country.py
"""

import json
import re
import sys
import time
from pathlib import Path

from geopy.exc import GeocoderServiceError, GeocoderTimedOut
from geopy.geocoders import Nominatim
from sqlalchemy import text

from teelo.db.session import get_session
from teelo.utils.geo import (
    COUNTRY_TO_IOC,
    city_to_country,
    country_to_ioc,
)

# ── Config ──────────────────────────────────────────────────────────────────

CACHE_FILE = Path(__file__).parent / "city_country_cache.json"

# Nominatim policy: max 1 request per second, with a descriptive user-agent
GEOCODER_USER_AGENT = "teelo-tournament-backfill/1.0 (tennis analytics research)"
GEOCODER_DELAY_SECS = 1.1  # slightly over 1s to stay safe
GEOCODER_TIMEOUT_SECS = 10
GEOCODER_MAX_RETRIES = 3
GEOCODER_RETRY_BACKOFF_SECS = 5  # multiplied by attempt number

# Additional country name variants that Nominatim may return
# (supplements COUNTRY_TO_IOC from geo.py)
EXTRA_COUNTRY_TO_IOC = {
    "United States of America": "USA",
    "Russian Federation": "RUS",
    "Republic of Korea": "KOR",
    "Korea": "KOR",
    "Republic of China": "TPE",
    "Türkiye": "TUR",
    "Republic of Türkiye": "TUR",
    "Republic of Turkey": "TUR",
    "People's Republic of China": "CHN",
    "Réunion": "FRA",
    "Guadeloupe": "FRA",
    "New Caledonia": "FRA",
    "Martinique": "FRA",
    "French Polynesia": "FRA",
    "Reunion": "FRA",
    "Mayotte": "FRA",
    "Aruba": "ARU",
    "Curaçao": "CUW",
    "Cayman Islands": "CAY",
    "Palestine": "PLE",
    "State of Palestine": "PLE",
    "Palestinian Territory": "PLE",
    "Republic of Serbia": "SRB",
    "Republic of India": "IND",
    "Kingdom of the Netherlands": "NED",
    "Federal Republic of Germany": "GER",
    "Republic of the Philippines": "PHI",
    "República de Colombia": "COL",
    "México": "MEX",
    "Brasil": "BRA",
    "España": "ESP",
    "Italia": "ITA",
    "Deutschland": "GER",
    "Österreich": "AUT",
    "Schweiz/Suisse/Svizzera": "SUI",
    "Suisse": "SUI",
    "Schweiz": "SUI",
    "Svizzera": "SUI",
    "België / Belgique / Belgien": "BEL",
    "Belgique": "BEL",
    "België": "BEL",
    "Sverige": "SWE",
    "Norge": "NOR",
    "Danmark": "DEN",
    "Suomi": "FIN",
    "Ελλάδα": "GRE",
    "Polska": "POL",
    "Česko": "CZE",
    "Slovensko": "SVK",
    "Slovenija": "SLO",
    "Hrvatska": "CRO",
    "Srbija": "SRB",
    "Crna Gora": "MNE",
    "Bosna i Hercegovina": "BIH",
    "Shqipëria": "ALB",
    "Северна Македонија": "MKD",
    "Magyarország": "HUN",
    "România": "ROU",
    "Lietuva": "LTU",
    "Latvija": "LAT",
    "Eesti": "EST",
    "საქართველო": "GEO",
    "Azərbaycan": "AZE",
    "Қазақстан": "KAZ",
    "O'zbekiston": "UZB",
    "Oʻzbekiston": "UZB",
    "Việt Nam": "VIE",
    "日本": "JPN",
    "中华人民共和国": "CHN",
    "대한민국": "KOR",
    "臺灣": "TPE",
    "香港": "HKG",
    "澳門": "MAC",
    "Guam": "GUM",
    "Libya": "LBA",
    "Eswatini": "SWZ",
    "Kyrgyzstan": "KGZ",
    "Кыргызстан": "KGZ",
    "Тоҷикистон": "TJK",
    "Tajikistan": "TJK",
    "Turkmenistan": "TKM",
    "Türkmenistan": "TKM",
    "Монгол Улс": "MGL",
    "ไทย": "THA",
}

# Manual overrides for city names that geocoders won't handle well
CITY_OVERRIDES: dict[str, tuple[str | None, str | None]] = {
    # Travelling/multi-city events — skip
    "ATP CHALLENGER TOUR FINALS": (None, None),
    "LAVER CUP": (None, None),
    "DAVIS CUP FINALS": (None, None),
    "DAVIS CUP FINALS GROUP STAGE": (None, None),
    "DAVIS CUP QUALIFIERS": (None, None),
    "DAVIS CUP WORLD GROUP I": (None, None),
    "DAVIS CUP WORLD GROUP II": (None, None),
    # Tournament brands with city embedded
    "MIAMI OPEN PRESENTED BY ITAÚ": ("United States", "USA"),
    "DUBROVNIK OPEN": ("Croatia", "CRO"),
    "BAKU CUP": ("Azerbaijan", "AZE"),
    "FUJAIRAH OPEN": ("United Arab Emirates", "UAE"),
    "CHACA CHALLENGER METEPEC OPEN": ("Mexico", "MEX"),
    "MONTEMAR ENE CONSTRUCCION": ("Chile", "CHI"),
    "STEVE CARTER BATON ROUGE CHALLENGER": ("United States", "USA"),
    # Typos, compound names, unusual spellings
    "SHARM ELSHEIKH": ("Egypt", "EGY"),
    "BAD WALTERSDORG": ("Austria", "AUT"),
    "SAO PAULO1": ("Brazil", "BRA"),
    "CHERBOUG-EN-COTENTIN": ("France", "FRA"),
    "EQUERDREVILLE": ("France", "FRA"),
    "FLEURUS (NEAR CHARLEROI).": ("Belgium", "BEL"),
    "SANDELFJORD": ("Norway", "NOR"),
    "VALLDUXO": ("Spain", "ESP"),
    "YEONG WOL": ("South Korea", "KOR"),
    "QIAN DAOHU": ("China", "CHN"),
    "JOHANNESBURG / ELLISPARK": ("South Africa", "RSA"),
    "JOHANNESBURG / MARKSPARK": ("South Africa", "RSA"),
    "TELDE - L.P DE GRAN CANARIA": ("Spain", "ESP"),
    "GEOGIA F1 FUTURES": ("Georgia", "GEO"),
    "GREAT BRITAIN F23 FUTURE": ("United Kingdom", "GBR"),
    # Compound/hyphenated names
    "AIN ELSOKHNA-SUIZ": ("Egypt", "EGY"),
    "ALAMINOS-LARNACA": ("Cyprus", "CYP"),
    "ANTALYA-ALI BEY MANAVGAT": ("Turkey", "TUR"),
    "ANTALYA-BELCONTI": ("Turkey", "TUR"),
    "ANTALYA-KAYA BELEK": ("Turkey", "TUR"),
    "ANTALYA-KAYA-BELEK": ("Turkey", "TUR"),
    "HAPPY VALLEY": ("Australia", "AUS"),
    "REUNION ISLAND": ("France", "FRA"),
    "LE GOSIER": ("France", "FRA"),
    "RAMAT HASHARON": ("Israel", "ISR"),
    "LES FRANQUESES DEL VALLES": ("Spain", "ESP"),
    "SOPHIA ANTIPOLIS": ("France", "FRA"),
    "ST. REMY": ("France", "FRA"),
    "VILLA ALLENDE": ("Argentina", "ARG"),
    "CAMPOS DO JORDAO": ("Brazil", "BRA"),
    "SAO JOSE DO RIO PRETO": ("Brazil", "BRA"),
    "RIO QUENTE": ("Brazil", "BRA"),
    "SALVADOR DE BAHIA": ("Brazil", "BRA"),
    "SAN BENEDETTO DEL TRONTO": ("Italy", "ITA"),
    "ROSETO DEGLI ABRUZZI": ("Italy", "ITA"),
    "MASPALOMAS": ("Spain", "ESP"),
    "GRAN CANARIA": ("Spain", "ESP"),
    "PALMAS DEL MAR": ("Puerto Rico", "PUR"),
    "SOUTHSEA": ("United Kingdom", "GBR"),
    "ANDREZIEUX-BOUTHEON": ("France", "FRA"),
    "CAGNES-SUR-MER": ("France", "FRA"),
    "SAINT-PETERSBURG": ("Russia", "RUS"),
    "NUR-SULTAN": ("Kazakhstan", "KAZ"),
    "SHARM EL SHEIKH": ("Egypt", "EGY"),
    "POTCHEFSTROOM": ("South Africa", "RSA"),
}

# US state abbreviations for detecting US cities
US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
}


# ── City extraction ─────────────────────────────────────────────────────────

def extract_city(name: str, level: str) -> str:
    """Extract city name from tournament name based on level-specific patterns."""
    if level in ("Challenger", "ATP 250", "ATP 500", "International",
                 "WTA 1000", "WTA 500", "WTA 250", "WTA 125",
                 "Masters 1000", "Grand Slam", "ATP Finals", "Finals",
                 "Premier"):
        city = name.strip()
        city = re.sub(r"\s+\d+$", "", city)  # "Bangkok 1" → "Bangkok"
        city = re.sub(r"\s*\(Suspended\)$", "", city)
        return city.upper()
    elif level == "ITF":
        # Old format: "$100,000 CITY" or "$10,000+H CITY (notes)"
        m = re.match(r"[\$\d,]+(?:\+H)?\s+(.+)", name)
        if m:
            city = m.group(1).strip()
            city = re.sub(r"\s*\(.*\)$", "", city)
            return city.upper()
    else:
        # New ITF format: "M15 CITY+H" or "W35 CITY (notes)"
        # Also handles "+H" before city: "M25+H RODEZ"
        m = re.match(r"[MW]\d+(?:\+\s*H)?\s+(.+?)(?:\+H)?$", name)
        if m:
            city = m.group(1).strip()
            city = re.sub(r"\s*\(.*\)$", "", city)
            return city.upper()
    return name.upper()


def _is_us_city(city: str) -> bool:
    """Check if city ends with a US state abbreviation (e.g. 'MIDLAND, MI')."""
    m = re.match(r".+,\s*([A-Z]{2})$", city)
    return bool(m and m.group(1) in US_STATES)


def _strip_state(city: str) -> str:
    """Remove trailing ', XX' state abbreviation."""
    return re.sub(r",\s*[A-Z]{2}$", "", city).strip()


def _strip_number_suffix(city: str) -> str:
    """Remove trailing number like 'POTCHEFSTROOM 1' → 'POTCHEFSTROOM'."""
    return re.sub(r"\s+\d+$", "", city).strip()


# ── Resolve city without API ────────────────────────────────────────────────

def resolve_city_local(city: str) -> tuple[str | None, str | None]:
    """
    Try to resolve city→(country, IOC) using only local lookups (no API).
    Returns (None, None) if not found locally.
    """
    # Check manual overrides
    if city in CITY_OVERRIDES:
        return CITY_OVERRIDES[city]

    # US state abbreviation shortcut
    if _is_us_city(city):
        return ("United States", "USA")

    # Old ITF "COUNTRY F# FUTURES" pattern (e.g. "ALGERIA F1 FUTURES")
    # Also handles parenthetical notes and "B" suffix on number
    futures_match = re.match(
        r"^([A-Z][A-Z &]+?)\s+F\d+[A-Z]?\s+FUTURES?(?:\s*\(.*\))?$",
        city, re.IGNORECASE
    )
    if futures_match:
        country_name = futures_match.group(1).strip().title()
        # Handle common name variants
        futures_country_map = {
            "Bosnia & Herzegovina": "Bosnia and Herzegovina",
            "Korea": "South Korea",
            "Slovak Republic": "Slovakia",
            "Macedonia": "North Macedonia",
            "Great Britain": "United Kingdom",
        }
        country_name = futures_country_map.get(country_name, country_name)
        ioc = country_to_ioc(country_name)
        if ioc:
            return (country_name, ioc)

    # Try existing geo.py lookup (case-insensitive)
    city_title = _strip_state(city).title()
    country = city_to_country(city_title)
    if country:
        ioc = country_to_ioc(country)
        return (country, ioc)

    # Try with number suffix stripped
    city_stripped = _strip_number_suffix(city)
    if city_stripped != city:
        country = city_to_country(_strip_state(city_stripped).title())
        if country:
            return (country, country_to_ioc(country))

    return (None, None)


# ── Geocoding ───────────────────────────────────────────────────────────────

def _lookup_ioc(country_name: str) -> str | None:
    """Look up IOC code from country name, checking both geo.py and extras."""
    ioc = country_to_ioc(country_name)
    if ioc:
        return ioc
    return EXTRA_COUNTRY_TO_IOC.get(country_name)


def geocode_city(geolocator, city: str) -> tuple[str | None, str | None]:
    """
    Geocode a city via Nominatim. Returns (country_name, IOC_code).
    Rate limiting is handled by the caller (sleep between calls).
    """
    query = _strip_state(_strip_number_suffix(city)).title()
    query_variants = [query, query.replace("-", " ")]
    # Remove duplicates while preserving order
    seen = set()
    query_variants = [q for q in query_variants if q not in seen and not seen.add(q)]

    for variant in query_variants:
        for attempt in range(GEOCODER_MAX_RETRIES):
            try:
                location = geolocator.geocode(
                    variant,
                    language="en",
                    timeout=GEOCODER_TIMEOUT_SECS,
                    exactly_one=True,
                )
                if location:
                    address = location.raw.get("display_name", "")
                    country = address.split(",")[-1].strip()
                    ioc = _lookup_ioc(country)
                    if ioc:
                        return (country, ioc)
                    # Found location but can't map country
                    print(f"    WARNING: No IOC for '{country}' "
                          f"(city: {city}, variant: {variant})")
                    return (country, None)
                break  # Location not found, try next variant

            except GeocoderTimedOut:
                print(f"    Timeout ({attempt + 1}/{GEOCODER_MAX_RETRIES})")
                time.sleep(GEOCODER_RETRY_BACKOFF_SECS * (attempt + 1))
            except GeocoderServiceError as e:
                print(f"    Service error: {e} "
                      f"({attempt + 1}/{GEOCODER_MAX_RETRIES})")
                time.sleep(GEOCODER_RETRY_BACKOFF_SECS * (attempt + 1))

    return (None, None)


# ── Cache ───────────────────────────────────────────────────────────────────

def load_cache() -> dict[str, list]:
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text())
    return {}


def save_cache(cache: dict):
    CACHE_FILE.write_text(json.dumps(cache, indent=2, sort_keys=True))


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    dry_run = "--dry-run" in sys.argv
    geocode_only = "--geocode-only" in sys.argv

    if dry_run:
        print("=== DRY RUN — no database changes ===\n")

    cache = load_cache()
    print(f"Loaded {len(cache)} cached geocoding results\n")

    # Phase 0: Read tournaments (short-lived DB session)
    with get_session() as session:
        rows = session.execute(text("""
            SELECT t.id, t.name, t.level, t.country, t.country_ioc, t.city
            FROM tournaments t
            WHERE t.country_ioc IS NULL
            ORDER BY t.level, t.name
        """)).fetchall()

    print(f"Found {len(rows)} tournaments missing country_ioc\n")

    # Extract cities and group tournaments by city
    city_to_tournaments: dict[str, list] = {}
    for r in rows:
        city = extract_city(r.name, r.level)
        city_to_tournaments.setdefault(city, []).append(r)

    unique_cities = sorted(city_to_tournaments.keys())
    print(f"Extracted {len(unique_cities)} unique city names\n")

    # Phase 1: Local resolution (no API calls)
    resolved: dict[str, tuple[str | None, str | None]] = {}
    needs_geocoding = []

    for city in unique_cities:
        # Check cache first
        if city in cache:
            resolved[city] = tuple(cache[city])
            continue

        # Try local lookup
        country, ioc = resolve_city_local(city)
        if ioc:
            resolved[city] = (country, ioc)
            cache[city] = [country, ioc]
        elif country is None and ioc is None and city in CITY_OVERRIDES:
            # Explicitly skipped (travelling events)
            resolved[city] = (None, None)
            cache[city] = [None, None]
        else:
            needs_geocoding.append(city)

    local_found = sum(1 for c, i in resolved.values() if i)
    print(f"Resolved locally: {local_found} cities")
    print(f"Need geocoding:   {len(needs_geocoding)} cities\n")

    # Phase 2: Geocode remaining cities (no DB connection held)
    if needs_geocoding:
        est_minutes = len(needs_geocoding) * GEOCODER_DELAY_SECS / 60
        print(f"Estimated geocoding time: {est_minutes:.0f} minutes "
              f"({GEOCODER_DELAY_SECS}s between requests)\n")

        geolocator = Nominatim(
            user_agent=GEOCODER_USER_AGENT,
            timeout=GEOCODER_TIMEOUT_SECS,
        )

        for i, city in enumerate(needs_geocoding):
            time.sleep(GEOCODER_DELAY_SECS)

            country, ioc = geocode_city(geolocator, city)
            resolved[city] = (country, ioc)
            cache[city] = [country, ioc]

            status = ioc or "???"
            print(f"  [{i + 1}/{len(needs_geocoding)}] "
                  f"{city:<45} → {status:>3} ({country or 'NOT FOUND'})")

            # Save cache periodically
            if (i + 1) % 50 == 0:
                save_cache(cache)
                print(f"  ... cache saved ({i + 1}/{len(needs_geocoding)})")

        save_cache(cache)
        print(f"\nGeocoding complete. Cache saved to {CACHE_FILE}\n")
    else:
        save_cache(cache)  # save local resolutions

    if geocode_only:
        _print_summary(resolved, city_to_tournaments)
        return

    # Phase 3: Apply to database (fresh session, batched by IOC code)
    print("=== Applying to database ===\n")

    updated = 0
    skipped_travelling = 0
    not_found = 0

    # Group tournament IDs by (country, ioc) for batch updates
    batch: dict[tuple[str, str], list[int]] = {}
    for city, tournaments in city_to_tournaments.items():
        country, ioc = resolved.get(city, (None, None))

        if country is None and ioc is None:
            skipped_travelling += len(tournaments)
            continue
        if not ioc:
            not_found += len(tournaments)
            continue

        key = (country, ioc)
        if key not in batch:
            batch[key] = []
        for t in tournaments:
            batch[key].append(t.id)
            updated += 1

    if dry_run:
        for (country, ioc), ids in sorted(batch.items()):
            print(f"  WOULD UPDATE {len(ids)} tournaments → {country} ({ioc})")
    else:
        with get_session() as session:
            for (country, ioc), ids in batch.items():
                # Batch update: one query per country, using ANY() array
                session.execute(text("""
                    UPDATE tournaments
                    SET country = COALESCE(country, :country),
                        country_ioc = :ioc
                    WHERE id = ANY(:ids) AND country_ioc IS NULL
                """), {"country": country, "ioc": ioc, "ids": ids})
            session.commit()

    print(f"\nResults:")
    print(f"  Updated:              {updated}")
    print(f"  Skipped (travelling): {skipped_travelling}")
    print(f"  Not found:            {not_found}")

    _print_summary(resolved, city_to_tournaments)


def _print_summary(resolved, city_to_tournaments):
    """Print cities that couldn't be mapped."""
    unmapped = []
    for city, ts in city_to_tournaments.items():
        country, ioc = resolved.get(city, (None, None))
        if not ioc and country is not None:
            # Has country name but no IOC mapping
            unmapped.append((city, len(ts), country))
        elif country is None and ioc is None and city not in CITY_OVERRIDES:
            # Completely unresolved
            unmapped.append((city, len(ts), None))

    if unmapped:
        print(f"\n=== {len(unmapped)} cities could not be fully mapped ===")
        for city, count, country in sorted(unmapped, key=lambda x: -x[1]):
            extra = f" (country: {country})" if country else ""
            print(f"  {city:<45} {count:>4} tournaments{extra}")
    else:
        print("\nAll cities mapped successfully!")


if __name__ == "__main__":
    main()
