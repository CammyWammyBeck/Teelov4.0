"""Fix tournament city/country data that was ingested without proper formatting.

Handles:
1. Merged city+country in the city field (e.g. "ALBANYUNITED STATES")
2. ALL CAPS city/country that just needs title-casing
3. Missing country — fill from geo lookup
4. US state abbreviations (e.g. "CHARLESTONSC")

Usage:
    cd /path/to/Teelov4.0
    source venv/bin/activate
    python scripts/fix_tournament_locations.py          # dry run
    python scripts/fix_tournament_locations.py --apply   # apply changes
"""

import argparse
import re

from teelo.db.models import Tournament
from teelo.db.session import get_session
from teelo.utils.geo import COUNTRY_TO_IOC, city_to_country, country_to_ioc


# US state abbreviations that appear suffixed to city names
US_STATE_ABBREVS = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
}

# Also handle 3-letter country codes that appear as suffixes (e.g. "OEIRASPORTUGAL" won't match
# but "OEIRASPOR" uses IOC code POR). Build reverse lookup: IOC -> country name
IOC_TO_COUNTRY = {}
for country, ioc in COUNTRY_TO_IOC.items():
    # Prefer the shortest/most standard name
    if ioc not in IOC_TO_COUNTRY or len(country) < len(IOC_TO_COUNTRY[ioc]):
        IOC_TO_COUNTRY[ioc] = country

# Extra country aliases not in COUNTRY_TO_IOC
EXTRA_COUNTRIES = {
    "Korea": "South Korea",
    "Azerbaijan": "Azerbaijan",
    "KSA": "Saudi Arabia",
}

# Build a list of known country names (sorted longest first for greedy matching)
_ALL_COUNTRY_NAMES = list(COUNTRY_TO_IOC.keys()) + list(EXTRA_COUNTRIES.keys())
KNOWN_COUNTRIES = sorted(_ALL_COUNTRY_NAMES, key=len, reverse=True)

# Country names that should NOT be title-cased (acronyms)
_UPPERCASE_COUNTRIES = {"UAE", "USA", "KSA"}


def is_all_caps(s: str) -> bool:
    """Check if string is all uppercase (ignoring non-alpha chars)."""
    alpha = re.sub(r"[^a-zA-Z]", "", s)
    return len(alpha) > 1 and alpha == alpha.upper()


def try_split_merged(city_field: str) -> tuple[str, str] | None:
    """Try to split a merged city+country string like 'ALBANYUNITED STATES'.

    Strategy: check if the string ends with a known country name (case-insensitive).
    Longest match wins to handle "United States" vs "States", etc.
    """
    upper = city_field.strip().upper()

    # 1. Try known country names (longest first)
    for country in KNOWN_COUNTRIES:
        country_upper = country.upper()
        if upper.endswith(country_upper) and len(upper) > len(country_upper):
            city_part = city_field[:len(city_field) - len(country)].strip()
            if city_part:  # Don't split if city would be empty
                # Resolve aliases (e.g. "Korea" -> "South Korea")
                resolved = EXTRA_COUNTRIES.get(country, country)
                # Don't title-case acronyms like UAE, USA
                country_display = resolved if resolved in _UPPERCASE_COUNTRIES else resolved.title()
                return city_part.title(), country_display

    # 2. Try IOC codes first (3 letters at end, e.g. "OEIRASPOR")
    #    Check before US state abbrevs to avoid "OR" matching Oregon
    if len(upper) > 3:
        suffix = upper[-3:]
        if suffix in IOC_TO_COUNTRY:
            city_part = city_field[:-3].strip()
            if city_part:
                return city_part.title(), IOC_TO_COUNTRY[suffix]

    # 3. Try US state abbreviations (2 letters at end)
    if len(upper) > 2:
        suffix = upper[-2:]
        if suffix in US_STATE_ABBREVS:
            city_part = city_field[:-2].strip()
            if city_part:
                return city_part.title(), "United States"

    return None


def fix_tournament_locations(apply: bool = False):
    with get_session() as session:
        tournaments = session.query(Tournament).all()

        fixes = []

        for t in tournaments:
            changes = {}

            # Case 1: City looks like merged city+country (all caps or has no country set)
            if t.city and is_all_caps(t.city):
                result = try_split_merged(t.city)
                if result:
                    city, country = result
                    changes["city"] = (t.city, city)
                    if not t.country:
                        changes["country"] = (t.country, country)
                        ioc = country_to_ioc(country)
                        if ioc and not t.country_ioc:
                            changes["country_ioc"] = (t.country_ioc, ioc)
                    elif is_all_caps(t.country):
                        changes["country"] = (t.country, t.country.strip().title())
                else:
                    # Can't split, but still title-case if ALL CAPS
                    changes["city"] = (t.city, t.city.strip().title())

            # Case 2: Country is ALL CAPS (standalone fix)
            if t.country and is_all_caps(t.country) and "country" not in changes:
                raw = t.country.strip()
                # Don't title-case acronyms like UAE, USA
                if raw.upper() in _UPPERCASE_COUNTRIES:
                    pass  # Already correct
                else:
                    changes["country"] = (t.country, raw.title())

            # Case 3: City exists but no country — try geo lookup
            if t.city and not t.country and "country" not in changes:
                city_val = changes["city"][1] if "city" in changes else t.city
                country = city_to_country(city_val)
                if country:
                    changes["country"] = (t.country, country)
                    if not t.country_ioc:
                        ioc = country_to_ioc(country)
                        if ioc:
                            changes["country_ioc"] = (t.country_ioc, ioc)

            if changes:
                fixes.append((t, changes))

        if not fixes:
            print("No tournaments need fixing.")
            return

        print(f"Found {len(fixes)} tournament(s) to fix:\n")
        for t, changes in fixes:
            print(f"  [{t.id}] {t.tournament_code} — {t.name}")
            for field, (old, new) in changes.items():
                print(f"    {field}: {old!r} -> {new!r}")
            print()

        if apply:
            for t, changes in fixes:
                for field, (old, new) in changes.items():
                    setattr(t, field, new)
            session.commit()
            print(f"Applied {len(fixes)} fix(es).")
        else:
            print("Dry run — no changes applied. Use --apply to apply.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix tournament location data")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry run)")
    args = parser.parse_args()
    fix_tournament_locations(apply=args.apply)
