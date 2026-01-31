#!/usr/bin/env python3
"""
Backfill tournament country and country_ioc from city names.

Uses the geo utility to look up country and IOC code for each tournament
based on its city field. Only updates tournaments missing country/country_ioc data.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/backfill_tournament_countries.py --dry-run
    python scripts/backfill_tournament_countries.py
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db.session import get_session
from teelo.db.models import Tournament
from teelo.utils.geo import city_to_country, country_to_ioc
from sqlalchemy import or_

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def backfill(dry_run: bool = False):
    """Backfill country and country_ioc for tournaments with city data."""
    with get_session() as session:
        # Tournaments that have a city but are missing country or IOC code
        tournaments = session.query(Tournament).filter(
            Tournament.city.isnot(None),
            or_(
                Tournament.country.is_(None),
                Tournament.country_ioc.is_(None),
            )
        ).order_by(Tournament.id).all()

        logger.info(f"Found {len(tournaments)} tournaments to backfill")

        updated = 0
        unmapped = []

        for t in tournaments:
            country = city_to_country(t.city)
            if not country:
                # Try stripping whitespace (some cities have trailing spaces)
                country = city_to_country(t.city.strip())

            if not country:
                unmapped.append(t.city)
                continue

            ioc = country_to_ioc(country)
            changes = {}

            if not t.country:
                changes["country"] = country
            if not t.country_ioc and ioc:
                changes["country_ioc"] = ioc

            if not changes:
                continue

            if dry_run:
                logger.info(f"  [DRY RUN] {t.name} ({t.city}): {changes}")
            else:
                if "country" in changes:
                    t.country = changes["country"]
                if "country_ioc" in changes:
                    t.country_ioc = changes["country_ioc"]

            updated += 1

        if not dry_run:
            session.commit()

        logger.info(f"\nUpdated: {updated}")
        if unmapped:
            unique_unmapped = sorted(set(unmapped))
            logger.warning(f"Unmapped cities ({len(unique_unmapped)}): {unique_unmapped}")


def main():
    parser = argparse.ArgumentParser(description="Backfill tournament country data from city names")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing to DB")
    args = parser.parse_args()

    backfill(args.dry_run)


if __name__ == "__main__":
    main()
