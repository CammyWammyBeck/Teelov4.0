#!/usr/bin/env python3
"""
Batch player profile enrichment script.

Queries the database for players missing demographic data, scrapes their
profile pages on ATP/WTA, and updates the database.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate

    # Dry run (print only, no DB writes)
    python scripts/enrich_players.py --dry-run

    # Enrich ATP players only
    python scripts/enrich_players.py --source atp --limit 10

    # Enrich WTA players only
    python scripts/enrich_players.py --source wta

    # Enrich all players
    python scripts/enrich_players.py
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import or_
from teelo.db.session import get_session
from teelo.db.models import Player
from teelo.scrape.player_enrichment import PlayerEnrichmentScraper, PlayerProfile
from teelo.utils.geo import country_to_ioc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_players_needing_enrichment(source: str, limit: int = 0, force: bool = False) -> list[dict]:
    """
    Query the database for players that are missing demographic data.

    A player needs enrichment if they have an external ID for the source
    but are missing any of: birth_date, height_cm, hand.

    Args:
        source: "atp", "wta", or "both"
        limit: Maximum number of players to return (0 = no limit)
        force: If True, return all players regardless of existing data

    Returns:
        List of dicts with player id, canonical_name, atp_id, wta_id
    """
    with get_session() as session:
        query = session.query(Player)

        # Filter to players with the relevant external ID
        if source == "atp":
            query = query.filter(Player.atp_id.isnot(None))
        elif source == "wta":
            query = query.filter(Player.wta_id.isnot(None))
        else:
            query = query.filter(
                or_(Player.atp_id.isnot(None), Player.wta_id.isnot(None))
            )

        # Filter to players missing at least one demographic field (unless --force)
        if not force:
            query = query.filter(
                or_(
                    Player.birth_date.is_(None),
                    Player.height_cm.is_(None),
                    Player.hand.is_(None),
                )
            )

        query = query.order_by(Player.id)
        if limit > 0:
            query = query.limit(limit)

        players = []
        for p in query.all():
            players.append({
                "id": p.id,
                "name": p.canonical_name,
                "atp_id": p.atp_id,
                "wta_id": p.wta_id,
            })

        return players


def update_player(player_id: int, profile: PlayerProfile, dry_run: bool = False, force: bool = False):
    """
    Update a player's demographic fields from a scraped profile.

    Only updates fields that are None in the database — unless force=True,
    which overwrites existing data.
    """
    with get_session() as session:
        player = session.get(Player, player_id)
        if not player:
            logger.warning(f"Player {player_id} not found in database")
            return

        updates = {}
        if profile.birth_date and (force or not player.birth_date):
            updates["birth_date"] = profile.birth_date
        if profile.height_cm and (force or not player.height_cm):
            updates["height_cm"] = profile.height_cm
        if profile.hand and (force or not player.hand):
            updates["hand"] = profile.hand
        if profile.backhand and (force or not player.backhand):
            updates["backhand"] = profile.backhand
        if profile.turned_pro_year and (force or not player.turned_pro_year):
            updates["turned_pro_year"] = profile.turned_pro_year
        if profile.nationality and (force or not player.nationality_ioc):
            # Convert full country name to 3-letter IOC code
            ioc = country_to_ioc(profile.nationality)
            if ioc:
                updates["nationality_ioc"] = ioc
            else:
                logger.warning(f"  Unknown country '{profile.nationality}' for {player.canonical_name}")

        if not updates:
            logger.info(f"  No new data for {player.canonical_name}")
            return

        if dry_run:
            logger.info(f"  [DRY RUN] Would update {player.canonical_name}: {updates}")
            return

        for field, value in updates.items():
            setattr(player, field, value)
        session.commit()
        logger.info(f"  Updated {player.canonical_name}: {updates}")


def slugify(name: str) -> str:
    """Convert a player name to a URL slug. e.g. 'Carlos Alcaraz' -> 'carlos-alcaraz'."""
    return name.lower().replace(" ", "-").replace("'", "").replace(".", "")


async def run_enrichment(source: str, limit: int, dry_run: bool, force: bool = False):
    """Main enrichment loop."""
    players = get_players_needing_enrichment(source, limit, force=force)
    logger.info(f"Found {len(players)} players needing enrichment (source={source})")

    if not players:
        return

    enriched = 0
    failed = 0
    skipped = 0

    async with PlayerEnrichmentScraper(headless=False) as scraper:
        for i, p in enumerate(players):
            logger.info(f"[{i+1}/{len(players)}] {p['name']} (atp={p['atp_id']}, wta={p['wta_id']})")

            profile = None
            try:
                # Prefer ATP if available (has more fields: backhand, turned pro)
                if p["atp_id"] and source in ("atp", "both"):
                    slug = slugify(p["name"])
                    profile = await scraper.scrape_atp_profile(p["atp_id"], slug)

                elif p["wta_id"] and source in ("wta", "both"):
                    slug = slugify(p["name"])
                    profile = await scraper.scrape_wta_profile(p["wta_id"], slug)

                else:
                    skipped += 1
                    continue

                if profile and (profile.birth_date or profile.height_cm or profile.hand):
                    update_player(p["id"], profile, dry_run=dry_run, force=force)
                    enriched += 1
                else:
                    logger.warning(f"  No data extracted for {p['name']}")
                    failed += 1

            except Exception as e:
                logger.error(f"  Error scraping {p['name']}: {e}")
                failed += 1

            # Random delay between requests to be polite
            await asyncio.sleep(2)

    logger.info(f"\nDone. Enriched: {enriched}, Failed: {failed}, Skipped: {skipped}")


def main():
    parser = argparse.ArgumentParser(description="Enrich player profiles from ATP/WTA websites")
    parser.add_argument("--source", choices=["atp", "wta", "both"], default="both",
                        help="Which tour to scrape profiles from")
    parser.add_argument("--limit", type=int, default=0,
                        help="Maximum number of players to process (0 = all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be updated without writing to DB")
    parser.add_argument("--force", action="store_true",
                        help="Re-enrich all players, overwriting existing data")
    args = parser.parse_args()

    asyncio.run(run_enrichment(args.source, args.limit, args.dry_run, args.force))


if __name__ == "__main__":
    main()
