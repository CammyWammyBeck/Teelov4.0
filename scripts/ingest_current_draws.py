#!/usr/bin/env python3
"""
Ingest draws for currently-running tournaments.

Reuses the backfill_historical.py flow to discover tournaments, then filters
for those within ±7 days of today. For each current tournament, scrapes the
draw page and ingests entries into the database (with propagation).

Supports ATP main tour and ATP Challenger tour.

Usage:
    cd /home/cammybeck/Documents/programming/Teelov4.0
    source venv/bin/activate
    python scripts/ingest_current_draws.py

    # Specific tours only
    python scripts/ingest_current_draws.py --tours ATP
    python scripts/ingest_current_draws.py --tours CHALLENGER
    python scripts/ingest_current_draws.py --tours ATP,CHALLENGER

    # Dry run — show what would be processed without writing to DB
    python scripts/ingest_current_draws.py --dry-run
"""

import argparse
import asyncio
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.db.session import get_session
from teelo.db.models import Tournament, TournamentEdition
from teelo.players.identity import PlayerIdentityService
from teelo.scrape.atp import ATPScraper
from teelo.services.draw_ingestion import ingest_draw

# Reuse get_or_create_edition from backfill
sys.path.insert(0, str(Path(__file__).parent))
from backfill_historical import get_or_create_edition, TOUR_TYPES


# How many days either side of today counts as "current"
CURRENT_WINDOW_DAYS = 7


def is_current_tournament(tournament: dict, today: date) -> bool:
    """
    Check if a tournament is within the current window (±7 days of today).

    A tournament is "current" if:
    - Its start_date is within CURRENT_WINDOW_DAYS in the past, OR
    - Its end_date hasn't passed yet (still ongoing), OR
    - No dates available but it's the current year (fallback)

    Args:
        tournament: Tournament dict from get_tournament_list()
        today: Today's date

    Returns:
        True if the tournament should be processed
    """
    window_start = today - timedelta(days=CURRENT_WINDOW_DAYS)
    window_end = today + timedelta(days=CURRENT_WINDOW_DAYS)

    start_str = tournament.get("start_date")
    end_str = tournament.get("end_date")

    start_date = None
    end_date = None

    if start_str:
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            pass

    if end_str:
        try:
            end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            pass

    # If we have both dates, check if the tournament window overlaps today's window
    if start_date and end_date:
        return start_date <= window_end and end_date >= window_start

    # If we only have start_date, check if it's within range
    # (estimate end as start + 7 days for normal tournaments)
    if start_date:
        estimated_end = start_date + timedelta(days=7)
        return start_date <= window_end and estimated_end >= window_start

    # No date info — skip (we can't determine if it's current)
    return False


async def discover_current_tournaments(
    tours: list[str],
    year: int,
    today: date,
) -> list[dict]:
    """
    Discover currently-running tournaments using the same scraper as backfill.

    Args:
        tours: List of tour keys (e.g., ["ATP", "CHALLENGER"])
        year: Current year
        today: Today's date for filtering

    Returns:
        List of tournament dicts that are within the current window
    """
    all_current = []

    for tour_key in tours:
        tour_config = TOUR_TYPES[tour_key]

        # Only ATP-based scrapers support draws currently
        if tour_config["scraper"] != "atp":
            print(f"  Skipping {tour_key} — draw scraping not yet supported")
            continue

        print(f"\nFetching {tour_config['description']} tournament list for {year}...")

        try:
            async with ATPScraper(headless=False) as scraper:
                tournaments = await scraper.get_tournament_list(
                    year, tour_type=tour_config["tour_type"]
                )
        except Exception as e:
            print(f"  Error fetching tournament list: {e}")
            continue

        print(f"  Found {len(tournaments)} total tournaments")

        # Filter to current tournaments
        current = []
        for t in tournaments:
            if is_current_tournament(t, today):
                t["tour_key"] = tour_key
                current.append(t)

        print(f"  {len(current)} are currently running (±{CURRENT_WINDOW_DAYS} days)")
        for t in current:
            print(f"    - {t.get('name', t['id'])} ({t.get('start_date', '?')} to {t.get('end_date', '?')})")

        all_current.extend(current)

    return all_current


async def scrape_and_ingest_draw(
    session,
    tournament: dict,
    identity_service: PlayerIdentityService,
    dry_run: bool = False,
) -> dict:
    """
    Scrape a tournament's draw and ingest into the database.

    Args:
        session: Database session
        tournament: Tournament dict with id, number, year, tour_key, etc.
        identity_service: Player matching service
        dry_run: If True, scrape but don't write to DB

    Returns:
        Dict with statistics
    """
    tour_key = tournament["tour_key"]
    tour_config = TOUR_TYPES[tour_key]
    tournament_id = tournament["id"]
    tournament_number = tournament.get("number")
    year = tournament.get("year", datetime.now().year)

    result = {
        "tournament": tournament.get("name", tournament_id),
        "entries_scraped": 0,
        "matches_created": 0,
        "byes": 0,
        "propagations": 0,
        "error": None,
    }

    try:
        # Scrape the draw
        print(f"\n  Scraping draw for {tournament_id}...")
        async with ATPScraper(headless=False) as scraper:
            entries = await scraper.scrape_tournament_draw(
                tournament_id,
                year,
                tournament_number=tournament_number,
                tour_type=tour_config.get("tour_type", "main"),
            )

        result["entries_scraped"] = len(entries)
        print(f"  Scraped {len(entries)} draw entries")

        if not entries:
            print(f"  No draw data found — skipping")
            return result

        if dry_run:
            # Just print what we found without writing
            from collections import Counter
            rounds = Counter(e.round for e in entries)
            byes = sum(1 for e in entries if e.is_bye)
            completed = sum(1 for e in entries if e.winner_name and not e.is_bye)
            tbd = sum(1 for e in entries if not e.is_bye
                      and (not e.player_a_name or not e.player_b_name))
            print(f"  Rounds: {dict(rounds)}")
            print(f"  Completed: {completed}, Byes: {byes}, TBD: {tbd}")
            return result

        # Get or create edition (reuses backfill pattern)
        task_params = {
            "tournament_id": tournament_id,
            "year": year,
            "tour_key": tour_key,
            "tournament_name": tournament.get("name"),
            "tournament_level": tournament.get("level"),
            "tournament_surface": tournament.get("surface"),
            "tournament_location": tournament.get("location"),
            "start_date": tournament.get("start_date"),
            "end_date": tournament.get("end_date"),
            "tour_type": tour_config.get("tour_type", "main"),
        }
        if tournament_number:
            task_params["tournament_number"] = tournament_number

        edition = await get_or_create_edition(session, task_params, tour_key)

        # Ingest the draw
        stats = ingest_draw(session, entries, edition, identity_service)

        result["matches_created"] = stats.matches_created
        result["byes"] = stats.byes_processed
        result["propagations"] = stats.propagations_created

        print(f"\n  {stats.summary()}")

    except Exception as e:
        result["error"] = str(e)
        print(f"  ERROR: {e}")

    return result


async def main():
    parser = argparse.ArgumentParser(
        description="Ingest draws for currently-running ATP tournaments",
    )
    parser.add_argument(
        "--tours", type=str, default="ATP,CHALLENGER",
        help="Comma-separated tour types (default: ATP,CHALLENGER)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Scrape draws but don't write to database",
    )
    args = parser.parse_args()

    tours = [t.strip().upper() for t in args.tours.split(",")]
    today = date.today()
    year = today.year

    print("=" * 70)
    print("Teelo v4.0 — Current Tournament Draw Ingestion")
    print(f"Date: {today}  |  Window: ±{CURRENT_WINDOW_DAYS} days")
    print(f"Tours: {', '.join(tours)}")
    if args.dry_run:
        print("MODE: DRY RUN (no database writes)")
    print("=" * 70)

    # Step 1: Discover current tournaments
    print("\n" + "-" * 70)
    print("STEP 1: Discovering current tournaments")
    print("-" * 70)

    current_tournaments = await discover_current_tournaments(tours, year, today)

    if not current_tournaments:
        print("\nNo current tournaments found. Nothing to do.")
        return

    # Step 2: Scrape draws and ingest
    print("\n" + "-" * 70)
    print(f"STEP 2: Scraping and ingesting draws for {len(current_tournaments)} tournaments")
    print("-" * 70)

    results = []
    with get_session() as session:
        identity_service = PlayerIdentityService(session)

        for tournament in current_tournaments:
            result = await scrape_and_ingest_draw(
                session, tournament, identity_service,
                dry_run=args.dry_run,
            )
            results.append(result)

            if not args.dry_run and result["error"] is None:
                # Commit after each successful tournament
                session.commit()
                print(f"  Committed to database.")

    # Step 3: Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    for r in results:
        status = f"ERROR: {r['error']}" if r["error"] else "OK"
        print(
            f"  {r['tournament']}: "
            f"{r['entries_scraped']} entries, "
            f"{r['matches_created']} matches created, "
            f"{r['byes']} byes, "
            f"{r['propagations']} propagations "
            f"[{status}]"
        )

    total_created = sum(r["matches_created"] for r in results)
    total_entries = sum(r["entries_scraped"] for r in results)
    print(f"\n  Total: {total_entries} entries scraped, {total_created} matches created")

    if args.dry_run:
        print("\n  (Dry run — nothing written to database)")

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
