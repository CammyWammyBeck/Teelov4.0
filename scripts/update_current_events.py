#!/usr/bin/env python3
"""
Update Current Events Script.

Discovers currently running tournaments (within ±1 week) across all tours
(ATP, WTA, ITF), scrapes their draws, schedules, and results, and ingests
the data into the database.

Usage:
    python scripts/update_current_events.py
    python scripts/update_current_events.py --tours ATP,WTA
    python scripts/update_current_events.py
"""

import argparse
import asyncio
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from teelo.scrape.atp import ATPScraper
from teelo.scrape.base import VirtualDisplay
from teelo.scrape.itf import ITFScraper
from teelo.scrape.pipeline import build_task_params, execute_task
from teelo.scrape.utils import TOUR_TYPES
from teelo.scrape.wta import WTAScraper
from teelo.config import settings
from teelo.db.session import SessionLocal
from teelo.players.identity import PlayerIdentityService


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def _is_tournament_current(
    tournament: dict,
    window_start: date,
    window_end: date,
) -> bool:
    start_date = _parse_date(tournament.get("start_date"))
    end_date = _parse_date(tournament.get("end_date"))

    if start_date and end_date:
        return start_date <= window_end and end_date >= window_start
    if start_date:
        est_end = start_date + timedelta(days=7)
        return start_date <= window_end and est_end >= window_start
    return False


async def _discover_tournaments_with_scraper(
    scraper,
    tour_key: str,
    year: int,
) -> list[dict]:
    config = TOUR_TYPES[tour_key]
    if config["scraper"] == "atp":
        tournaments = await scraper.get_tournament_list(year, tour_type=config["tour_type"])
    elif config["scraper"] == "wta":
        tournaments = await scraper.get_tournament_list(year, tour_type=config["tour_type"])
    else:
        tournaments = await scraper.get_tournament_list(year, gender=config["gender"])

    for tournament in tournaments:
        tournament["tour_key"] = tour_key
        tournament.setdefault("year", year)
    return tournaments


def _get_scraper_class(tour_key: str):
    scraper_type = TOUR_TYPES[tour_key]["scraper"]
    if scraper_type == "atp":
        return ATPScraper
    if scraper_type == "wta":
        return WTAScraper
    if scraper_type == "itf":
        return ITFScraper
    raise ValueError(f"Unknown scraper type for {tour_key}")


async def process_tournament(
    scraper,
    tournament: dict,
    session,
    identity_service: PlayerIdentityService,
    today: date,
):
    """Run pipeline for a single tournament using a shared scraper/session."""
    window_start = today - timedelta(days=7)
    window_end = today + timedelta(days=7)
    if not _is_tournament_current(tournament, window_start, window_end):
        return

    tour_key = tournament["tour_key"]
    tournament_name = tournament.get("name", tournament["id"])
    print(f"Processing {tournament_name} ({tour_key})...")

    task_params = build_task_params(tournament, tour_key)
    await execute_task(
        task_params,
        scraper=scraper,
        session=session,
        identity_service=identity_service,
        mode="current",
    )


async def process_tour(
    tour_key: str,
    year: int,
    today: date,
    headless: bool,
    semaphore: asyncio.Semaphore,
) -> int:
    """Discover and process current tournaments for one tour with shared resources."""
    scraper_cls = _get_scraper_class(tour_key)
    window_start = today - timedelta(days=7)
    window_end = today + timedelta(days=7)

    print(f"\n[{tour_key}] Starting tour worker...")

    async with semaphore:
        async with scraper_cls(headless=headless) as scraper:
            print(
                f"[{tour_key}] Discovering tournaments for {year} "
                f"(Window: {window_start} to {window_end})..."
            )
            tournaments = await _discover_tournaments_with_scraper(scraper, tour_key, year)
            current = [t for t in tournaments if _is_tournament_current(t, window_start, window_end)]
            print(f"[{tour_key}] Found {len(tournaments)} total, {len(current)} current.")

            if not current:
                return 0

            # Process in chronological order so immediate tournaments are updated first.
            current.sort(key=lambda t: (t.get("start_date") or "9999-12-31", t.get("name") or ""))

            session = SessionLocal()
            identity_service = PlayerIdentityService(session)
            processed = 0
            try:
                for tournament in current:
                    try:
                        await process_tournament(
                            scraper=scraper,
                            tournament=tournament,
                            session=session,
                            identity_service=identity_service,
                            today=today,
                        )
                        processed += 1
                    except Exception as e:
                        session.rollback()
                        name = tournament.get("name", tournament.get("id", "unknown"))
                        print(f"[{tour_key}] Tournament failed ({name}): {e}")
                return processed
            finally:
                session.close()


async def main():
    parser = argparse.ArgumentParser(description="Update Current Events")
    parser.add_argument("--tours", default="ATP,WTA,CHALLENGER,WTA_125,ITF_MEN,ITF_WOMEN", help="Comma-separated tours")
    parser.add_argument("--year", type=int, default=date.today().year, help="Season year to scan")
    parser.add_argument("--max-parallel-tours", type=int, default=3, help="Max tour workers to run concurrently")
    parser.add_argument("--headed", action="store_true", help="Force headed browser mode (slower)")
    args = parser.parse_args()
    
    tours = [t.strip().upper() for t in args.tours.split(",")]
    
    # Validate tours
    tours = [t for t in tours if t in TOUR_TYPES]
    
    print("=" * 60)
    print("UPDATE CURRENT EVENTS")
    print(f"Tours: {tours}")
    headless = False if args.headed else settings.scrape_headless
    print(
        "Settings: "
        f"headless={headless}, "
        f"virtual_display={settings.scrape_virtual_display}, "
        f"timeout_ms={settings.scrape_timeout}, "
        f"delays={settings.scrape_delay_min}-{settings.scrape_delay_max}s"
    )
    print("=" * 60)
    
    # Explicitly ensure virtual display is running if configured
    if settings.scrape_virtual_display and not headless:
        print("Starting Virtual Display...")
        VirtualDisplay.ensure_running()

    semaphore = asyncio.Semaphore(max(1, args.max_parallel_tours))
    today = date.today()
    results = await asyncio.gather(
        *(process_tour(tour_key=t, year=args.year, today=today, headless=headless, semaphore=semaphore) for t in tours),
        return_exceptions=True,
    )

    total_processed = 0
    for tour_key, result in zip(tours, results):
        if isinstance(result, Exception):
            print(f"[{tour_key}] Worker failed: {result}")
            continue
        total_processed += result
        print(f"[{tour_key}] Processed {result} tournaments.")

    print(f"\nDone! Database updated. Tournaments processed: {total_processed}")

if __name__ == "__main__":
    asyncio.run(main())
