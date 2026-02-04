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

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from teelo.scrape.atp import ATPScraper
from teelo.scrape.wta import WTAScraper
from teelo.scrape.itf import ITFScraper
from teelo.scrape.base import VirtualDisplay
from teelo.scrape.utils import TOUR_TYPES, get_tournaments_for_tour
from teelo.config import settings
from teelo.db.session import get_session
from teelo.players.identity import PlayerIdentityService
from teelo.services.draw_ingestion import ingest_draw
from teelo.services.schedule_ingestion import ingest_schedule
from teelo.services.results_ingestion import ingest_results
from backfill_historical import get_or_create_edition


async def discover_current_tournaments(tours: list[str], year: int, today: date) -> list[dict]:
    """Discover tournaments running within ±7 days."""
    all_tournaments = []
    window_start = today - timedelta(days=7)
    window_end = today + timedelta(days=7)
    
    print(f"Discovering tournaments for {year} (Window: {window_start} to {window_end})...")
    
    for tour_key in tours:
        print(f"  Checking {tour_key}...")
        
        try:
            # Use shared utility for discovery
            tournaments = await get_tournaments_for_tour(tour_key, year)
            
            # Filter
            current_count = 0
            for t in tournaments:
                # Ensure tour_key is attached
                t["tour_key"] = tour_key
                
                start_str = t.get("start_date")
                end_str = t.get("end_date")
                
                # Parse dates
                s_date, e_date = None, None
                if start_str:
                    try:
                        s_date = datetime.strptime(start_str, "%Y-%m-%d").date()
                    except: pass
                if end_str:
                    try:
                        e_date = datetime.strptime(end_str, "%Y-%m-%d").date()
                    except: pass
                    
                # Logic: 
                # 1. Overlap with window if both dates known
                # 2. Start date in window (assume 1 week duration if end unknown)
                is_current = False
                if s_date and e_date:
                    if s_date <= window_end and e_date >= window_start:
                        is_current = True
                elif s_date:
                    est_end = s_date + timedelta(days=7)
                    if s_date <= window_end and est_end >= window_start:
                        is_current = True
                # Fallback for ongoing without dates (rare/unlikely but safe to skip)
                
                if is_current:
                    all_tournaments.append(t)
                    current_count += 1
                    
            print(f"    Found {len(tournaments)} total, {current_count} current.")
            
        except Exception as e:
            print(f"    Error fetching list: {e}")
            
    return all_tournaments


async def process_tournament(tournament: dict, session):
    """Run pipeline for a single tournament."""
    tour_key = tournament["tour_key"]
    tour_config = TOUR_TYPES[tour_key]
    
    # Initialize appropriate scraper based on config
    scraper_kwargs = {"headless": False}
    if tour_config["scraper"] == "atp":
        scraper_cls = ATPScraper
    elif tour_config["scraper"] == "wta":
        scraper_cls = WTAScraper
    elif tour_config["scraper"] == "itf":
        scraper_cls = ITFScraper
    else:
        print(f"Unknown scraper type for {tour_key}")
        return

    tournament_name = tournament.get("name", tournament["id"])
    print(f"Processing {tournament_name} ({tour_key})...")

    identity_service = PlayerIdentityService(session)

    # Create or fetch tournament edition
    task_params = {
        "tournament_id": tournament["id"],
        "year": tournament["year"],
        "tour_key": tour_key,
        "tournament_name": tournament.get("name"),
        "tournament_level": tournament.get("level"),
        "tournament_surface": tournament.get("surface"),
        "tournament_location": tournament.get("location"),
        "start_date": tournament.get("start_date"),
        "end_date": tournament.get("end_date"),
    }
    if tournament.get("number"):
        task_params["tournament_number"] = tournament.get("number")
    if tournament.get("url"):
        task_params["tournament_url"] = tournament.get("url")

    edition = await get_or_create_edition(session, task_params, tour_key)
    
    async with scraper_cls(**scraper_kwargs) as scraper:
        # 1. DRAW
        try:
            print("  Scraping Draw...")
            # Prepare args
            draw_kwargs = {
                "tournament_id": tournament["id"],
                "year": tournament["year"],
            }
            # Add specific args
            if tour_key in ["ATP", "CHALLENGER"]:
                draw_kwargs["tournament_number"] = tournament.get("number")
                draw_kwargs["tour_type"] = tour_config["tour_type"]
            elif tour_key in ["WTA", "WTA_125"]:
                draw_kwargs["tournament_number"] = tournament.get("number")
            elif tour_key.startswith("ITF"):
                # ITF needs full URL and info dict
                draw_kwargs = {
                    "tournament_url": tournament.get("url"), # Scraper needs this
                    "tournament_info": tournament
                }
            
            entries = await scraper.scrape_tournament_draw(**draw_kwargs)
            stats = ingest_draw(session, entries, edition, identity_service)
            print(f"  Draw: {stats.summary()}")
        except Exception as e:
            print(f"  Draw Error: {e}")
            session.rollback()

        # 2. SCHEDULE
        try:
            print("  Scraping Schedule...")
            sched_kwargs = {}
            if tour_key.startswith("ITF"):
                sched_kwargs["tournament_url"] = tournament.get("url")
            elif tour_key in ["ATP", "CHALLENGER", "WTA", "WTA_125"]:
                sched_kwargs = {
                    "tournament_id": tournament["id"],
                    "tournament_number": tournament.get("number")
                }
                # Add year for WTA if needed (ATP doesn't use it in updated signature but good practice)
                if tour_key in ["WTA", "WTA_125"]:
                    sched_kwargs["year"] = tournament["year"]

            # Collect generator
            fixtures = []
            async for f in scraper.scrape_fixtures(**sched_kwargs):
                fixtures.append(f)
            stats = ingest_schedule(session, fixtures, edition, identity_service)
            print(f"  Schedule: {stats.summary()}")
        except Exception as e:
            print(f"  Schedule Error: {e}")
            session.rollback()

        # 3. RESULTS
        try:
            print("  Scraping Results...")
            res_kwargs = {
                "tournament_id": tournament["id"],
                "year": tournament["year"],
            }
            if tour_key in ["ATP", "CHALLENGER"]:
                res_kwargs["tournament_number"] = tournament.get("number")
                res_kwargs["tour_type"] = tour_config["tour_type"]
            elif tour_key in ["WTA", "WTA_125"]:
                res_kwargs["tournament_number"] = tournament.get("number")
            elif tour_key.startswith("ITF"):
                res_kwargs = {
                    "tournament_url": tournament.get("url"),
                    "tournament_info": tournament
                }

            matches = []
            async for m in scraper.scrape_tournament_results(**res_kwargs):
                matches.append(m)
            stats = ingest_results(session, matches, edition, identity_service)
            print(f"  Results: {stats.summary()}")
        except Exception as e:
            print(f"  Results Error: {e}")
            session.rollback()


async def main():
    parser = argparse.ArgumentParser(description="Update Current Events")
    parser.add_argument("--tours", default="ATP,WTA,CHALLENGER,WTA_125,ITF_MEN,ITF_WOMEN", help="Comma-separated tours")
    args = parser.parse_args()
    
    tours = [t.strip().upper() for t in args.tours.split(",")]
    
    # Validate tours
    tours = [t for t in tours if t in TOUR_TYPES]
    
    print("=" * 60)
    print("UPDATE CURRENT EVENTS")
    print(f"Tours: {tours}")
    print(f"Settings: headless={settings.scrape_headless}, virtual_display={settings.scrape_virtual_display}")
    print("=" * 60)
    
    # Explicitly ensure virtual display is running if configured
    if settings.scrape_virtual_display:
        print("Starting Virtual Display...")
        VirtualDisplay.ensure_running()
    
    # 1. Discover
    tournaments = await discover_current_tournaments(tours, date.today().year, date.today())
    
    if not tournaments:
        print("No current tournaments found.")
        return

    print(f"\nProcessing {len(tournaments)} tournaments...")
    
    # 2. Process
    with get_session() as session:
        for t in tournaments:
            await process_tournament(t, session)
            session.commit()
        
    print("\nDone! Database updated.")

if __name__ == "__main__":
    asyncio.run(main())
