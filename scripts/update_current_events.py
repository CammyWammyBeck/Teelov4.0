#!/usr/bin/env python3
"""
Update Current Events Script.

Discovers currently running tournaments (within ±1 week) across all tours
(ATP, WTA, ITF), scrapes their draws, schedules, and results, and logs
the data to a text file for verification.

Usage:
    python scripts/update_current_events.py
    python scripts/update_current_events.py --tours ATP,WTA
    python scripts/update_current_events.py --output my_log.txt
"""

import argparse
import asyncio
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.scrape.atp import ATPScraper
from teelo.scrape.wta import WTAScraper
from teelo.scrape.itf import ITFScraper
from teelo.scrape.base import ScrapedDrawEntry, ScrapedFixture, ScrapedMatch, VirtualDisplay
from teelo.scrape.utils import TOUR_TYPES, get_tournaments_for_tour
from teelo.config import settings

class FileLogger:
    """Handles logging of scraped data to a text file."""
    
    def __init__(self, filename: str):
        self.filename = filename
        # Clear file on init
        with open(self.filename, "w", encoding="utf-8") as f:
            f.write(f"TEELO UPDATE LOG - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
    def log(self, text: str):
        """Write raw text to file."""
        with open(self.filename, "a", encoding="utf-8") as f:
            f.write(text + "\n")
            
    def log_header(self, tournament: dict):
        """Log tournament header."""
        name = tournament.get("name", tournament["id"])
        dates = f"{tournament.get('start_date', '?')} to {tournament.get('end_date', '?')}"
        level = tournament.get("level", "Unknown")
        surface = tournament.get("surface", "Unknown")
        
        self.log("\n" + "#" * 80)
        self.log(f"TOURNAMENT: {name}")
        self.log(f"ID: {tournament['id']} | Dates: {dates}")
        self.log(f"Level: {level} | Surface: {surface}")
        self.log("#" * 80 + "\n")

    def log_draw(self, entries: list[ScrapedDrawEntry]):
        """Log draw entries."""
        self.log(f"--- DRAW ENTRIES ({len(entries)}) ---")
        if not entries:
            self.log("(No draw entries found)\n")
            return
            
        # Group by round
        by_round = {}
        for e in entries:
            by_round.setdefault(e.round, []).append(e)
            
        for r, items in by_round.items():
            self.log(f"\n[Round: {r}]")
            for item in items:
                p1 = f"{item.player_a_name}"
                if item.player_a_seed: p1 += f" ({item.player_a_seed})"
                
                p2 = f"{item.player_b_name}"
                if item.player_b_seed: p2 += f" ({item.player_b_seed})"
                
                status = ""
                if item.winner_name:
                    status = f" -> Winner: {item.winner_name} ({item.score_raw})"
                elif item.is_bye:
                    status = " (BYE)"
                    
                self.log(f"  #{item.draw_position}: {p1} vs {p2}{status}")
        self.log("")

    def log_schedule(self, fixtures: list[ScrapedFixture]):
        """Log schedule/fixtures."""
        self.log(f"--- SCHEDULE / ORDER OF PLAY ({len(fixtures)}) ---")
        if not fixtures:
            self.log("(No scheduled matches found)\n")
            return
            
        # Sort by date/time/court
        fixtures.sort(key=lambda x: (x.scheduled_date or "9999", x.scheduled_time or "99:99", x.court or "Z"))
        
        current_date = None
        for f in fixtures:
            if f.scheduled_date != current_date:
                current_date = f.scheduled_date
                self.log(f"\n[Date: {current_date}]")
                
            time_str = f.scheduled_time if f.scheduled_time else "TBD"
            court_str = f.court if f.court else "TBA"
            
            p1 = f"{f.player_a_name}"
            p2 = f"{f.player_b_name}"
            
            self.log(f"  {time_str} | {court_str} | {p1} vs {p2} ({f.round})")
        self.log("")

    def log_results(self, matches: list[ScrapedMatch]):
        """Log completed results."""
        self.log(f"--- RESULTS ({len(matches)}) ---")
        if not matches:
            self.log("(No results found)\n")
            return
            
        matches.sort(key=lambda x: (x.match_date or "0000", x.match_number or 0))
        
        for m in matches:
            date_str = f"[{m.match_date}] " if m.match_date else ""
            loser = m.player_b_name or "Unknown"
            self.log(f"  {date_str}{m.round}: {m.winner_name} d. {loser} {m.score_raw}")
        self.log("")


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


async def process_tournament(tournament: dict, logger: FileLogger):
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

    print(f"Processing {tournament['name']} ({tour_key})...")
    logger.log_header(tournament)
    
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
            logger.log_draw(entries)
        except Exception as e:
            print(f"  Draw Error: {e}")
            logger.log(f"Error scraping draw: {e}\n")

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
            logger.log_schedule(fixtures)
        except Exception as e:
            print(f"  Schedule Error: {e}")
            logger.log(f"Error scraping schedule: {e}\n")

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
            logger.log_results(matches)
        except Exception as e:
            print(f"  Results Error: {e}")
            logger.log(f"Error scraping results: {e}\n")


async def main():
    parser = argparse.ArgumentParser(description="Update Current Events Log")
    parser.add_argument("--tours", default="ATP,WTA,CHALLENGER,WTA_125,ITF_MEN,ITF_WOMEN", help="Comma-separated tours")
    parser.add_argument(
        "--output",
        default=str(Path("scratchpad") / f"scraped_updates_{date.today()}.txt"),
        help="Output filename",
    )
    args = parser.parse_args()
    
    tours = [t.strip().upper() for t in args.tours.split(",")]
    
    # Validate tours
    tours = [t for t in tours if t in TOUR_TYPES]
    
    logger = FileLogger(args.output)
    
    print("=" * 60)
    print("UPDATE CURRENT EVENTS")
    print(f"Output: {args.output}")
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
    for t in tournaments:
        await process_tournament(t, logger)
        
    print("\nDone! Check log file for details.")

if __name__ == "__main__":
    asyncio.run(main())
