#!/usr/bin/env python3
"""
Update Current Events Script.

Discovers currently running tournaments (within ±1 week) across all tours
(ATP, WTA, ITF), scrapes their draws, schedules, and results, and ingests
the data into the database.

Usage:
    python scripts/update_current_events.py
    python scripts/update_current_events.py --tours ATP,WTA
    python scripts/update_current_events.py --discover-only
    python scripts/update_current_events.py --process-only --tasks-file current_tasks.json
"""

import argparse
import asyncio
import json
import sys
from datetime import date, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from teelo.scrape.atp import ATPScraper
from teelo.scrape.base import VirtualDisplay
from teelo.scrape.discovery import discover_tournament_tasks
from teelo.scrape.itf import ITFScraper
from teelo.scrape.pipeline import TaskParams, TournamentTask, execute_task
from teelo.scrape.utils import TOUR_TYPES
from teelo.scrape.wta import WTAScraper
from teelo.config import settings
from teelo.db.session import SessionLocal
from teelo.players.identity import PlayerIdentityService


def _get_scraper_class(tour_key: str):
    scraper_type = TOUR_TYPES[tour_key]["scraper"]
    if scraper_type == "atp":
        return ATPScraper
    if scraper_type == "wta":
        return WTAScraper
    if scraper_type == "itf":
        return ITFScraper
    raise ValueError(f"Unknown scraper type for {tour_key}")


def _serialize_tasks(tasks: list[TournamentTask]) -> list[dict]:
    return [
        {"task_type": task.task_type, "params": task.params.to_dict()}
        for task in tasks
    ]


def _load_tasks(path: Path) -> list[TournamentTask]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    tasks: list[TournamentTask] = []
    for item in payload:
        params = TaskParams.from_dict(item["params"])
        tasks.append(TournamentTask(task_type=item["task_type"], params=params))
    return tasks


def _save_tasks(path: Path, tasks: list[TournamentTask]) -> None:
    payload = _serialize_tasks(tasks)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


async def process_task(
    scraper,
    task_params: TaskParams,
    session,
    identity_service: PlayerIdentityService,
):
    """Run pipeline for a single tournament using a shared scraper/session."""
    tour_key = task_params.tour_key
    tournament_name = task_params.tournament_name or task_params.tournament_id
    print(f"Processing {tournament_name} ({tour_key})...")

    await execute_task(
        task_params,
        scraper=scraper,
        session=session,
        identity_service=identity_service,
        mode="current",
    )


async def discover_tour_tasks(
    tour_key: str,
    year: int,
    today: date,
    headless: bool,
    semaphore: asyncio.Semaphore,
) -> list[TournamentTask]:
    """Discover current tournaments for one tour."""
    scraper_cls = _get_scraper_class(tour_key)
    window_start = today - timedelta(days=7)
    window_end = today + timedelta(days=7)

    print(f"\n[{tour_key}] Starting tour discovery...")

    async with semaphore:
        async with scraper_cls(headless=headless) as scraper:
            print(
                f"[{tour_key}] Discovering tournaments for {year} "
                f"(Window: {window_start} to {window_end})..."
            )
            tasks = await discover_tournament_tasks(
                tour_key,
                year,
                task_type="current_tournament",
                scraper=scraper,
                window=(window_start, window_end),
            )
            print(f"[{tour_key}] Found {len(tasks)} current tournaments.")
            return tasks


async def process_tour_tasks(
    tour_key: str,
    headless: bool,
    semaphore: asyncio.Semaphore,
    tasks: list[TournamentTask],
) -> int:
    """Process current tournaments for one tour with shared resources."""
    scraper_cls = _get_scraper_class(tour_key)

    print(f"\n[{tour_key}] Starting tour worker...")

    async with semaphore:
        async with scraper_cls(headless=headless) as scraper:
            if not tasks:
                return 0

            # Process in chronological order so immediate tournaments are updated first.
            tasks.sort(
                key=lambda task: (
                    task.params.start_date or "9999-12-31",
                    task.params.tournament_name or "",
                )
            )

            session = SessionLocal()
            identity_service = PlayerIdentityService(session)
            processed = 0
            try:
                for task in tasks:
                    try:
                        await process_task(
                            scraper=scraper,
                            task_params=task.params,
                            session=session,
                            identity_service=identity_service,
                        )
                        processed += 1
                    except Exception as e:
                        session.rollback()
                        name = task.params.tournament_name or task.params.tournament_id
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
    parser.add_argument("--discover-only", action="store_true", help="Discover current tournaments only")
    parser.add_argument("--process-only", action="store_true", help="Process from tasks file only (skip discovery)")
    parser.add_argument("--tasks-file", type=str, default="current_tasks.json", help="Path to tasks JSON file")
    args = parser.parse_args()

    if args.discover_only and args.process_only:
        raise SystemExit("Error: --discover-only cannot be combined with --process-only.")

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
    tasks_file = Path(args.tasks_file)
    tasks_by_tour: dict[str, list[TournamentTask]] = {tour: [] for tour in tours}

    if args.process_only:
        if not tasks_file.exists():
            raise SystemExit(f"Tasks file not found: {tasks_file}")
        loaded_tasks = _load_tasks(tasks_file)
        for task in loaded_tasks:
            if task.params.tour_key in tasks_by_tour:
                tasks_by_tour[task.params.tour_key].append(task)
    else:
        discovered = await asyncio.gather(
            *(
                discover_tour_tasks(
                    tour_key=t,
                    year=args.year,
                    today=today,
                    headless=headless,
                    semaphore=semaphore,
                )
                for t in tours
            ),
            return_exceptions=True,
        )
        all_tasks: list[TournamentTask] = []
        for tour_key, result in zip(tours, discovered):
            if isinstance(result, Exception):
                print(f"[{tour_key}] Discovery failed: {result}")
                continue
            tasks_by_tour[tour_key] = result
            all_tasks.extend(result)

        _save_tasks(tasks_file, all_tasks)
        print(f"\nSaved {len(all_tasks)} tasks to {tasks_file}")

        if args.discover_only:
            print("\nDiscovery complete (--discover-only).")
            return

    results = await asyncio.gather(
        *(
            process_tour_tasks(
                tour_key=t,
                headless=headless,
                semaphore=semaphore,
                tasks=tasks_by_tour.get(t, []),
            )
            for t in tours
        ),
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
