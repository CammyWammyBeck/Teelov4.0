#!/usr/bin/env python3
"""
Historical Data Backfill Script.

Orchestrates scraping of historical tennis data (2020-2024) across multiple tours.
Uses the queue-based approach for reliable processing with retry logic.

Tours supported:
- ATP Main Tour (Grand Slams, Masters 1000, ATP 500, ATP 250)
- ATP Challenger Tour
- ITF Men's World Tennis Tour
- ITF Women's World Tennis Tour
- WTA Tour
- WTA 125 Tour

Usage:
    # Full backfill of all tours for 2020-2024
    python scripts/backfill_historical.py --years 2020-2024

    # Specific tours only
    python scripts/backfill_historical.py --years 2024 --tours ATP,CHALLENGER

    # Resume interrupted backfill (processes existing queue)
    python scripts/backfill_historical.py --resume

    # Discover tournaments only (no processing)
    python scripts/backfill_historical.py --discover-only

    # Process queue only (no new tasks added)
    python scripts/backfill_historical.py --process-only

    # Show queue statistics
    python scripts/backfill_historical.py --stats

Examples:
    # Start with most recent ATP data
    python scripts/backfill_historical.py --years 2024 --tours ATP

    # Backfill ITF men's data
    python scripts/backfill_historical.py --years 2023-2024 --tours ITF_MEN

    # Full historical backfill (will take days)
    python scripts/backfill_historical.py --years 2020-2024 --tours ATP,CHALLENGER,ITF_MEN,ITF_WOMEN,WTA
"""

import argparse
import asyncio
import json
import multiprocessing
import sys
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import func, cast, Integer

from teelo.config import settings
from teelo.db import get_session
from teelo.db.models import ScrapeQueue
from teelo.scrape.atp import ATPScraper
from teelo.scrape.base import VirtualDisplay
from teelo.scrape.discovery import discover_tournament_tasks
from teelo.scrape.itf import ITFScraper
from teelo.scrape.pipeline import TaskParams, execute_task
from teelo.scrape.queue import ScrapeQueueManager
from teelo.scrape.utils import TOUR_TYPES
from teelo.scrape.wta import WTAScraper
from teelo.players.identity import PlayerIdentityService


# Processing order (most important first)
TOUR_ORDER = ["ATP", "CHALLENGER", "WTA", "WTA_125", "ITF_MEN", "ITF_WOMEN"]


def _get_scraper_class(tour_key: str):
    scraper_type = TOUR_TYPES[tour_key]["scraper"]
    if scraper_type == "atp":
        return ATPScraper
    if scraper_type == "wta":
        return WTAScraper
    if scraper_type == "itf":
        return ITFScraper
    raise ValueError(f"Unknown scraper type for {tour_key}")


def apply_fast_delays(enabled: bool) -> None:
    if not enabled:
        return

    settings.scrape_delay_min = 0.3
    settings.scrape_delay_max = 0.8


def parse_year_range(year_str: str) -> list[int]:
    """
    Parse a year string into a list of years.

    Args:
        year_str: Year string like "2024" or "2020-2024"

    Returns:
        List of years in descending order (newest first)

    Examples:
        >>> parse_year_range("2024")
        [2024]
        >>> parse_year_range("2020-2024")
        [2024, 2023, 2022, 2021, 2020]
    """
    if "-" in year_str:
        start, end = year_str.split("-")
        years = list(range(int(start), int(end) + 1))
        return sorted(years, reverse=True)  # Newest first
    else:
        return [int(year_str)]


def parse_tours(tours_str: Optional[str]) -> list[str]:
    """
    Parse tour string into list of tour types.

    Args:
        tours_str: Comma-separated tour types, or None for all

    Returns:
        List of tour type keys
    """
    if not tours_str:
        return TOUR_ORDER

    tours = [t.strip().upper() for t in tours_str.split(",")]

    # Validate tour types without mutating the list while iterating
    valid_tours: list[str] = []
    for tour in tours:
        if tour in TOUR_TYPES:
            valid_tours.append(tour)
        else:
            print(f"Warning: Unknown tour type '{tour}'. Valid types: {', '.join(TOUR_TYPES.keys())}")

    # Sort by processing order
    return [t for t in TOUR_ORDER if t in valid_tours]


async def populate_queue(
    session,
    queue_manager: ScrapeQueueManager,
    years: list[int],
    tours: list[str],
) -> tuple[int, list[dict[str, float | int | str]]]:
    """
    Populate the scrape queue with tournament tasks.

    Fetches tournament lists for each tour/year combination and adds
    tasks to the queue with appropriate priorities.

    Args:
        session: Database session
        queue_manager: ScrapeQueueManager instance
        years: List of years to backfill (newest first)
        tours: List of tour types to include

    Returns:
        Number of tasks added to queue
    """
    tasks_added = 0
    discovery_metrics: list[dict[str, float | int | str]] = []
    
    # Current date for filtering future tournaments
    today = datetime.now().date()
    # Include tournaments starting within the next 7 days to capture qualifying
    future_cutoff = today + timedelta(days=7)

    for tour_key in tours:
        tour_config = TOUR_TYPES[tour_key]
        print(f"\n{'=' * 60}")
        print(f"Loading tournaments for: {tour_config['description']}")
        print("=" * 60)

        for year in years:
            # Calculate priority based on year (recent = higher priority)
            # 2024 = priority 7, 2020 = priority 9
            base_priority = 7 + min(max(2024 - year, 0), 2)
            
            year_tasks_added = 0
            skipped_future = 0
            tasks_to_add: list[ScrapeQueue] = []

            try:
                # Fetch existing tasks for this tour/year to avoid duplicates
                existing_tournament_ids = set(
                    tid for (tid,) in (
                        session.query(
                            ScrapeQueue.task_params["tournament_id"].astext
                        )
                        .filter(
                            ScrapeQueue.task_type == "historical_tournament",
                            ScrapeQueue.status.in_(["pending", "in_progress", "retry"]),
                            ScrapeQueue.task_params["tour_key"].astext == tour_key,
                            cast(ScrapeQueue.task_params["year"].astext, Integer) == year,
                        )
                        .all()
                    )
                    if tid
                )

                discovery_start = perf_counter()
                tasks = await discover_tournament_tasks(
                    tour_key,
                    year,
                    task_type="historical_tournament",
                )
                discovery_elapsed = perf_counter() - discovery_start
                discovery_metrics.append(
                    {
                        "tour_key": tour_key,
                        "year": year,
                        "duration_s": discovery_elapsed,
                        "tasks_found": len(tasks),
                    }
                )

                for task in tasks:
                    # Skip tournaments that are too far in the future
                    start_date_str = task.params.start_date
                    if start_date_str:
                        try:
                            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                            if start_date > future_cutoff:
                                skipped_future += 1
                                continue
                        except (ValueError, TypeError):
                            pass

                    # Skip if already queued
                    if task.params.tournament_id in existing_tournament_ids:
                        continue

                    # Enqueue task (bulk later)
                    tasks_to_add.append(
                        ScrapeQueue(
                            task_type="historical_tournament",
                            task_params=task.params.to_dict(),
                            priority=base_priority,
                            max_attempts=3,
                            status="pending",
                        )
                    )
                    year_tasks_added += 1
                    tasks_added += 1
                
                msg = f"\n  {year}: Found {len(tasks)} tournaments, added {year_tasks_added} to queue"
                if skipped_future > 0:
                    msg += f" ({skipped_future} skipped as too far in future)"
                print(msg)

            except Exception as e:
                print(f"  Error loading {year} tournaments: {e}")
                continue

            if tasks_to_add:
                session.bulk_save_objects(tasks_to_add)
            # Commit after each tour
            session.commit()

    return tasks_added, discovery_metrics


async def process_queue(
    session,
    overwrite: bool = False,
    headless: bool = True,
    worker_id: Optional[int] = None,
) -> dict:
    """
    Process all tasks in the scrape queue.

    Runs until queue is empty or user interrupts (Ctrl+C).

    Args:
        session: Database session
        overwrite: If True, overwrite existing matches instead of skipping duplicates

    Returns:
        Dictionary with processing statistics
    """
    queue_manager = ScrapeQueueManager(session)
    identity_service = PlayerIdentityService(session)
    active_scraper = None
    active_ctx = None
    active_tour_key = None

    stats = {
        "tasks_processed": 0,
        "tasks_completed": 0,
        "tasks_failed": 0,
        "matches_scraped": 0,
        "matches_created": 0,
        "players_created": 0,
        "current_tasks_completed": 0,
        "timings": {
            "scraping": 0.0,
            "ingestion": 0.0,
            "db_commit": 0.0,
            "total": 0.0,
        },
        "task_timings": [],
    }
    if worker_id is not None:
        stats["worker_id"] = worker_id

    def log(message: str) -> None:
        if worker_id is None:
            print(message)
        else:
            print(f"[Worker {worker_id}] {message}")

    log("\n" + "=" * 60)
    log("Processing scrape queue...")
    log("Press Ctrl+C to pause (progress is saved)")
    log("=" * 60)

    # Get initial count of tasks to process
    initial_queue_size = queue_manager.pending_count()
    log(f"Tasks in queue: {initial_queue_size}")

    async def ensure_scraper(tour_key: str):
        nonlocal active_scraper, active_ctx, active_tour_key
        if active_scraper is not None and active_tour_key == tour_key:
            return active_scraper
        if active_ctx is not None:
            await active_ctx.__aexit__(None, None, None)
        scraper_cls = _get_scraper_class(tour_key)
        active_ctx = scraper_cls(headless=headless)
        active_scraper = await active_ctx.__aenter__()
        active_tour_key = tour_key
        return active_scraper

    try:
        while True:
            # Get next task
            task = queue_manager.get_next_task(skip_locked=True)
            if not task:
                log("\nQueue empty - all tasks processed!")
                break

            stats["tasks_processed"] += 1

            # Mark as in progress
            queue_manager.mark_in_progress(task.id)

            # Process the task
            task_params = TaskParams.from_dict(task.task_params)
            tour_key = task_params.tour_key
            task_type = task.task_type

            log(
                f"\n[Task {stats['tasks_processed']}/{initial_queue_size}] "
                f"{task_params.tournament_name or task_params.tournament_id} "
                f"({task_params.year})"
            )
            log(f"  Tour: {TOUR_TYPES.get(tour_key, {}).get('description', tour_key)}")
            log(f"  Task type: {task_type}")

            try:
                scraper = await ensure_scraper(tour_key)

                if task_type == "historical_tournament":
                    # Scrape the tournament
                    matches_result = await execute_task(
                        task_params,
                        scraper=scraper,
                        session=session,
                        identity_service=identity_service,
                        mode="historical",
                        overwrite=overwrite,
                    )

                    stats["matches_scraped"] += matches_result["matches_scraped"]
                    stats["matches_created"] += matches_result["matches_created"]
                    stats["players_created"] += matches_result["players_created"]
                    task_result = matches_result
                elif task_type == "current_tournament":
                    task_result = await execute_task(
                        task_params,
                        scraper=scraper,
                        session=session,
                        identity_service=identity_service,
                        mode="current",
                    )
                    stats["current_tasks_completed"] += 1
                else:
                    raise ValueError(f"Unsupported task type: {task_type}")

                task_timings = task_result.get("timings", {})
                if task_timings:
                    stats["timings"]["scraping"] += task_timings.get("scraping", 0.0)
                    stats["timings"]["ingestion"] += task_timings.get("ingestion", 0.0)
                    stats["timings"]["db_commit"] += task_timings.get("db_commit", 0.0)
                    stats["timings"]["total"] += task_timings.get("total", 0.0)
                    stats["task_timings"].append(
                        {
                            "task_id": task.id,
                            "task_type": task_type,
                            "tour_key": tour_key,
                            "tournament_id": task_params.tournament_id,
                            "year": task_params.year,
                            "timings": task_timings,
                        }
                    )
                    log(
                        "  Timings: "
                        f"scrape={task_timings.get('scraping', 0.0):.2f}s, "
                        f"ingest={task_timings.get('ingestion', 0.0):.2f}s, "
                        f"commit={task_timings.get('db_commit', 0.0):.2f}s, "
                        f"total={task_timings.get('total', 0.0):.2f}s"
                    )

                # Commit the matches, then mark completed
                session.commit()
                queue_manager.mark_completed(task.id)
                stats["tasks_completed"] += 1
                if task_type == "historical_tournament":
                    log(f"  Completed: {matches_result['matches_created']} matches created")
                else:
                    log("  Completed: current tournament updated")

            except Exception as e:
                # Rollback the failed transaction before marking task as failed
                # This is necessary because SQLAlchemy requires rollback after errors
                session.rollback()

                # Mark failed (will retry if attempts remain)
                queue_manager.mark_failed(task.id, str(e))
                stats["tasks_failed"] += 1
                log(f"  Failed: {e}")

            # Commit queue status changes
            session.commit()

            # Show periodic progress
            if stats["tasks_processed"] % 10 == 0:
                show_progress(stats, queue_manager)

    except KeyboardInterrupt:
        log("\n\nPaused by user. Progress saved - run with --resume to continue.")
    finally:
        if active_ctx is not None:
            await active_ctx.__aexit__(None, None, None)

    log(
        "Timing totals: "
        f"scrape={stats['timings']['scraping']:.2f}s, "
        f"ingest={stats['timings']['ingestion']:.2f}s, "
        f"commit={stats['timings']['db_commit']:.2f}s, "
        f"total={stats['timings']['total']:.2f}s"
    )

    return stats


def run_worker(
    worker_id: int,
    overwrite: bool,
    fast: bool,
    headless: bool,
    stats_queue: Optional[multiprocessing.Queue] = None,
) -> None:
    apply_fast_delays(fast)

    with get_session() as session:
        stats = asyncio.run(
            process_queue(
                session,
                overwrite=overwrite,
                headless=headless,
                worker_id=worker_id,
            )
        )

    if stats_queue is not None:
        stats_queue.put(stats)




def show_progress(stats: dict, queue_manager: ScrapeQueueManager) -> None:
    """Show current progress statistics."""
    queue_stats = queue_manager.get_stats()

    print("\n" + "-" * 40)
    print("Progress Update:")
    print(f"  Tasks processed: {stats['tasks_processed']}")
    print(f"  Tasks completed: {stats['tasks_completed']}")
    print(f"  Tasks failed: {stats['tasks_failed']}")
    print(f"  Matches created: {stats['matches_created']}")
    print(f"  Queue remaining: {queue_stats.get('ready_to_process', 0)}")
    print("-" * 40)


def show_queue_stats(session) -> None:
    """Display queue statistics."""
    queue_manager = ScrapeQueueManager(session)
    stats = queue_manager.get_stats()

    print("\n" + "=" * 60)
    print("Scrape Queue Statistics")
    print("=" * 60)

    for key, value in stats.items():
        print(f"  {key}: {value}")

    # Show failed tasks
    failed_tasks = queue_manager.get_failed_tasks(limit=10)
    if failed_tasks:
        print("\n  Recent Failed Tasks:")
        for task in failed_tasks:
            params = task.task_params
            print(f"    - {params.get('tournament_id')} ({params.get('year')}): {task.last_error[:50]}...")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Backfill historical tennis data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill 2024 ATP data
  python scripts/backfill_historical.py --years 2024 --tours ATP

  # Backfill all tours for 2020-2024
  python scripts/backfill_historical.py --years 2020-2024

  # Resume interrupted backfill
  python scripts/backfill_historical.py --resume

  # Show queue statistics
  python scripts/backfill_historical.py --stats
        """,
    )

    parser.add_argument(
        "--years",
        type=str,
        default="2024",
        help="Year or year range to backfill (e.g., '2024' or '2020-2024')",
    )

    parser.add_argument(
        "--tours",
        type=str,
        default=None,
        help=f"Comma-separated list of tours to include. Options: {', '.join(TOUR_TYPES.keys())}",
    )

    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume processing existing queue without adding new tasks",
    )

    parser.add_argument(
        "--process-only",
        action="store_true",
        help="Only process queue, don't add new tasks",
    )

    parser.add_argument(
        "--discover-only",
        action="store_true",
        help="Only discover tournaments and populate the queue (no processing)",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel worker processes to spawn (default: 1)",
    )

    parser.add_argument(
        "--fast",
        "--fast-delays",
        dest="fast",
        action="store_true",
        help="Use faster delays for historical scraping (reduced rate-limit concern)",
    )

    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show queue statistics and exit",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be added to queue without actually adding",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing matches instead of skipping duplicates. "
             "Useful for re-scraping to fix data (e.g., missing dates, temporal_order).",
    )

    parser.add_argument(
        "--clear-queue",
        action="store_true",
        help="Clear all pending/retry/in_progress tasks from the queue before adding new ones. "
             "Use this to remove stale tasks from previous runs.",
    )
    parser.add_argument(
        "--metrics-json",
        type=str,
        default=None,
        help="Write benchmark metrics JSON to the specified path",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Teelo v4.0 - Historical Data Backfill")
    print("=" * 60)

    if args.fast:
        apply_fast_delays(True)
    headless = settings.scrape_headless
    if settings.scrape_virtual_display and not headless:
        print("Starting Virtual Display...")
        VirtualDisplay.ensure_running()

    with get_session() as session:
        queue_manager = ScrapeQueueManager(session)

        # Show stats and exit
        if args.stats:
            show_queue_stats(session)
            return

        metrics_payload = {
            "script": "backfill_historical",
            "started_at": datetime.utcnow().isoformat(),
            "discovery": [],
            "workers": [],
            "aggregate": {},
        }

        # Parse arguments
        years = parse_year_range(args.years)
        tours = parse_tours(args.tours)

        print(f"\nConfiguration:")
        print(f"  Years: {years}")
        print(f"  Tours: {tours}")
        if args.overwrite:
            print(f"  Overwrite: ENABLED (existing matches will be updated)")
        if args.workers > 1:
            print(f"  Workers: {args.workers}")
        if args.fast:
            print(
                f"  Fast delays: ENABLED "
                f"({settings.scrape_delay_min}-{settings.scrape_delay_max}s)"
            )

        # Clear queue if requested
        if args.clear_queue:
            cleared = (
                session.query(ScrapeQueue)
                .filter(ScrapeQueue.status.in_(["pending", "retry", "in_progress"]))
                .delete(synchronize_session="fetch")
            )
            session.commit()
            print(f"\n  Cleared {cleared} tasks from queue")

        if args.discover_only and (args.resume or args.process_only):
            print("Error: --discover-only cannot be combined with --resume or --process-only.")
            return

        # Check for resume mode
        if args.resume or args.process_only:
            pending = queue_manager.pending_count()
            print(f"  Pending tasks: {pending}")

            if pending == 0 and not args.process_only:
                print("\nNo pending tasks. Run without --resume to add new tasks.")
                return
        else:
            # Populate queue with new tasks
            if args.dry_run:
                print("\n[DRY RUN] Would add tasks for:")
                for tour in tours:
                    print(f"  - {TOUR_TYPES[tour]['description']}: {len(years)} years")
                return

            print("\nPopulating queue with tournament tasks...")
            tasks_added, discovery_metrics = await populate_queue(session, queue_manager, years, tours)
            metrics_payload["discovery"].extend(discovery_metrics)
            print(f"\nAdded {tasks_added} tasks to queue")
            session.commit()

            if args.discover_only:
                print("\nDiscovery complete (--discover-only).")
                return

        # Process the queue
        if args.workers > 1:
            ctx = multiprocessing.get_context("spawn")
            stats_queue: multiprocessing.Queue = ctx.Queue()
            processes = []

            for worker_id in range(1, args.workers + 1):
                process = ctx.Process(
                    target=run_worker,
                    args=(worker_id, args.overwrite, args.fast, headless, stats_queue),
                )
                process.start()
                processes.append(process)

            for process in processes:
                process.join()

            aggregated = {
                "tasks_processed": 0,
                "tasks_completed": 0,
                "tasks_failed": 0,
                "matches_scraped": 0,
                "matches_created": 0,
                "players_created": 0,
                "current_tasks_completed": 0,
                "timings": {
                    "scraping": 0.0,
                    "ingestion": 0.0,
                    "db_commit": 0.0,
                    "total": 0.0,
                },
                "task_timings": [],
            }

            stats_received = 0
            while stats_received < len(processes):
                try:
                    worker_stats = stats_queue.get_nowait()
                except Exception:
                    break
                metrics_payload["workers"].append(worker_stats)
                for key in aggregated:
                    if key == "timings":
                        for timing_key, timing_value in worker_stats.get("timings", {}).items():
                            aggregated["timings"][timing_key] += timing_value
                    elif key == "task_timings":
                        aggregated["task_timings"].extend(worker_stats.get("task_timings", []))
                    else:
                        aggregated[key] += worker_stats.get(key, 0)
                stats_received += 1

            stats = aggregated
        else:
            stats = await process_queue(
                session,
                overwrite=args.overwrite,
                headless=headless,
            )
            metrics_payload["workers"].append(stats)

        metrics_payload["aggregate"] = stats

        # Final summary
        print("\n" + "=" * 60)
        print("Backfill Complete")
        print("=" * 60)
        print(f"  Tasks processed: {stats['tasks_processed']}")
        print(f"  Tasks completed: {stats['tasks_completed']}")
        print(f"  Tasks failed: {stats['tasks_failed']}")
        print(f"  Matches scraped: {stats['matches_scraped']}")
        print(f"  Matches created: {stats['matches_created']}")
        if stats.get("current_tasks_completed"):
            print(f"  Current tournaments updated: {stats['current_tasks_completed']}")
        print(
            "  Timing totals: "
            f"scrape={stats['timings']['scraping']:.2f}s, "
            f"ingest={stats['timings']['ingestion']:.2f}s, "
            f"commit={stats['timings']['db_commit']:.2f}s, "
            f"total={stats['timings']['total']:.2f}s"
        )

        # Show remaining queue
        remaining = queue_manager.pending_count()
        if remaining > 0:
            print(f"\n  Remaining in queue: {remaining}")
            print("  Run with --resume to continue")

        if args.metrics_json:
            metrics_path = Path(args.metrics_json)
            metrics_path.parent.mkdir(parents=True, exist_ok=True)
            metrics_path.write_text(json.dumps(metrics_payload, indent=2))
            print(f"\nMetrics written to {metrics_path}")


if __name__ == "__main__":
    asyncio.run(main())
