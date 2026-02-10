#!/usr/bin/env python3
"""
Update Current Events Script.

Discovers currently running tournaments (within ±1 week) across all tours
(ATP, WTA, ITF), enqueues them in the scrape queue, and processes tasks
via worker loops that reuse a single scraper per worker.

Usage:
    python scripts/update_current_events.py
    python scripts/update_current_events.py --tours ATP,WTA
    python scripts/update_current_events.py --discover-only
    python scripts/update_current_events.py --process-only
"""

import argparse
import asyncio
import json
import multiprocessing
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from time import perf_counter

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.config import settings
from teelo.db import get_session
from teelo.players.identity import PlayerIdentityService
from teelo.scrape.atp import ATPScraper
from teelo.scrape.base import VirtualDisplay
from teelo.scrape.discovery import discover_tournament_tasks
from teelo.scrape.itf import ITFScraper
from teelo.scrape.pipeline import TaskParams, execute_task
from teelo.scrape.queue import ScrapeQueueManager
from teelo.scrape.utils import TOUR_TYPES
from teelo.scrape.wta import WTAScraper


def _get_scraper_class(tour_key: str):
    scraper_type = TOUR_TYPES[tour_key]["scraper"]
    if scraper_type == "atp":
        return ATPScraper
    if scraper_type == "wta":
        return WTAScraper
    if scraper_type == "itf":
        return ITFScraper
    raise ValueError(f"Unknown scraper type for {tour_key}")


async def discover_tour_tasks(
    tour_key: str,
    year: int,
    today: date,
    headless: bool,
    semaphore: asyncio.Semaphore,
) -> tuple[list, float]:
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
            discovery_start = perf_counter()
            tasks = await discover_tournament_tasks(
                tour_key,
                year,
                task_type="current_tournament",
                scraper=scraper,
                window=(window_start, window_end),
            )
            discovery_elapsed = perf_counter() - discovery_start
            print(f"[{tour_key}] Found {len(tasks)} current tournaments in {discovery_elapsed:.2f}s.")
            return tasks, discovery_elapsed


def enqueue_current_tasks(
    session,
    queue_manager: ScrapeQueueManager,
    tasks: list,
) -> int:
    queue_payload = []
    for task in tasks:
        queue_payload.append(
            {
                "task_type": "current_tournament",
                "params": task.params.to_dict(),
                "priority": ScrapeQueueManager.PRIORITY_HIGH,
            }
        )
    if not queue_payload:
        return 0
    queue_manager.enqueue_batch(queue_payload)
    session.commit()
    return len(queue_payload)


async def process_queue(
    session,
    headless: bool,
    worker_id: int | None = None,
) -> dict:
    queue_manager = ScrapeQueueManager(session)
    identity_service = PlayerIdentityService(session)
    active_scraper = None
    active_ctx = None
    active_tour_key = None

    stats = {
        "tasks_processed": 0,
        "tasks_completed": 0,
        "tasks_failed": 0,
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

    log("\n" + "=" * 60)
    log("Processing scrape queue...")
    log("Press Ctrl+C to pause (progress is saved)")
    log("=" * 60)

    try:
        while True:
            task = queue_manager.get_next_task(skip_locked=True)
            if not task:
                log("\nQueue empty - all tasks processed!")
                break

            stats["tasks_processed"] += 1
            queue_manager.mark_in_progress(task.id)

            task_params = TaskParams.from_dict(task.task_params)
            tour_key = task_params.tour_key
            task_type = task.task_type

            log(
                f"\n[Task {stats['tasks_processed']}] "
                f"{task_params.tournament_name or task_params.tournament_id} "
                f"({task_params.year})"
            )
            log(f"  Tour: {TOUR_TYPES.get(tour_key, {}).get('description', tour_key)}")
            log(f"  Task type: {task_type}")

            try:
                scraper = await ensure_scraper(tour_key)

                if task_type == "current_tournament":
                    result = await execute_task(
                        task_params,
                        scraper=scraper,
                        session=session,
                        identity_service=identity_service,
                        mode="current",
                    )
                    stats["current_tasks_completed"] += 1
                elif task_type == "historical_tournament":
                    result = await execute_task(
                        task_params,
                        scraper=scraper,
                        session=session,
                        identity_service=identity_service,
                        mode="historical",
                    )
                else:
                    raise ValueError(f"Unsupported task type: {task_type}")

                task_timings = result.get("timings", {})
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

                session.commit()
                queue_manager.mark_completed(task.id)
                stats["tasks_completed"] += 1
                log("  Completed")

            except Exception as e:
                session.rollback()
                queue_manager.mark_failed(task.id, str(e))
                stats["tasks_failed"] += 1
                log(f"  Failed: {e}")

            session.commit()

    except KeyboardInterrupt:
        log("\n\nPaused by user. Progress saved - run with --process-only to continue.")
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
    headless: bool,
    stats_queue: multiprocessing.Queue | None = None,
) -> None:
    with get_session() as session:
        stats = asyncio.run(
            process_queue(
                session,
                headless=headless,
                worker_id=worker_id,
            )
        )
    if stats_queue is not None:
        stats_queue.put(stats)


async def main():
    parser = argparse.ArgumentParser(description="Update Current Events")
    parser.add_argument(
        "--tours",
        default="ATP,WTA,CHALLENGER,WTA_125,ITF_MEN,ITF_WOMEN",
        help="Comma-separated tours",
    )
    parser.add_argument("--year", type=int, default=date.today().year, help="Season year to scan")
    parser.add_argument("--max-parallel-tours", type=int, default=3, help="Max tour workers to run concurrently")
    parser.add_argument("--headed", action="store_true", help="Force headed browser mode (slower)")
    parser.add_argument("--discover-only", action="store_true", help="Discover current tournaments only")
    parser.add_argument("--process-only", action="store_true", help="Process from queue only (skip discovery)")
    parser.add_argument(
        "--metrics-json",
        type=str,
        default=None,
        help="Write benchmark metrics JSON to the specified path",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel worker processes to spawn (default: 1)",
    )
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

    metrics_payload = {
        "script": "update_current_events",
        "started_at": datetime.utcnow().isoformat(),
        "discovery": [],
        "workers": [],
        "aggregate": {},
    }

    if not args.process_only:
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
        all_tasks = []
        for tour_key, result in zip(tours, discovered):
            if isinstance(result, Exception):
                print(f"[{tour_key}] Discovery failed: {result}")
                continue
            tasks, discovery_elapsed = result
            metrics_payload["discovery"].append(
                {
                    "tour_key": tour_key,
                    "duration_s": discovery_elapsed,
                    "tasks_found": len(tasks),
                }
            )
            all_tasks.extend(tasks)

        with get_session() as session:
            queue_manager = ScrapeQueueManager(session)
            tasks_added = enqueue_current_tasks(session, queue_manager, all_tasks)

        print(f"\nAdded {tasks_added} current tasks to the queue")

        if args.discover_only:
            print("\nDiscovery complete (--discover-only).")
            return

    if args.workers > 1:
        ctx = multiprocessing.get_context("spawn")
        stats_queue: multiprocessing.Queue = ctx.Queue()
        processes = []

        for worker_id in range(1, args.workers + 1):
            process = ctx.Process(
                target=run_worker,
                args=(worker_id, headless, stats_queue),
            )
            process.start()
            processes.append(process)

        for process in processes:
            process.join()

        aggregated = {
            "tasks_processed": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
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
        with get_session() as session:
            stats = await process_queue(session, headless=headless)
        metrics_payload["workers"].append(stats)

    metrics_payload["aggregate"] = stats

    print("\n" + "=" * 60)
    print("Current Events Update Complete")
    print("=" * 60)
    print(f"  Tasks processed: {stats['tasks_processed']}")
    print(f"  Tasks completed: {stats['tasks_completed']}")
    print(f"  Tasks failed: {stats['tasks_failed']}")
    if stats.get("current_tasks_completed"):
        print(f"  Current tournaments updated: {stats['current_tasks_completed']}")
    print(
        "  Timing totals: "
        f"scrape={stats['timings']['scraping']:.2f}s, "
        f"ingest={stats['timings']['ingestion']:.2f}s, "
        f"commit={stats['timings']['db_commit']:.2f}s, "
        f"total={stats['timings']['total']:.2f}s"
    )

    if args.metrics_json:
        metrics_path = Path(args.metrics_json)
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(json.dumps(metrics_payload, indent=2))
        print(f"\nMetrics written to {metrics_path}")


if __name__ == "__main__":
    asyncio.run(main())
