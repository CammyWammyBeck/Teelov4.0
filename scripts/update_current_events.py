#!/usr/bin/env python3
from __future__ import annotations

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
import contextlib
import json
import multiprocessing
import os
from queue import Empty
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.config import settings
from teelo.db import get_session
from teelo.db.models import ScrapeQueue
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


def apply_fast_scrape_profile(enabled: bool) -> None:
    if not enabled:
        return
    settings.scrape_delay_min = 0.1
    settings.scrape_delay_max = 0.4
    settings.scrape_timeout = min(settings.scrape_timeout, 10000)
    settings.scrape_max_retries = min(settings.scrape_max_retries, 2)


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


def _queue_event(
    event_queue: multiprocessing.Queue | None,
    payload: dict,
) -> None:
    if event_queue is None:
        return
    message = dict(payload)
    message["timestamp"] = datetime.now(timezone.utc).isoformat()
    event_queue.put(message)


def _status_line(event: dict) -> str:
    worker_id = event.get("worker_id", "?")
    state = event.get("state", "idle")
    tournament_name = event.get("tournament_name")
    tournament_id = event.get("tournament_id")
    phase = event.get("phase")
    error = event.get("error")
    tour_key = event.get("tour_key")

    if state == "idle":
        return f"Worker {worker_id}: Idle - waiting for task"
    if state == "done":
        if tournament_name:
            return f"Worker {worker_id}: Completed {tournament_name} ({tournament_id})"
        return f"Worker {worker_id}: Completed task"
    if state == "failed":
        if tournament_name:
            return f"Worker {worker_id}: Failed {tournament_name} ({tournament_id}) - {error}"
        return f"Worker {worker_id}: Failed - {error}"

    if tournament_name:
        tour_label = TOUR_TYPES.get(tour_key, {}).get("description", tour_key)
        return (
            f"Worker {worker_id}: Processing {tour_label} "
            f"{tournament_name} ({tournament_id}) - {phase or 'Processing'}"
        )
    return f"Worker {worker_id}: {phase or 'Processing'}"


class LiveWorkerDashboard:
    def __init__(self, worker_ids: list[int], enabled: bool):
        self.worker_ids = worker_ids
        self.enabled = enabled and sys.stdout.isatty()
        self._initialized = False
        self._status_by_worker = {
            worker_id: f"Worker {worker_id}: Starting..."
            for worker_id in worker_ids
        }
        self._last_rendered_by_worker = dict(self._status_by_worker)
        self._summary_line = "Run: initializing..."

    def _fit_line(self, line: str) -> str:
        width = max(40, shutil.get_terminal_size(fallback=(120, 24)).columns - 1)
        if len(line) <= width:
            return line
        return line[: max(0, width - 3)] + "..."

    def update(self, event: dict) -> None:
        worker_id = event.get("worker_id")
        if worker_id not in self._status_by_worker:
            return
        next_line = self._fit_line(_status_line(event))
        if self._status_by_worker.get(worker_id) == next_line:
            return
        self._status_by_worker[worker_id] = next_line
        if self.enabled:
            self.render()
        else:
            print(next_line)

    def render(self) -> None:
        lines = [self._fit_line(self._status_by_worker[worker_id]) for worker_id in self.worker_ids]
        lines.append(self._fit_line(self._summary_line))
        if not self._initialized:
            for line in lines:
                print(line)
            self._last_rendered_by_worker = {
                worker_id: line for worker_id, line in zip(self.worker_ids, lines[:-1])
            }
            self._initialized = True
            return

        if all(
            self._last_rendered_by_worker.get(worker_id) == line
            for worker_id, line in zip(self.worker_ids, lines[:-1])
        ):
            return

        # Move cursor back up and repaint all worker lines in place.
        sys.stdout.write(f"\x1b[{len(lines)}A")
        for line in lines:
            sys.stdout.write("\x1b[2K\r")
            sys.stdout.write(line + "\n")
        sys.stdout.flush()
        self._last_rendered_by_worker = {
            worker_id: line for worker_id, line in zip(self.worker_ids, lines[:-1])
        }

    def finish(self) -> None:
        if self.enabled and self._initialized:
            print("")

    def set_summary(self, line: str) -> None:
        next_line = self._fit_line(line)
        if self._summary_line == next_line:
            return
        self._summary_line = next_line
        if self.enabled:
            self.render()
        else:
            print(next_line)


async def process_queue(
    session,
    headless: bool,
    fast_mode: bool = True,
    worker_id: int | None = None,
    event_queue: multiprocessing.Queue | None = None,
    show_logs: bool = True,
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
        if not show_logs:
            return
        if worker_id is None:
            print(message)
        else:
            print(f"[Worker {worker_id}] {message}")

    def emit_status(
        state: str,
        *,
        phase: str | None = None,
        task_params: TaskParams | None = None,
        task_type: str | None = None,
        tour_key: str | None = None,
        error: str | None = None,
    ) -> None:
        if worker_id is None:
            return
        _queue_event(
            event_queue,
            {
                "event": "worker_status",
                "worker_id": worker_id,
                "state": state,
                "phase": phase,
                "task_type": task_type,
                "tour_key": tour_key,
                "tournament_name": task_params.tournament_name if task_params else None,
                "tournament_id": task_params.tournament_id if task_params else None,
                "error": error,
            },
        )

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
    emit_status("idle")

    try:
        while True:
            task = queue_manager.get_next_task(skip_locked=True)
            if not task:
                log("\nQueue empty - all tasks processed!")
                emit_status("idle")
                break

            stats["tasks_processed"] += 1
            queue_manager.mark_in_progress(task.id)
            _queue_event(
                event_queue,
                {
                    "event": "task_started",
                    "worker_id": worker_id,
                    "task_id": task.id,
                },
            )

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
                emit_status(
                    "running",
                    phase="Preparing Task",
                    task_params=task_params,
                    task_type=task_type,
                    tour_key=tour_key,
                )

                def on_phase(phase_message: str) -> None:
                    emit_status(
                        "running",
                        phase=phase_message,
                        task_params=task_params,
                        task_type=task_type,
                        tour_key=tour_key,
                    )

                if task_type == "current_tournament":
                    result = await execute_task(
                        task_params,
                        scraper=scraper,
                        session=session,
                        identity_service=identity_service,
                        mode="current",
                        fast_mode=fast_mode,
                        progress_callback=on_phase,
                        verbose=show_logs,
                    )
                    stats["current_tasks_completed"] += 1
                elif task_type == "historical_tournament":
                    result = await execute_task(
                        task_params,
                        scraper=scraper,
                        session=session,
                        identity_service=identity_service,
                        mode="historical",
                        verbose=show_logs,
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
                emit_status(
                    "done",
                    phase="Completed",
                    task_params=task_params,
                    task_type=task_type,
                    tour_key=tour_key,
                )
                _queue_event(
                    event_queue,
                    {
                        "event": "task_finished",
                        "worker_id": worker_id,
                        "task_id": task.id,
                        "outcome": "completed",
                    },
                )

            except Exception as e:
                session.rollback()
                queue_manager.mark_failed(task.id, str(e))
                stats["tasks_failed"] += 1
                log(f"  Failed: {e}")
                emit_status(
                    "failed",
                    phase="Failed",
                    task_params=task_params,
                    task_type=task_type,
                    tour_key=tour_key,
                    error=str(e),
                )
                _queue_event(
                    event_queue,
                    {
                        "event": "task_finished",
                        "worker_id": worker_id,
                        "task_id": task.id,
                        "outcome": "failed",
                    },
                )

            session.commit()

    except KeyboardInterrupt:
        log("\n\nPaused by user. Progress saved - run with --process-only to continue.")
        emit_status("idle", phase="Paused")
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
    fast_mode: bool = True,
    event_queue: multiprocessing.Queue | None = None,
    quiet_worker_logs: bool = True,
) -> None:
    with get_session() as session:
        if quiet_worker_logs:
            with open(os.devnull, "w", encoding="utf-8") as devnull:
                with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
                    stats = asyncio.run(
                        process_queue(
                            session,
                            headless=headless,
                            fast_mode=fast_mode,
                            worker_id=worker_id,
                            event_queue=event_queue,
                            show_logs=False,
                        )
                    )
        else:
            stats = asyncio.run(
                process_queue(
                    session,
                    headless=headless,
                    fast_mode=fast_mode,
                    worker_id=worker_id,
                    event_queue=event_queue,
                    show_logs=True,
                )
            )
    _queue_event(
        event_queue,
        {
            "event": "worker_stats",
            "worker_id": worker_id,
            "stats": stats,
        },
    )


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
    parser.add_argument(
        "--fast",
        dest="fast",
        action="store_true",
        default=True,
        help="Enable fast profile for hourly retries (default: enabled).",
    )
    parser.add_argument(
        "--no-fast",
        dest="fast",
        action="store_false",
        help="Disable fast profile.",
    )
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
    parser.add_argument(
        "--live-status",
        dest="live_status",
        action="store_true",
        default=True,
        help="Render per-worker live status rows in the terminal (default: enabled).",
    )
    parser.add_argument(
        "--no-live-status",
        dest="live_status",
        action="store_false",
        help="Disable live status rows.",
    )
    parser.add_argument(
        "--quiet-worker-logs",
        dest="quiet_worker_logs",
        action="store_true",
        default=True,
        help="Suppress worker stdout logs and rely on parent live status rows (default: enabled).",
    )
    parser.add_argument(
        "--no-quiet-worker-logs",
        dest="quiet_worker_logs",
        action="store_false",
        help="Allow worker logs to print directly.",
    )
    parser.add_argument(
        "--status-jsonl",
        type=str,
        default=None,
        help="Write worker status events as JSONL (for external dashboards/web UI).",
    )
    parser.add_argument(
        "--clear-queue",
        action="store_true",
        help="Clear pending/retry/in_progress queue tasks before starting.",
    )
    args = parser.parse_args()

    if args.discover_only and args.process_only:
        raise SystemExit("Error: --discover-only cannot be combined with --process-only.")

    tours = [t.strip().upper() for t in args.tours.split(",")]
    apply_fast_scrape_profile(args.fast)

    # Validate tours
    tours = [t for t in tours if t in TOUR_TYPES]

    print("=" * 60)
    print("UPDATE CURRENT EVENTS")
    print(f"Tours: {tours}")
    headless = False if args.headed else settings.scrape_headless
    print(
        "Settings: "
        f"headless={headless}, "
        f"fast={args.fast}, "
        f"virtual_display={settings.scrape_virtual_display}, "
        f"timeout_ms={settings.scrape_timeout}, "
        f"delays={settings.scrape_delay_min}-{settings.scrape_delay_max}s"
    )
    print("=" * 60)

    if args.clear_queue:
        with get_session() as session:
            cleared = (
                session.query(ScrapeQueue)
                .filter(ScrapeQueue.status.in_(["pending", "retry", "in_progress"]))
                .delete(synchronize_session="fetch")
            )
            session.commit()
        print(f"Cleared {cleared} queue tasks (pending/retry/in_progress).")

    # Explicitly ensure virtual display is running if configured
    if settings.scrape_virtual_display and not headless:
        print("Starting Virtual Display...")
        VirtualDisplay.ensure_running()

    semaphore = asyncio.Semaphore(max(1, args.max_parallel_tours))
    today = date.today()

    metrics_payload = {
        "script": "update_current_events",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "discovery": [],
        "workers": [],
        "aggregate": {},
        "status_jsonl": args.status_jsonl,
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
        event_queue: multiprocessing.Queue = ctx.Queue()
        processes = []
        worker_ids = list(range(1, args.workers + 1))
        dashboard = LiveWorkerDashboard(worker_ids, enabled=args.live_status)
        worker_stats: dict[int, dict] = {}
        with get_session() as session:
            initial_pending_count = ScrapeQueueManager(session).pending_count()
        tasks_started = 0
        tasks_completed_live = 0
        tasks_failed_live = 0
        run_started_at = perf_counter()
        status_jsonl_path = Path(args.status_jsonl) if args.status_jsonl else None
        status_jsonl_file = None
        if status_jsonl_path is not None:
            status_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            status_jsonl_file = status_jsonl_path.open("a", encoding="utf-8")

        for worker_id in worker_ids:
            process = ctx.Process(
                target=run_worker,
                args=(worker_id, headless, args.fast, event_queue, args.quiet_worker_logs),
            )
            process.start()
            processes.append(process)

        def refresh_summary_line() -> None:
            elapsed = perf_counter() - run_started_at
            processed = tasks_completed_live + tasks_failed_live
            remaining = max(initial_pending_count - processed, 0)
            in_progress = max(tasks_started - processed, 0)
            dashboard.set_summary(
                "Run: "
                f"queue_remaining={remaining} "
                f"in_progress={in_progress} "
                f"processed={processed} "
                f"completed={tasks_completed_live} "
                f"failed={tasks_failed_live} "
                f"elapsed={elapsed:.1f}s"
            )

        def handle_event(event: dict) -> None:
            nonlocal tasks_started, tasks_completed_live, tasks_failed_live
            if status_jsonl_file is not None:
                status_jsonl_file.write(json.dumps(event) + "\n")
                status_jsonl_file.flush()
            if event.get("event") == "worker_status":
                dashboard.update(event)
            elif event.get("event") == "worker_stats":
                worker_id = event.get("worker_id")
                stats_payload = event.get("stats")
                if isinstance(worker_id, int) and isinstance(stats_payload, dict):
                    worker_stats[worker_id] = stats_payload
            elif event.get("event") == "task_started":
                tasks_started += 1
            elif event.get("event") == "task_finished":
                if event.get("outcome") == "completed":
                    tasks_completed_live += 1
                elif event.get("outcome") == "failed":
                    tasks_failed_live += 1
            refresh_summary_line()

        refresh_summary_line()

        while any(process.is_alive() for process in processes):
            try:
                handle_event(event_queue.get(timeout=0.2))
            except Empty:
                continue

        for process in processes:
            process.join()

        while True:
            try:
                handle_event(event_queue.get_nowait())
            except Empty:
                break

        if status_jsonl_file is not None:
            status_jsonl_file.close()
        dashboard.finish()

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

        for worker_id in worker_ids:
            stats_payload = worker_stats.get(worker_id)
            if not stats_payload:
                continue
            metrics_payload["workers"].append(stats_payload)
            for key in aggregated:
                if key == "timings":
                    for timing_key, timing_value in stats_payload.get("timings", {}).items():
                        aggregated["timings"][timing_key] += timing_value
                elif key == "task_timings":
                    aggregated["task_timings"].extend(stats_payload.get("task_timings", []))
                else:
                    aggregated[key] += stats_payload.get(key, 0)

        stats = aggregated
    else:
        with get_session() as session:
            stats = await process_queue(session, headless=headless, fast_mode=args.fast)
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
