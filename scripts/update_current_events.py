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

Fast mode is always enabled.
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
from typing import IO

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from teelo.config import settings
from teelo.db import get_session
from teelo.db.models import Match, ScrapeQueue, Tournament, TournamentEdition
from teelo.players.identity import PlayerIdentityService
from teelo.scrape.atp import ATPScraper
from teelo.scrape.base import VirtualDisplay
from teelo.scrape.discovery import discover_tournament_tasks
from teelo.scrape.itf import ITFScraper
from teelo.scrape.pipeline import TaskParams, execute_task
from teelo.scrape.queue import ScrapeQueueManager
from teelo.scrape.utils import TOUR_TYPES
from teelo.scrape.wta import WTAScraper


def parse_year_range(year_str: str) -> list[int]:
    """Parse '2024' or '2020-2024' into a list of years, newest first."""
    if "-" in year_str:
        start, end = year_str.split("-", 1)
        return sorted(range(int(start), int(end) + 1), reverse=True)
    return [int(year_str)]


class _Tee:
    """Write to both the real stdout and a log file, prefixing each line with [HH:MM:SS]."""

    def __init__(self, real_stdout: IO[str], log_file: IO[str]) -> None:
        self._real = real_stdout
        self._log = log_file

    def write(self, data: str) -> int:
        self._real.write(data)
        if data:
            ts = datetime.now().strftime("%H:%M:%S")
            for line in data.splitlines(keepends=True):
                self._log.write(f"[{ts}] {line}")
            self._log.flush()
        return len(data)

    def flush(self) -> None:
        self._real.flush()
        self._log.flush()

    def isatty(self) -> bool:
        return self._real.isatty()

    def __getattr__(self, name: str):
        return getattr(self._real, name)


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
    lookback_days: int = 7,
) -> tuple[list, float]:
    """Discover current tournaments for one tour."""
    scraper_cls = _get_scraper_class(tour_key)
    window_start = today - timedelta(days=lookback_days)
    # Current-events discovery should stay narrow. End-of-year discovery creates
    # hundreds of future tasks, bloats scrape_queue dedupe work, and can stall
    # the hourly pipeline before it ever reaches actual match ingestion.
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


# Statuses that mean a Final match has a known result and the tournament is done.
_TERMINAL_STATUSES = frozenset(["completed", "retired", "walkover", "default"])


def _get_completed_edition_keys(session, tasks: list) -> set[tuple[str, int, str]]:
    """
    Return the set of (tournament_code, year, tour) tuples for which a
    completed Final already exists in the database.

    Single batch query — one DB round-trip regardless of how many tasks
    were discovered.
    """
    if not tasks:
        return set()

    tournament_codes = list({task.params.tournament_id for task in tasks})

    rows = (
        session.query(Tournament.tournament_code, TournamentEdition.year, Tournament.tour)
        .join(TournamentEdition, TournamentEdition.tournament_id == Tournament.id)
        .join(Match, Match.tournament_edition_id == TournamentEdition.id)
        .filter(
            Tournament.tournament_code.in_(tournament_codes),
            Match.round == "F",
            Match.status.in_(list(_TERMINAL_STATUSES)),
            Match.winner_id.isnot(None),
        )
        .distinct()
        .all()
    )

    return {(row.tournament_code, row.year, row.tour) for row in rows}


def enqueue_current_tasks(
    session,
    queue_manager: ScrapeQueueManager,
    tasks: list,
    force: bool = False,
) -> int:
    """
    Enqueue discovered tournament tasks, skipping any edition whose
    Final is already recorded in the database as completed.
    This prevents re-scraping fully finished tournaments on every run.

    When ``force`` is True the completed-edition check is skipped and all
    discovered tasks are enqueued regardless of their current DB state.
    """
    # Mapping from task tour_key values to DB Tournament.tour values.
    _TOUR_KEY_TO_DB_TOUR = {
        "ATP": "ATP",
        "CHALLENGER": "Challenger",
        "ITF_MEN": "ITF",
        "ITF_WOMEN": "ITF",
        "WTA": "WTA",
        "WTA_125": "WTA 125",
    }

    # One batch query to find all already-finished editions upfront.
    completed_editions = set() if force else _get_completed_edition_keys(session, tasks)

    skipped = 0
    queue_payload = []
    for task in tasks:
        db_tour = _TOUR_KEY_TO_DB_TOUR.get(task.params.tour_key, task.params.tour_key)
        edition_key = (task.params.tournament_id, task.params.year, db_tour)
        if edition_key in completed_editions:
            display_name = task.params.tournament_name or task.params.tournament_id
            print(f"  Skipping {display_name} ({task.params.year}) — final already completed.")
            skipped += 1
            continue
        queue_payload.append(
            {
                "task_type": "current_tournament",
                "params": task.params.to_dict(),
                "priority": ScrapeQueueManager.PRIORITY_HIGH,
            }
        )

    if skipped:
        print(f"  Skipped {skipped} already-completed tournament(s).")

    if not queue_payload:
        return 0

    queue_manager.enqueue_batch(queue_payload)
    session.commit()
    return len(queue_payload)


async def populate_historical_queue(
    session,
    queue_manager: ScrapeQueueManager,
    years: list[int],
    tours: list[str],
    overwrite: bool = False,
) -> tuple[int, list[dict]]:
    """Discover and enqueue historical tournaments for past years."""
    from datetime import datetime, timedelta
    from sqlalchemy import cast, Integer
    from teelo.db.models import ScrapeQueue as _SQ

    tasks_added = 0
    discovery_metrics: list[dict] = []
    today = date.today()
    future_cutoff = today + timedelta(days=7)

    for tour_key in tours:
        tour_config = TOUR_TYPES[tour_key]
        print(f"\n{'=' * 60}")
        print(f"Historical discovery: {tour_config['description']}")
        print("=" * 60)

        for year in years:
            base_priority = 7 + min(max(date.today().year - year, 0), 2)
            tasks_to_add: list = []
            skipped_future = 0

            try:
                existing_ids = set(
                    tid for (tid,) in (
                        session.query(_SQ.task_params["tournament_id"].astext)
                        .filter(
                            _SQ.task_type == "historical_tournament",
                            _SQ.status.in_(["pending", "in_progress", "retry"]),
                            _SQ.task_params["tour_key"].astext == tour_key,
                            cast(_SQ.task_params["year"].astext, Integer) == year,
                        )
                        .all()
                    )
                    if tid
                )

                discovery_start = perf_counter()
                tasks = await discover_tournament_tasks(
                    tour_key, year, task_type="historical_tournament"
                )
                discovery_elapsed = perf_counter() - discovery_start
                discovery_metrics.append({
                    "tour_key": tour_key, "year": year,
                    "duration_s": discovery_elapsed, "tasks_found": len(tasks),
                })

                for task in tasks:
                    start_date_str = task.params.start_date
                    if start_date_str:
                        try:
                            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                            if start_date > future_cutoff:
                                skipped_future += 1
                                continue
                        except (ValueError, TypeError):
                            pass
                    if task.params.tournament_id in existing_ids:
                        continue
                    tasks_to_add.append(
                        _SQ(
                            task_type="historical_tournament",
                            task_params=task.params.to_dict(),
                            priority=base_priority,
                            max_attempts=3,
                            status="pending",
                        )
                    )
                    tasks_added += 1

                msg = f"  {year}: Found {len(tasks)} tournaments, added {len(tasks_to_add)} to queue"
                if skipped_future:
                    msg += f" ({skipped_future} skipped as future)"
                print(msg)

            except Exception as e:
                print(f"  Error loading {year} tournaments: {e}")
                continue

            if tasks_to_add:
                session.bulk_save_objects(tasks_to_add)
            session.commit()

    return tasks_added, discovery_metrics


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
    force: bool = False,
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
                        force=force,
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
                    # Top-level summary
                    log(
                        "  Timings: "
                        f"total={task_timings.get('total', 0.0):.2f}s  "
                        f"(scrape={task_timings.get('scraping', 0.0):.2f}s  "
                        f"ingest={task_timings.get('ingestion', 0.0):.2f}s  "
                        f"commit={task_timings.get('db_commit', 0.0):.2f}s"
                        + (f"  elo={task_timings.get('elo_update', 0.0):.2f}s" if task_timings.get("elo_update") else "")
                        + ")"
                    )
                    # Per-phase breakdown (draw / schedule / results)
                    phases = task_timings.get("phases", {})
                    for phase_name, phase_t in phases.items():
                        if not isinstance(phase_t, dict):
                            continue
                        s = phase_t.get("scrape", 0.0)
                        i = phase_t.get("ingest", 0.0)
                        if s or i:
                            log(f"    {phase_name:10}: scrape={s:.2f}s  ingest={i:.2f}s")

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
        "\nTiming totals: "
        f"total={stats['timings']['total']:.2f}s  "
        f"(scrape={stats['timings']['scraping']:.2f}s  "
        f"ingest={stats['timings']['ingestion']:.2f}s  "
        f"commit={stats['timings']['db_commit']:.2f}s)"
    )

    # Aggregate per-phase totals across all tasks
    phase_totals: dict[str, dict[str, float]] = {}
    for t in stats.get("task_timings", []):
        phases = t.get("timings", {}).get("phases", {})
        for phase_name, phase_t in phases.items():
            if not isinstance(phase_t, dict):
                continue
            if phase_name not in phase_totals:
                phase_totals[phase_name] = {"scrape": 0.0, "ingest": 0.0}
            phase_totals[phase_name]["scrape"] += phase_t.get("scrape", 0.0)
            phase_totals[phase_name]["ingest"] += phase_t.get("ingest", 0.0)
    if phase_totals:
        log("Phase breakdown (cumulative across all tasks):")
        for phase_name, phase_t in phase_totals.items():
            log(f"  {phase_name:10}: scrape={phase_t['scrape']:.2f}s  ingest={phase_t['ingest']:.2f}s")

    return stats


def run_worker(
    worker_id: int,
    headless: bool,
    fast_mode: bool = True,
    event_queue: multiprocessing.Queue | None = None,
    quiet_worker_logs: bool = True,
    log_file_path: str | None = None,
    force: bool = False,
) -> None:
    with get_session() as session:
        if quiet_worker_logs:
            if log_file_path is not None:
                # Suppress console but capture to log file with timestamp prefix.
                log_f = open(log_file_path, "a", encoding="utf-8")
                with open(os.devnull, "w", encoding="utf-8") as devnull:
                    tee = _Tee(devnull, log_f)
                    with contextlib.redirect_stdout(tee), contextlib.redirect_stderr(devnull):
                        stats = asyncio.run(
                            process_queue(
                                session,
                                headless=headless,
                                fast_mode=fast_mode,
                                force=force,
                                worker_id=worker_id,
                                event_queue=event_queue,
                                show_logs=True,
                            )
                        )
                log_f.close()
            else:
                with open(os.devnull, "w", encoding="utf-8") as devnull:
                    with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
                        stats = asyncio.run(
                            process_queue(
                                session,
                                headless=headless,
                                fast_mode=fast_mode,
                                force=force,
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
                    force=force,
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
    parser.add_argument(
        "--years",
        type=str,
        default=str(date.today().year),
        help="Year or year range to scrape (e.g. '2025' or '2023-2025'). "
             "Current year uses windowed current-event discovery; past years use full historical discovery.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing matches instead of skipping duplicates (historical mode only).",
    )
    parser.add_argument("--max-parallel-tours", type=int, default=3, help="Max tour workers to run concurrently")
    parser.add_argument("--headed", action="store_true", help="Force headed browser mode (slower)")
    parser.add_argument("--discover-only", action="store_true", help="Discover current tournaments only")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="How many days back to look for tournaments (default: 7). Increase to reprocess recently completed tournaments.",
    )
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
    parser.add_argument(
        "--max-per-tour",
        type=int,
        default=0,
        help="Limit tasks enqueued per tour (0 = unlimited). Useful for quick timing tests.",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Tee all stdout output to this file (append mode, timestamped lines).",
    )
    args = parser.parse_args()

    log_file_handle = None
    original_stdout = sys.stdout
    if args.log_file:
        log_path = Path(args.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file_handle = log_path.open("a", encoding="utf-8")
        sys.stdout = _Tee(original_stdout, log_file_handle)

    try:
        await _main_impl(args, original_stdout)
    finally:
        sys.stdout = original_stdout
        if log_file_handle is not None:
            log_file_handle.close()


async def _main_impl(args, original_stdout) -> None:
    if args.discover_only and args.process_only:
        raise SystemExit("Error: --discover-only cannot be combined with --process-only.")

    tours = [t.strip().upper() for t in args.tours.split(",")]
    apply_fast_scrape_profile(True)
    years = parse_year_range(args.years)
    current_year = date.today().year
    current_years = [year for year in years if year == current_year]
    past_years = [year for year in years if year < current_year]
    future_years = [year for year in years if year > current_year]

    # Validate tours
    tours = [t for t in tours if t in TOUR_TYPES]

    print("=" * 60)
    print("UPDATE CURRENT EVENTS")
    print(f"Tours: {tours}")
    print(f"Years: {years}")
    headless = False if args.headed else settings.scrape_headless
    print(
        "Settings: "
        f"headless={headless}, "
        f"virtual_display={settings.scrape_virtual_display}, "
        f"timeout_ms={settings.scrape_timeout}, "
        f"delays={settings.scrape_delay_min}-{settings.scrape_delay_max}s"
    )
    print("=" * 60)
    if future_years:
        print(f"Skipping future years: {future_years}")

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
        total_tasks_added = 0

        if current_years:
            discovered = await asyncio.gather(
                *(
                    discover_tour_tasks(
                        tour_key=t,
                        year=current_year,
                        today=today,
                        headless=headless,
                        semaphore=semaphore,
                        lookback_days=args.lookback_days,
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
                # Optionally cap the number of tasks per tour for quick test runs.
                if args.max_per_tour > 0 and len(tasks) > args.max_per_tour:
                    print(f"[{tour_key}] Limiting to {args.max_per_tour} of {len(tasks)} tasks (--max-per-tour)")
                    tasks = tasks[: args.max_per_tour]
                metrics_payload["discovery"].append(
                    {
                        "tour_key": tour_key,
                        "year": current_year,
                        "task_type": "current_tournament",
                        "duration_s": discovery_elapsed,
                        "tasks_found": len(tasks),
                    }
                )
                all_tasks.extend(tasks)

            with get_session() as session:
                queue_manager = ScrapeQueueManager(session)
                current_tasks_added = enqueue_current_tasks(session, queue_manager, all_tasks, force=args.overwrite)
            total_tasks_added += current_tasks_added
            print(f"\nAdded {current_tasks_added} current tasks to the queue")

        if past_years:
            with get_session() as session:
                queue_manager = ScrapeQueueManager(session)
                historical_tasks_added, historical_metrics = await populate_historical_queue(
                    session,
                    queue_manager,
                    past_years,
                    tours,
                    overwrite=args.overwrite,
                )
            total_tasks_added += historical_tasks_added
            metrics_payload["discovery"].extend(historical_metrics)
            print(f"\nAdded {historical_tasks_added} historical tasks to the queue")

        print(f"\nTotal tasks added to queue: {total_tasks_added}")

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
                args=(worker_id, headless, True, event_queue, args.quiet_worker_logs, args.log_file),
                kwargs={"force": args.overwrite},
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
            stats = await process_queue(session, headless=headless, fast_mode=True, force=args.overwrite)
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
