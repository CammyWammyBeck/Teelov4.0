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
import multiprocessing
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import func, cast, Integer

from teelo.config import settings
from teelo.db import get_session, Player, Match, Tournament, TournamentEdition
from teelo.db.models import ScrapeQueue, estimate_match_date_from_round
from teelo.scrape.queue import ScrapeQueueManager
from teelo.scrape.atp import ATPScraper
from teelo.scrape.itf import ITFScraper
from teelo.scrape.wta import WTAScraper
from teelo.scrape.utils import TOUR_TYPES, get_tournaments_for_tour
from teelo.scrape.parsers.score import parse_score, ScoreParseError
from teelo.players.identity import PlayerIdentityService
from teelo.players.aliases import normalize_name
from teelo.utils.geo import city_to_country, country_to_ioc


# Processing order (most important first)
TOUR_ORDER = ["ATP", "CHALLENGER", "WTA", "WTA_125", "ITF_MEN", "ITF_WOMEN"]


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

    # Validate tour types
    for tour in tours:
        if tour not in TOUR_TYPES:
            print(f"Warning: Unknown tour type '{tour}'. Valid types: {', '.join(TOUR_TYPES.keys())}")
            tours.remove(tour)

    # Sort by processing order
    return [t for t in TOUR_ORDER if t in tours]


async def populate_queue(
    session,
    queue_manager: ScrapeQueueManager,
    years: list[int],
    tours: list[str],
) -> int:
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

                tournaments = await get_tournaments_for_tour(tour_key, year)
                
                for tournament in tournaments:
                    # Skip tournaments that are too far in the future
                    start_date_str = tournament.get("start_date")
                    if start_date_str:
                        try:
                            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                            if start_date > future_cutoff:
                                skipped_future += 1
                                continue
                        except (ValueError, TypeError):
                            pass

                    # Create task params
                    task_params = {
                        "tournament_id": tournament["id"],
                        "year": year,
                        "tour_key": tour_key,
                        "tournament_name": tournament.get("name"),
                        "tournament_level": tournament.get("level"),
                        "tournament_surface": tournament.get("surface"),
                        "tournament_location": tournament.get("location"),
                        "start_date": tournament.get("start_date"),
                        "end_date": tournament.get("end_date"),
                    }

                    # Add tour-specific params
                    if tour_key in ["ATP", "CHALLENGER"]:
                        task_params["tour_type"] = tour_config["tour_type"]
                        if tournament.get("number"):
                            task_params["tournament_number"] = tournament["number"]
                    elif tour_key in ["WTA", "WTA_125"]:
                        task_params["tour_type"] = tour_config["tour_type"]
                        # WTA needs the tournament number for draw URLs
                        if tournament.get("number"):
                            task_params["tournament_number"] = tournament["number"]
                    elif tour_key.startswith("ITF_"):
                        task_params["gender"] = tour_config["gender"]
                        task_params["tournament_url"] = tournament.get("url")

                    # Skip if already queued
                    if tournament["id"] in existing_tournament_ids:
                        continue

                    # Enqueue task (bulk later)
                    tasks_to_add.append(
                        ScrapeQueue(
                            task_type="historical_tournament",
                            task_params=task_params,
                            priority=base_priority,
                            max_attempts=3,
                            status="pending",
                        )
                    )
                    year_tasks_added += 1
                    tasks_added += 1
                
                msg = f"\n  {year}: Found {len(tournaments)} tournaments, added {year_tasks_added} to queue"
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

    return tasks_added


async def process_queue(
    session,
    overwrite: bool = False,
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

    stats = {
        "tasks_processed": 0,
        "tasks_completed": 0,
        "tasks_failed": 0,
        "matches_scraped": 0,
        "matches_created": 0,
        "players_created": 0,
    }

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
            task_params = task.task_params
            tour_key = task_params.get("tour_key", "ATP")

            log(
                f"\n[Task {stats['tasks_processed']}/{initial_queue_size}] "
                f"{task_params.get('tournament_name', task_params['tournament_id'])} "
                f"({task_params['year']})"
            )
            log(f"  Tour: {TOUR_TYPES.get(tour_key, {}).get('description', tour_key)}")

            try:
                # Scrape the tournament
                matches_result = await scrape_tournament_task(
                    session,
                    task_params,
                    identity_service,
                    overwrite=overwrite,
                )

                stats["matches_scraped"] += matches_result["matches_scraped"]
                stats["matches_created"] += matches_result["matches_created"]
                stats["players_created"] += matches_result["players_created"]

                # Commit the matches, then mark completed
                session.commit()
                queue_manager.mark_completed(task.id)
                stats["tasks_completed"] += 1
                log(f"  Completed: {matches_result['matches_created']} matches created")

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

    return stats


def run_worker(
    worker_id: int,
    overwrite: bool,
    fast: bool,
    stats_queue: Optional[multiprocessing.Queue] = None,
) -> None:
    apply_fast_delays(fast)

    with get_session() as session:
        stats = asyncio.run(
            process_queue(session, overwrite=overwrite, worker_id=worker_id)
        )

    if stats_queue is not None:
        stats_queue.put(stats)


async def scrape_tournament_task(
    session,
    task_params: dict,
    identity_service: PlayerIdentityService,
    overwrite: bool = False,
) -> dict:
    """
    Scrape a single tournament based on task parameters.

    Args:
        session: Database session
        task_params: Task parameters from queue
        identity_service: PlayerIdentityService for player matching
        overwrite: If True, overwrite existing matches instead of skipping

    Returns:
        Dictionary with scraping statistics
    """
    tour_key = task_params.get("tour_key", "ATP")
    tour_config = TOUR_TYPES.get(tour_key, TOUR_TYPES["ATP"])

    result = {
        "matches_scraped": 0,
        "matches_created": 0,
        "players_created": 0,
    }

    # Preload existing external_ids for this tournament edition to avoid per-match DB checks
    # and track new ones in-memory to prevent duplicates within this run.
    known_external_ids: set[str] = set()
    player_cache_by_external_id: dict[tuple[str, str], int] = {}
    player_cache_by_name: dict[tuple[str, str], int] = {}

    # Get or create tournament edition
    edition = await get_or_create_edition(session, task_params, tour_key)

    # Preload existing matches for this edition (single DB call)
    existing_ids = (
        session.query(Match.external_id)
        .filter(Match.tournament_edition_id == edition.id)
        .all()
    )
    known_external_ids.update([eid for (eid,) in existing_ids if eid])
    
    # Flag to update tournament metadata once we have real data
    metadata_updated = False

    if tour_config["scraper"] == "atp":
        async with ATPScraper(headless=False) as scraper:
            async for scraped_match in scraper.scrape_tournament_results(
                task_params["tournament_id"],
                task_params["year"],
                tournament_number=task_params.get("tournament_number"),
                tour_type=task_params.get("tour_type", "main"),
            ):
                # Update tournament metadata using the first match found
                # This fixes issues with poor initial list parsing (garbage names, missing location)
                if not metadata_updated:
                    await update_tournament_metadata(session, edition, scraped_match)
                    metadata_updated = True
                
                result["matches_scraped"] += 1
                match_result = await process_scraped_match(
                    session,
                    scraped_match,
                    edition,
                    identity_service,
                    known_external_ids,
                    player_cache_by_external_id,
                    player_cache_by_name,
                    overwrite=overwrite,
                )
                if match_result:
                    result["matches_created"] += 1

    elif tour_config["scraper"] == "itf":
        tournament_url = task_params.get("tournament_url")
        if not tournament_url:
            raise ValueError("ITF tasks require tournament_url")

        tournament_info = {
            "id": task_params["tournament_id"],
            "name": task_params.get("tournament_name", task_params["tournament_id"]),
            "year": task_params["year"],
            "level": task_params.get("tournament_level", "ITF"),
            "surface": task_params.get("tournament_surface", "Hard"),
            "location": task_params.get("tournament_location"),
            "gender": task_params.get("gender", "men"),
        }

        async with ITFScraper(headless=False) as scraper:
            async for scraped_match in scraper.scrape_tournament_results(
                tournament_url, tournament_info
            ):
                result["matches_scraped"] += 1
                match_result = await process_scraped_match(
                    session,
                    scraped_match,
                    edition,
                    identity_service,
                    known_external_ids,
                    player_cache_by_external_id,
                    player_cache_by_name,
                    overwrite=overwrite,
                )
                if match_result:
                    result["matches_created"] += 1

    elif tour_config["scraper"] == "wta":
        async with WTAScraper(headless=False) as scraper:
            async for scraped_match in scraper.scrape_tournament_results(
                task_params["tournament_id"],
                task_params["year"],
                tournament_number=task_params.get("tournament_number"),
            ):
                result["matches_scraped"] += 1
                match_result = await process_scraped_match(
                    session,
                    scraped_match,
                    edition,
                    identity_service,
                    known_external_ids,
                    player_cache_by_external_id,
                    player_cache_by_name,
                    overwrite=overwrite,
                )
                if match_result:
                    result["matches_created"] += 1

    return result


async def get_or_create_edition(
    session,
    task_params: dict,
    tour_key: str,
) -> TournamentEdition:
    """
    Get or create tournament and edition in database.

    Args:
        session: Database session
        task_params: Task parameters with tournament info
        tour_key: Tour type key

    Returns:
        TournamentEdition object
    """
    tournament_id = task_params["tournament_id"]
    year = task_params["year"]

    # Determine tour and gender for database
    if tour_key in ["ATP", "CHALLENGER"]:
        tour = "ATP" if tour_key == "ATP" else "Challenger"
        gender = "men"
    elif tour_key.startswith("ITF_"):
        tour = "ITF"
        gender = task_params.get("gender", "men")
    elif tour_key == "WTA_125":
        tour = "WTA 125"
        gender = "women"
    else:
        tour = "WTA"
        gender = "women"

    # Check if tournament exists (prefer exact gender match)
    tournament = session.query(Tournament).filter(
        Tournament.tournament_code == tournament_id,
        Tournament.tour == tour,
        Tournament.gender == gender,
    ).first()

    # Backward-compat: if we find an old row with missing gender, claim it.
    if not tournament:
        legacy_tournament = session.query(Tournament).filter(
            Tournament.tournament_code == tournament_id,
            Tournament.tour == tour,
            Tournament.gender.is_(None),
        ).first()
        if legacy_tournament:
            legacy_tournament.gender = gender
            tournament = legacy_tournament

    if not tournament:
        tournament = Tournament(
            tournament_code=tournament_id,
            name=task_params.get("tournament_name", tournament_id.replace("-", " ").title()),
            tour=tour,
            gender=gender,
            level=task_params.get("tournament_level", "ATP 250"),
            surface=task_params.get("tournament_surface", "Hard"),
            city=task_params.get("tournament_location", "").split(",")[0] if task_params.get("tournament_location") else None,
        )
        session.add(tournament)
        session.flush()

    # Check if edition exists
    edition = session.query(TournamentEdition).filter(
        TournamentEdition.tournament_id == tournament.id,
        TournamentEdition.year == year,
    ).first()

    if not edition:
        edition = TournamentEdition(
            tournament_id=tournament.id,
            year=year,
            surface=task_params.get("tournament_surface", "Hard"),
        )
        session.add(edition)

    # Set dates from task params if the edition is missing them
    # (applies to both new and existing editions with missing dates)
    if not edition.start_date and task_params.get("start_date"):
        try:
            edition.start_date = datetime.strptime(task_params["start_date"], "%Y-%m-%d")
        except Exception:
            pass

    if not edition.end_date and task_params.get("end_date"):
        try:
            edition.end_date = datetime.strptime(task_params["end_date"], "%Y-%m-%d")
        except Exception:
            pass

    # Estimate end_date from start_date if we still don't have one
    # Most ATP tournaments last ~7 days, Grand Slams ~14, Masters ~9
    if edition.start_date and not edition.end_date:
        level = task_params.get("tournament_level", "ATP 250")
        if level == "Grand Slam":
            duration_days = 14
        elif level == "Masters 1000":
            duration_days = 9
        else:
            duration_days = 7
        edition.end_date = edition.start_date + timedelta(days=duration_days)

    session.flush()

    return edition


async def update_tournament_metadata(
    session,
    edition: TournamentEdition,
    scraped_match,
):
    """
    Update tournament and edition metadata from scraped match data.
    
    This fixes issues where the initial tournament list scraping yielded
    incomplete or incorrect data (e.g. missing location, wrong surface).
    """
    # Update Tournament
    tournament = edition.tournament
    
    # Update Name if it looks like garbage (contains dates/locations) or if we have a better one
    # The scraper returns clean names attached to matches
    if scraped_match.tournament_name and (
        "|" in tournament.name or  # Garbage indicator
        len(tournament.name) > 50 or # Suspiciously long
        tournament.name == tournament.tournament_code # Default fallback
    ):
        tournament.name = scraped_match.tournament_name

    # Update Location
    if scraped_match.tournament_location and not tournament.city:
        loc = scraped_match.tournament_location
        if "," in loc:
            parts = loc.split(",")
            tournament.city = parts[0].strip()
            tournament.country = parts[1].strip()
        else:
            tournament.city = loc
            
    if scraped_match.tournament_country_ioc and not tournament.country_ioc:
        tournament.country_ioc = scraped_match.tournament_country_ioc

    # Fill in country/IOC from city via geo lookup if still missing
    if tournament.city and not tournament.country:
        country = city_to_country(tournament.city)
        if country:
            tournament.country = country
    if tournament.city and tournament.country and not tournament.country_ioc:
        ioc = country_to_ioc(tournament.country)
        if ioc:
            tournament.country_ioc = ioc

    # Update Surface
    # Only update if current is generic/default and new is specific
    if scraped_match.tournament_surface:
        new_surface = scraped_match.tournament_surface
        # Update tournament default if not set
        if not tournament.surface or tournament.surface == "Hard":
             tournament.surface = new_surface
        
        # Always update edition surface to match actual event
        edition.surface = new_surface
        
    session.flush()


async def process_scraped_match(
    session,
    scraped_match,
    edition: TournamentEdition,
    identity_service: PlayerIdentityService,
    known_external_ids: set[str] = None,
    player_cache_by_external_id: dict[tuple[str, str], int] | None = None,
    player_cache_by_name: dict[tuple[str, str], int] | None = None,
    overwrite: bool = False,
) -> Optional[Match]:
    """
    Process a scraped match and store in database.

    Args:
        session: Database session
        scraped_match: ScrapedMatch from scraper
        edition: TournamentEdition to link match to
        identity_service: PlayerIdentityService for player matching
        seen_external_ids: Optional set to track external_ids seen in this batch
                          (prevents duplicates before DB commit)
        overwrite: If True, update existing matches with fresh scraped data

    Returns:
        Match object if created/updated, None if skipped
    """
    # Check for in-batch duplicate first (before DB query)
    # This catches duplicates that haven't been committed yet
    if known_external_ids is not None:
        if scraped_match.external_id in known_external_ids and not overwrite:
            return None  # Skip duplicate

    # Find or create player A (with cache)
    player_a_id = None
    if player_cache_by_external_id is not None and scraped_match.player_a_external_id:
        cache_key = (scraped_match.source, scraped_match.player_a_external_id)
        player_a_id = player_cache_by_external_id.get(cache_key)
    if player_a_id is None and player_cache_by_name is not None:
        name_key = (scraped_match.source, normalize_name(scraped_match.player_a_name))
        player_a_id = player_cache_by_name.get(name_key)
    if player_a_id is None:
        player_a_id, _ = identity_service.find_or_queue_player(
            name=scraped_match.player_a_name,
            source=scraped_match.source,
            external_id=scraped_match.player_a_external_id,
        )

    if not player_a_id and scraped_match.player_a_external_id:
        player_a_id = identity_service.create_player(
            name=scraped_match.player_a_name,
            source=scraped_match.source,
            external_id=scraped_match.player_a_external_id,
            nationality=scraped_match.player_a_nationality,
        )

    if not player_a_id:
        return None

    # Find or create player B (with cache)
    player_b_id = None
    if player_cache_by_external_id is not None and scraped_match.player_b_external_id:
        cache_key = (scraped_match.source, scraped_match.player_b_external_id)
        player_b_id = player_cache_by_external_id.get(cache_key)
    if player_b_id is None and player_cache_by_name is not None:
        name_key = (scraped_match.source, normalize_name(scraped_match.player_b_name))
        player_b_id = player_cache_by_name.get(name_key)
    if player_b_id is None:
        player_b_id, _ = identity_service.find_or_queue_player(
            name=scraped_match.player_b_name,
            source=scraped_match.source,
            external_id=scraped_match.player_b_external_id,
        )

    if not player_b_id and scraped_match.player_b_external_id:
        player_b_id = identity_service.create_player(
            name=scraped_match.player_b_name,
            source=scraped_match.source,
            external_id=scraped_match.player_b_external_id,
            nationality=scraped_match.player_b_nationality,
        )

    if not player_b_id:
        return None
    
    # Cache matched players
    if player_cache_by_external_id is not None:
        if scraped_match.player_a_external_id:
            player_cache_by_external_id[(scraped_match.source, scraped_match.player_a_external_id)] = player_a_id
        if scraped_match.player_b_external_id:
            player_cache_by_external_id[(scraped_match.source, scraped_match.player_b_external_id)] = player_b_id
    if player_cache_by_name is not None:
        player_cache_by_name[(scraped_match.source, normalize_name(scraped_match.player_a_name))] = player_a_id
        player_cache_by_name[(scraped_match.source, normalize_name(scraped_match.player_b_name))] = player_b_id

    existing = None
    if known_external_ids is not None and scraped_match.external_id in known_external_ids:
        existing = session.query(Match).filter(
            Match.external_id == scraped_match.external_id
        ).first()

    if existing and not overwrite:
        return existing

    # Parse score
    score_structured = None
    try:
        parsed = parse_score(scraped_match.score_raw)
        score_structured = parsed.to_structured()
    except ScoreParseError:
        pass

    # Parse date — if scraper didn't provide one, estimate from tournament dates + round
    match_date = None
    match_date_estimated = False
    if scraped_match.match_date:
        try:
            match_date = datetime.strptime(scraped_match.match_date, "%Y-%m-%d").date()
        except ValueError:
            pass

    if match_date is None and edition.start_date and edition.end_date:
        match_date = estimate_match_date_from_round(
            round_code=scraped_match.round or "R128",
            tournament_start=edition.start_date,
            tournament_end=edition.end_date,
        )
        if match_date is not None:
            match_date_estimated = True

    if existing and overwrite:
        # Update the existing match with fresh scraped data
        existing.source = scraped_match.source
        existing.tournament_edition_id = edition.id
        existing.round = scraped_match.round
        existing.match_number = scraped_match.match_number
        existing.player_a_id = player_a_id
        existing.player_b_id = player_b_id
        existing.winner_id = player_a_id
        existing.score = scraped_match.score_raw
        existing.score_structured = score_structured
        existing.match_date = match_date
        existing.match_date_estimated = match_date_estimated
        existing.status = scraped_match.status
        existing.retirement_set = scraped_match.retirement_set

        # Recompute temporal order with (potentially new) edition dates
        existing.update_temporal_order(
            tournament_start=edition.start_date,
            tournament_end=edition.end_date,
        )

        return existing

    # Create new match
    match = Match(
        external_id=scraped_match.external_id,
        source=scraped_match.source,
        tournament_edition_id=edition.id,
        round=scraped_match.round,
        match_number=scraped_match.match_number,
        player_a_id=player_a_id,
        player_b_id=player_b_id,
        winner_id=player_a_id,  # Player A is typically the winner
        score=scraped_match.score_raw,
        score_structured=score_structured,
        match_date=match_date,
        match_date_estimated=match_date_estimated,
        status=scraped_match.status,
        retirement_set=scraped_match.retirement_set,
    )

    # Compute temporal order
    match.update_temporal_order(
        tournament_start=edition.start_date,
        tournament_end=edition.end_date,
    )

    session.add(match)
    if known_external_ids is not None:
        known_external_ids.add(scraped_match.external_id)
    return match


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

    args = parser.parse_args()

    print("=" * 60)
    print("Teelo v4.0 - Historical Data Backfill")
    print("=" * 60)

    if args.fast:
        apply_fast_delays(True)

    with get_session() as session:
        queue_manager = ScrapeQueueManager(session)

        # Show stats and exit
        if args.stats:
            show_queue_stats(session)
            return

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
            tasks_added = await populate_queue(session, queue_manager, years, tours)
            print(f"\nAdded {tasks_added} tasks to queue")
            session.commit()

        # Process the queue
        if args.workers > 1:
            ctx = multiprocessing.get_context("spawn")
            stats_queue: multiprocessing.Queue = ctx.Queue()
            processes = []

            for worker_id in range(1, args.workers + 1):
                process = ctx.Process(
                    target=run_worker,
                    args=(worker_id, args.overwrite, args.fast, stats_queue),
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
            }

            stats_received = 0
            while stats_received < len(processes):
                try:
                    worker_stats = stats_queue.get_nowait()
                except Exception:
                    break
                for key in aggregated:
                    aggregated[key] += worker_stats.get(key, 0)
                stats_received += 1

            stats = aggregated
        else:
            stats = await process_queue(session, overwrite=args.overwrite)

        # Final summary
        print("\n" + "=" * 60)
        print("Backfill Complete")
        print("=" * 60)
        print(f"  Tasks processed: {stats['tasks_processed']}")
        print(f"  Tasks completed: {stats['tasks_completed']}")
        print(f"  Tasks failed: {stats['tasks_failed']}")
        print(f"  Matches scraped: {stats['matches_scraped']}")
        print(f"  Matches created: {stats['matches_created']}")

        # Show remaining queue
        remaining = queue_manager.pending_count()
        if remaining > 0:
            print(f"\n  Remaining in queue: {remaining}")
            print("  Run with --resume to continue")


if __name__ == "__main__":
    asyncio.run(main())
