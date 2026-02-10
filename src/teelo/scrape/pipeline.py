"""
Shared scraping pipeline helpers.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Any, Mapping, Optional

from teelo.db import Match, Tournament, TournamentEdition
from teelo.db.models import estimate_match_date_from_round
from teelo.players.aliases import normalize_name
from teelo.players.identity import PlayerIdentityService
from teelo.scrape.atp import ATPScraper
from teelo.scrape.itf import ITFScraper
from teelo.scrape.parsers.score import ScoreParseError, parse_score
from teelo.scrape.utils import TOUR_TYPES
from teelo.scrape.wta import WTAScraper
from teelo.services.draw_ingestion import ingest_draw
from teelo.services.results_ingestion import ingest_results
from teelo.services.schedule_ingestion import ingest_schedule
from teelo.utils.geo import city_to_country, country_to_ioc


@dataclass(frozen=True)
class TaskParams:
    tournament_id: str
    year: int
    tour_key: str
    tournament_name: Optional[str] = None
    tournament_level: Optional[str] = None
    tournament_surface: Optional[str] = None
    tournament_location: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    tournament_number: Optional[int | str] = None
    tournament_url: Optional[str] = None
    tour_type: Optional[str] = None
    gender: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value is not None}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TaskParams":
        return cls(
            tournament_id=str(data["tournament_id"]),
            year=int(data["year"]),
            tour_key=str(data["tour_key"]),
            tournament_name=data.get("tournament_name"),
            tournament_level=data.get("tournament_level"),
            tournament_surface=data.get("tournament_surface"),
            tournament_location=data.get("tournament_location"),
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
            tournament_number=data.get("tournament_number"),
            tournament_url=data.get("tournament_url"),
            tour_type=data.get("tour_type"),
            gender=data.get("gender"),
        )


@dataclass(frozen=True)
class TournamentTask:
    task_type: str
    params: TaskParams


def build_task_params(tournament: Mapping[str, Any], tour_key: str) -> TaskParams:
    tour_config = TOUR_TYPES[tour_key]
    year = tournament.get("year")
    if year is None:
        raise ValueError("Tournament data missing year")

    tour_type = None
    gender = None
    if tour_key in ["ATP", "CHALLENGER", "WTA", "WTA_125"]:
        tour_type = tour_config.get("tour_type")
    if tour_key.startswith("ITF_"):
        gender = tour_config.get("gender")

    return TaskParams(
        tournament_id=tournament["id"],
        year=int(year),
        tour_key=tour_key,
        tournament_name=tournament.get("name"),
        tournament_level=tournament.get("level"),
        tournament_surface=tournament.get("surface"),
        tournament_location=tournament.get("location"),
        start_date=tournament.get("start_date"),
        end_date=tournament.get("end_date"),
        tournament_number=tournament.get("number"),
        tournament_url=tournament.get("url"),
        tour_type=tour_type,
        gender=gender,
    )


async def get_or_create_edition(
    session,
    task_params: TaskParams,
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
    tournament_id = task_params.tournament_id
    year = task_params.year

    # Determine tour and gender for database
    if tour_key in ["ATP", "CHALLENGER"]:
        tour = "ATP" if tour_key == "ATP" else "Challenger"
        gender = "men"
    elif tour_key.startswith("ITF_"):
        tour = "ITF"
        gender = task_params.gender or "men"
    elif tour_key == "WTA_125":
        tour = "WTA 125"
        gender = "women"
    else:
        tour = "WTA"
        gender = "women"

    # Check if tournament exists (prefer exact gender match)
    tournament = (
        session.query(Tournament)
        .filter(
            Tournament.tournament_code == tournament_id,
            Tournament.tour == tour,
            Tournament.gender == gender,
        )
        .first()
    )

    # Backward-compat: if we find an old row with missing gender, claim it.
    if not tournament:
        legacy_tournament = (
            session.query(Tournament)
            .filter(
                Tournament.tournament_code == tournament_id,
                Tournament.tour == tour,
                Tournament.gender.is_(None),
            )
            .first()
        )
        if legacy_tournament:
            legacy_tournament.gender = gender
            tournament = legacy_tournament

    if not tournament:
        tournament = Tournament(
            tournament_code=tournament_id,
            name=task_params.tournament_name or tournament_id.replace("-", " ").title(),
            tour=tour,
            gender=gender,
            level=task_params.tournament_level or "ATP 250",
            surface=task_params.tournament_surface or "Hard",
            city=task_params.tournament_location.split(",")[0]
            if task_params.tournament_location
            else None,
        )
        session.add(tournament)
        session.flush()

    # Check if edition exists
    edition = (
        session.query(TournamentEdition)
        .filter(
            TournamentEdition.tournament_id == tournament.id,
            TournamentEdition.year == year,
        )
        .first()
    )

    if not edition:
        edition = TournamentEdition(
            tournament_id=tournament.id,
            year=year,
            surface=task_params.tournament_surface or "Hard",
        )
        session.add(edition)

    # Set dates from task params if the edition is missing them
    # (applies to both new and existing editions with missing dates)
    if not edition.start_date and task_params.start_date:
        try:
            edition.start_date = datetime.strptime(task_params.start_date, "%Y-%m-%d")
        except Exception:
            pass

    if not edition.end_date and task_params.end_date:
        try:
            edition.end_date = datetime.strptime(task_params.end_date, "%Y-%m-%d")
        except Exception:
            pass

    # Estimate end_date from start_date if we still don't have one
    # Most ATP tournaments last ~7 days, Grand Slams ~14, Masters ~9
    if edition.start_date and not edition.end_date:
        level = task_params.tournament_level or "ATP 250"
        if level == "Grand Slam":
            duration_days = 14
        elif level == "Masters 1000":
            duration_days = 9
        else:
            duration_days = 7
        edition.end_date = edition.start_date + timedelta(days=duration_days)

    session.flush()

    return edition


async def execute_task(
    task_params: TaskParams | Mapping[str, Any],
    scraper,
    session,
    identity_service: PlayerIdentityService,
    mode: str,
    overwrite: bool = False,
) -> dict[str, Any]:
    normalized = _normalize_task_params(task_params)
    if mode == "historical":
        return await _execute_historical_task(
            normalized,
            scraper,
            session,
            identity_service,
            overwrite=overwrite,
        )
    if mode == "current":
        return await _execute_current_task(
            normalized,
            scraper,
            session,
            identity_service,
        )
    raise ValueError(f"Unknown execute_task mode: {mode}")


def _normalize_task_params(task_params: TaskParams | Mapping[str, Any]) -> TaskParams:
    if isinstance(task_params, TaskParams):
        return task_params
    return TaskParams.from_dict(task_params)


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def _should_scrape_schedule(task_params: TaskParams, today: date) -> bool:
    end_date = _parse_date(task_params.end_date)
    if end_date and end_date < (today - timedelta(days=1)):
        return False
    return True


def _should_scrape_results(task_params: TaskParams, today: date) -> bool:
    start_date = _parse_date(task_params.start_date)
    if start_date and start_date > (today + timedelta(days=1)):
        return False
    return True


async def _execute_current_task(
    task_params: TaskParams,
    scraper,
    session,
    identity_service: PlayerIdentityService,
) -> dict[str, Any]:
    today = date.today()
    tour_key = task_params.tour_key
    tour_config = TOUR_TYPES[tour_key]
    scraper_ctx = (
        _scraper_context(task_params.tour_key, scraper)
        if scraper is None
        else _passthrough_context(scraper)
    )

    results: dict[str, Any] = {"draw": None, "schedule": None, "results": None}

    async with scraper_ctx as active_scraper:
        edition = await get_or_create_edition(session, task_params, tour_key)

        # 1. DRAW
        try:
            print("  Scraping Draw...")
            draw_kwargs = {
                "tournament_id": task_params.tournament_id,
                "year": task_params.year,
            }
            if tour_key in ["ATP", "CHALLENGER"]:
                draw_kwargs["tournament_number"] = task_params.tournament_number
                draw_kwargs["tour_type"] = tour_config["tour_type"]
            elif tour_key in ["WTA", "WTA_125"]:
                draw_kwargs["tournament_number"] = task_params.tournament_number
            elif tour_key.startswith("ITF"):
                draw_kwargs = {
                    "tournament_url": task_params.tournament_url,
                    "tournament_info": _itf_tournament_info(task_params),
                }

            entries = await active_scraper.scrape_tournament_draw(**draw_kwargs)
            stats = ingest_draw(session, entries, edition, identity_service)
            results["draw"] = stats.summary()
            print(f"  Draw: {results['draw']}")
        except Exception as exc:
            print(f"  Draw Error: {exc}")
            session.rollback()
            edition = await get_or_create_edition(session, task_params, tour_key)

        # 2. SCHEDULE
        if _should_scrape_schedule(task_params, today):
            try:
                print("  Scraping Schedule...")
                sched_kwargs: dict[str, Any] = {}
                if tour_key.startswith("ITF"):
                    sched_kwargs["tournament_url"] = task_params.tournament_url
                elif tour_key in ["ATP", "CHALLENGER", "WTA", "WTA_125"]:
                    sched_kwargs = {
                        "tournament_id": task_params.tournament_id,
                        "tournament_number": task_params.tournament_number,
                    }
                    if tour_key in ["WTA", "WTA_125"]:
                        sched_kwargs["year"] = task_params.year

                fixtures = []
                async for fixture in active_scraper.scrape_fixtures(**sched_kwargs):
                    fixtures.append(fixture)
                stats = ingest_schedule(session, fixtures, edition, identity_service)
                results["schedule"] = stats.summary()
                print(f"  Schedule: {results['schedule']}")
            except Exception as exc:
                print(f"  Schedule Error: {exc}")
                session.rollback()
                edition = await get_or_create_edition(session, task_params, tour_key)
        else:
            print("  Skipping Schedule (tournament appears fully completed).")

        # 3. RESULTS
        if _should_scrape_results(task_params, today):
            try:
                print("  Scraping Results...")
                res_kwargs = {
                    "tournament_id": task_params.tournament_id,
                    "year": task_params.year,
                }
                if tour_key in ["ATP", "CHALLENGER"]:
                    res_kwargs["tournament_number"] = task_params.tournament_number
                    res_kwargs["tour_type"] = tour_config["tour_type"]
                elif tour_key in ["WTA", "WTA_125"]:
                    res_kwargs["tournament_number"] = task_params.tournament_number
                elif tour_key.startswith("ITF"):
                    res_kwargs = {
                        "tournament_url": task_params.tournament_url,
                        "tournament_info": _itf_tournament_info(task_params),
                    }

                matches = []
                async for match in active_scraper.scrape_tournament_results(**res_kwargs):
                    matches.append(match)
                stats = ingest_results(session, matches, edition, identity_service)
                results["results"] = stats.summary()
                print(f"  Results: {results['results']}")
            except Exception as exc:
                print(f"  Results Error: {exc}")
                session.rollback()
        else:
            print("  Skipping Results (tournament has not started yet).")

    session.commit()
    return results


async def _execute_historical_task(
    task_params: TaskParams,
    scraper,
    session,
    identity_service: PlayerIdentityService,
    overwrite: bool = False,
) -> dict[str, int]:
    tour_key = task_params.tour_key
    tour_config = TOUR_TYPES.get(tour_key, TOUR_TYPES["ATP"])

    result = {
        "matches_scraped": 0,
        "matches_created": 0,
        "players_created": 0,
    }

    # Preload existing external_ids for this tournament edition to avoid per-match DB checks
    # and track new ones in-memory to prevent duplicates within this run.
    known_external_ids: set[str] = set()
    existing_matches_by_external_id: dict[str, Match] = {}
    player_cache_by_external_id: dict[tuple[str, str], int] = {}
    player_cache_by_name: dict[tuple[str, str], int] = {}

    # Get or create tournament edition
    edition = await get_or_create_edition(session, task_params, tour_key)

    # Preload existing matches for this edition (single DB call)
    existing_matches = (
        session.query(Match)
        .filter(Match.tournament_edition_id == edition.id)
        .all()
    )
    existing_matches_by_external_id = {
        match.external_id: match for match in existing_matches if match.external_id
    }
    known_external_ids.update(existing_matches_by_external_id.keys())

    # Flag to update tournament metadata once we have real data
    metadata_updated = False

    scraper_ctx = _scraper_context(tour_key, scraper)
    pending_matches: list[Match] = []
    batch_size = 200

    async with scraper_ctx as active_scraper:
        if tour_config["scraper"] == "atp":
            async for scraped_match in active_scraper.scrape_tournament_results(
                task_params.tournament_id,
                task_params.year,
                tournament_number=task_params.tournament_number,
                tour_type=task_params.tour_type or "main",
            ):
                if not metadata_updated:
                    await update_tournament_metadata(session, edition, scraped_match)
                    metadata_updated = True

                result["matches_scraped"] += 1
                match_result, created = await process_scraped_match(
                    session,
                    scraped_match,
                    edition,
                    identity_service,
                    known_external_ids,
                    existing_matches_by_external_id,
                    player_cache_by_external_id,
                    player_cache_by_name,
                    overwrite=overwrite,
                )
                if match_result and created:
                    pending_matches.append(match_result)
                    result["matches_created"] += 1
                    if len(pending_matches) >= batch_size:
                        session.add_all(pending_matches)
                        session.flush()
                        pending_matches.clear()

        elif tour_config["scraper"] == "itf":
            if not task_params.tournament_url:
                raise ValueError("ITF tasks require tournament_url")

            tournament_info = _itf_tournament_info(task_params)

            async for scraped_match in active_scraper.scrape_tournament_results(
                task_params.tournament_url, tournament_info
            ):
                result["matches_scraped"] += 1
                match_result, created = await process_scraped_match(
                    session,
                    scraped_match,
                    edition,
                    identity_service,
                    known_external_ids,
                    existing_matches_by_external_id,
                    player_cache_by_external_id,
                    player_cache_by_name,
                    overwrite=overwrite,
                )
                if match_result and created:
                    pending_matches.append(match_result)
                    result["matches_created"] += 1
                    if len(pending_matches) >= batch_size:
                        session.add_all(pending_matches)
                        session.flush()
                        pending_matches.clear()

        elif tour_config["scraper"] == "wta":
            async for scraped_match in active_scraper.scrape_tournament_results(
                task_params.tournament_id,
                task_params.year,
                tournament_number=task_params.tournament_number,
            ):
                result["matches_scraped"] += 1
                match_result, created = await process_scraped_match(
                    session,
                    scraped_match,
                    edition,
                    identity_service,
                    known_external_ids,
                    existing_matches_by_external_id,
                    player_cache_by_external_id,
                    player_cache_by_name,
                    overwrite=overwrite,
                )
                if match_result and created:
                    pending_matches.append(match_result)
                    result["matches_created"] += 1
                    if len(pending_matches) >= batch_size:
                        session.add_all(pending_matches)
                        session.flush()
                        pending_matches.clear()

    if pending_matches:
        session.add_all(pending_matches)
        session.flush()

    session.commit()
    return result


def _itf_tournament_info(task_params: TaskParams) -> dict[str, Any]:
    return {
        "id": task_params.tournament_id,
        "name": task_params.tournament_name or task_params.tournament_id,
        "year": task_params.year,
        "level": task_params.tournament_level or "ITF",
        "surface": task_params.tournament_surface or "Hard",
        "location": task_params.tournament_location,
        "gender": task_params.gender or "men",
    }


def _scraper_context(tour_key: str, scraper=None):
    if scraper is not None:
        return _passthrough_context(scraper)

    scraper_type = TOUR_TYPES[tour_key]["scraper"]
    if scraper_type == "atp":
        return ATPScraper(headless=False)
    if scraper_type == "wta":
        return WTAScraper(headless=False)
    if scraper_type == "itf":
        return ITFScraper(headless=False)
    raise ValueError(f"Unknown scraper type for {tour_key}")


class _passthrough_context:
    def __init__(self, scraper):
        self.scraper = scraper

    async def __aenter__(self):
        return self.scraper

    async def __aexit__(self, exc_type, exc, exc_tb):
        return False


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
        "|" in tournament.name
        or len(tournament.name) > 50
        or tournament.name == tournament.tournament_code
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
    existing_matches_by_external_id: dict[str, Match] | None = None,
    player_cache_by_external_id: dict[tuple[str, str], int] | None = None,
    player_cache_by_name: dict[tuple[str, str], int] | None = None,
    overwrite: bool = False,
) -> tuple[Optional[Match], bool]:
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
            return None, False  # Skip duplicate

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
        return None, False

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
        return None, False

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
    if existing_matches_by_external_id is not None and scraped_match.external_id:
        existing = existing_matches_by_external_id.get(scraped_match.external_id)

    if existing and not overwrite:
        return existing, False

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

        return existing, False

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

    if known_external_ids is not None:
        known_external_ids.add(scraped_match.external_id)
    if existing_matches_by_external_id is not None and scraped_match.external_id:
        existing_matches_by_external_id[scraped_match.external_id] = match
    return match, True
