"""
Shared helpers for discovering tournament tasks.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Mapping, Optional

from teelo.scrape.atp import ATPScraper
from teelo.scrape.itf import ITFScraper
from teelo.scrape.pipeline import TournamentTask, build_task_params
from teelo.scrape.utils import TOUR_TYPES
from teelo.scrape.wta import WTAScraper


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def _is_tournament_in_window(
    tournament: Mapping[str, Any],
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


def normalize_tournament(
    tournament: Mapping[str, Any],
    tour_key: str,
    year: int,
) -> dict[str, Any]:
    tournament_id = tournament.get("id") or tournament.get("tournament_id")
    if not tournament_id:
        raise ValueError("Tournament entry missing id")

    return {
        "id": tournament_id,
        "name": tournament.get("name"),
        "level": tournament.get("level"),
        "surface": tournament.get("surface"),
        "location": tournament.get("location"),
        "start_date": tournament.get("start_date"),
        "end_date": tournament.get("end_date"),
        "number": tournament.get("number"),
        "url": tournament.get("url"),
        "tour_key": tour_key,
        "year": year,
    }


def _get_scraper_class(tour_key: str):
    scraper_type = TOUR_TYPES[tour_key]["scraper"]
    if scraper_type == "atp":
        return ATPScraper
    if scraper_type == "wta":
        return WTAScraper
    if scraper_type == "itf":
        return ITFScraper
    raise ValueError(f"Unknown scraper type for {tour_key}")


async def _fetch_tournaments_with_scraper(
    scraper,
    tour_key: str,
    year: int,
) -> list[dict]:
    config = TOUR_TYPES[tour_key]
    if config["scraper"] == "atp":
        return await scraper.get_tournament_list(year, tour_type=config["tour_type"])
    if config["scraper"] == "wta":
        return await scraper.get_tournament_list(year, tour_type=config["tour_type"])
    return await scraper.get_tournament_list(year, gender=config["gender"])


async def discover_tournament_tasks(
    tour_key: str,
    year: int,
    task_type: str,
    *,
    scraper=None,
    window: Optional[tuple[date, date]] = None,
    headless: Optional[bool] = None,
) -> list[TournamentTask]:
    if scraper is None:
        scraper_cls = _get_scraper_class(tour_key)
        use_headless = False if headless is None else headless
        async with scraper_cls(headless=use_headless) as active_scraper:
            tournaments = await _fetch_tournaments_with_scraper(active_scraper, tour_key, year)
    else:
        tournaments = await _fetch_tournaments_with_scraper(scraper, tour_key, year)

    tasks: list[TournamentTask] = []
    for tournament in tournaments:
        try:
            normalized = normalize_tournament(tournament, tour_key, year)
        except ValueError:
            continue
        if window and not _is_tournament_in_window(normalized, window[0], window[1]):
            continue
        task_params = build_task_params(normalized, tour_key)
        tasks.append(TournamentTask(task_type=task_type, params=task_params))
    return tasks
