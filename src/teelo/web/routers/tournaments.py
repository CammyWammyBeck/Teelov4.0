import math
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import and_, case, func, or_
from sqlalchemy.orm import Session, contains_eager, joinedload

from teelo.db.models import ROUND_ORDER, Match, Player, Tournament, TournamentEdition
from teelo.db.session import get_db
from teelo.match_statuses import get_status_group
from teelo.web.app_context import templates
from teelo.web.services.match_service import serialize_match, slugify_name, upcoming_sort_expressions

router = APIRouter()

MAIN_DRAW_ROUNDS: tuple[str, ...] = ("R128", "R64", "R32", "R16", "QF", "SF", "F")
ROUND_LABELS: dict[str, str] = {
    "R128": "Round of 128",
    "R64": "Round of 64",
    "R32": "Round of 32",
    "R16": "Round of 16",
    "QF": "Quarterfinals",
    "SF": "Semifinals",
    "F": "Final",
}
TOURNAMENT_LEVEL_FILTERS: tuple[str, ...] = (
    "Grand Slam",
    "Masters 1000",
    "WTA 1000",
    "ATP 500",
    "WTA 500",
    "ATP 250",
    "WTA 250",
    "Challenger",
    "WTA 125",
    "ITF",
    "ITF Men",
    "ITF Women",
    "ATP Finals",
    "WTA Finals",
)
TOURNAMENT_BROWSE_YEAR_CHIP_COUNT = 6
CURRENT_WINDOW_BEFORE_DAYS = 3
CURRENT_WINDOW_AFTER_DAYS = 3
LEVEL_PRIORITY = {
    "Grand Slam": 1,
    "Masters 1000": 2,
    "WTA 1000": 2,
    "ATP 500": 3,
    "WTA 500": 3,
    "ATP 250": 4,
    "WTA 250": 4,
    "WTA 125": 5,
    "Challenger": 6,
    "ITF": 7,
    "ITF Men": 7,
    "ITF Women": 7,
    "ATP Finals": 2,
    "WTA Finals": 2,
}


def _build_edition_history_payload(
    editions: list[TournamentEdition],
    finals_by_edition: dict[int, Match],
    tournament: Tournament,
    tour_key: str,
    tournament_code: str,
) -> list[dict]:
    payload = []
    for edition in editions:
        final = finals_by_edition.get(edition.id)
        if final is None:
            continue
        champion = None
        champion_id = None
        runner_up = None
        runner_up_id = None
        score = final.score
        if final.winner is not None:
            champion = final.winner.canonical_name
            champion_id = final.winner.id
        elif final.winner_id == final.player_a_id and final.player_a is not None:
            champion = final.player_a.canonical_name
            champion_id = final.player_a.id
        elif final.winner_id == final.player_b_id and final.player_b is not None:
            champion = final.player_b.canonical_name
            champion_id = final.player_b.id

        if final.winner_id == final.player_a_id and final.player_b is not None:
            runner_up = final.player_b.canonical_name
            runner_up_id = final.player_b.id
        elif final.winner_id == final.player_b_id and final.player_a is not None:
            runner_up = final.player_a.canonical_name
            runner_up_id = final.player_a.id
        payload.append(
            {
                "year": edition.year,
                "champion": champion,
                "champion_id": champion_id,
                "runner_up": runner_up,
                "runner_up_id": runner_up_id,
                "score": score,
                "surface": edition.surface or tournament.surface,
                "url": f"/tournaments/{tour_key.lower()}/{tournament_code}/{edition.year}",
            }
        )
    return payload


def _filter_editions_with_finals(
    editions: list[TournamentEdition],
    finals_by_edition: dict[int, Match],
) -> list[TournamentEdition]:
    return [edition for edition in editions if edition.id in finals_by_edition]


def _normalize_tour(tour: str) -> str:
    return (tour or "").strip().upper()


_TOUR_ALIASES: dict[str, list[str]] = {
    "ATP": ["ATP"],
    "WTA": ["WTA"],
    "CHALLENGER": ["CHALLENGER", "Challenger"],
    "ITF": ["ITF"],
    "WTA_125": ["WTA_125", "WTA 125"],
}


def _resolve_tour_values(tour: str) -> list[str]:
    """Return all DB-stored tour values matching the input."""
    key = _normalize_tour(tour).replace(" ", "_")
    return _TOUR_ALIASES.get(key, [(tour or "").strip()])


def _badge_for_tournament(tournament: Tournament) -> tuple[str, str]:
    if tournament.level == "Grand Slam":
        return "GSL", "bg-tour-grandslam"
    tour = (tournament.tour or "").strip()
    if tour in ("CHALLENGER", "Challenger"):
        return "CHL", "bg-tour-challenger"
    if tour in ("WTA 125", "WTA_125") or tournament.level in ("WTA 125", "WTA_125"):
        return "125", "bg-tour-250"
    if tour == "WTA":
        return "WTA", "bg-tour-wta"
    if tour == "ATP":
        return "ATP", "bg-tour-atp"
    return "ITF", "bg-tour-itf"


def _serialize_browse_edition(
    edition: TournamentEdition,
    tournament: Tournament,
    *,
    status_key: str,
    completed_results: int = 0,
    final_count: int = 0,
    winner: dict | None = None,
    is_forecast_eligible: bool = False,
) -> dict:
    badge_label, badge_color = _badge_for_tournament(tournament)
    surface = edition.surface or tournament.surface
    location_bits = [value for value in (tournament.city, tournament.country) if value]
    return {
        "edition_id": edition.id,
        "tournament_name": tournament.name,
        "tour": tournament.tour,
        "gender": tournament.gender,
        "level": tournament.level,
        "surface": surface,
        "city": tournament.city,
        "country": tournament.country,
        "location": ", ".join(location_bits),
        "start_date": edition.start_date.isoformat() if edition.start_date else None,
        "end_date": edition.end_date.isoformat() if edition.end_date else None,
        "year": edition.year,
        "tournament_code": tournament.tournament_code,
        "status": status_key,
        "has_completed_matches": completed_results > 0,
        "completed_results": completed_results,
        "has_completed_final": final_count > 0,
        "url": (
            f"/tournaments/{tournament.tour.lower()}/{tournament.tournament_code}/{edition.year}"
            if tournament.tour
            else None
        ),
        "badge_label": badge_label,
        "badge_color": badge_color,
        "winner_name": winner["name"] if winner else None,
        "winner_url": (
            f"/players/{winner['id']}/{slugify_name(winner['name'])}"
            if winner and winner.get("id") is not None
            else None
        ),
        "is_forecast_eligible": is_forecast_eligible,
    }


def _parse_csv_values(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    return [part.strip() for part in raw_value.split(",") if part.strip()]


def _parse_year_values(raw_value: str | None) -> list[int]:
    values: list[int] = []
    for part in _parse_csv_values(raw_value):
        if part.isdigit():
            values.append(int(part))
    return values


def _tournament_status_case(match_flags_subquery, today_value: date):
    completed_results = func.coalesce(match_flags_subquery.c.completed_results, 0)
    finals = func.coalesce(match_flags_subquery.c.final_count, 0)
    is_current_window = and_(
        TournamentEdition.start_date.isnot(None),
        TournamentEdition.end_date.isnot(None),
        TournamentEdition.start_date <= today_value + timedelta(days=CURRENT_WINDOW_BEFORE_DAYS),
        TournamentEdition.end_date >= today_value - timedelta(days=CURRENT_WINDOW_AFTER_DAYS),
    )
    return case(
        (
            and_(is_current_window, completed_results > 0, finals == 0),
            "current",
        ),
        (
            and_(
                finals == 0,
                or_(
                    and_(
                        TournamentEdition.start_date.isnot(None),
                        TournamentEdition.start_date
                        > today_value + timedelta(days=CURRENT_WINDOW_BEFORE_DAYS),
                    ),
                    and_(is_current_window, completed_results == 0),
                ),
            ),
            "upcoming",
        ),
        else_="completed",
    )


def _get_tournament_edition_or_404(
    db: Session, tour: str, tournament_code: str, year: int
) -> TournamentEdition:
    edition = (
        db.query(TournamentEdition)
        .outerjoin(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .options(contains_eager(TournamentEdition.tournament))
        .filter(
            Tournament.tournament_code == tournament_code,
            Tournament.tour.in_(_resolve_tour_values(tour)),
            TournamentEdition.year == year,
        )
        .first()
    )
    if edition is None or edition.tournament is None:
        raise HTTPException(status_code=404, detail="Tournament edition not found")
    return edition


@router.get("/tournaments", response_class=HTMLResponse)
async def tournaments_page(
    request: Request,
    db: Session = Depends(get_db),
):
    available_years = [
        row[0]
        for row in (
            db.query(TournamentEdition.year)
            .distinct()
            .order_by(TournamentEdition.year.desc())
            .limit(16)
            .all()
        )
        if row[0] is not None
    ]
    year_chip_options = available_years[:TOURNAMENT_BROWSE_YEAR_CHIP_COUNT]
    return templates.TemplateResponse(
        "tournaments.html",
        {
            "request": request,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
            "level_filters": TOURNAMENT_LEVEL_FILTERS,
            "year_chip_options": year_chip_options,
            "available_years": available_years,
        },
    )


@router.get("/api/tournaments")
async def api_tournaments(
    db: Session = Depends(get_db),
    tour: str | None = Query(None),
    gender: str | None = Query(None),
    surface: str | None = Query(None),
    level: str | None = Query(None),
    year: str | None = Query(None),
    status: str | None = Query(None),
    tournament: str | None = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(24, ge=1, le=100),
):
    today_value = date.today()
    tour_values = _parse_csv_values(tour)
    expanded_tour_values = [
        resolved
        for value in tour_values
        for resolved in _resolve_tour_values(value)
    ]
    gender_values = [value.lower() for value in _parse_csv_values(gender)]
    surface_values = _parse_csv_values(surface)
    level_values = _parse_csv_values(level)
    year_values = _parse_year_values(year)
    status_values = [value.lower() for value in _parse_csv_values(status)]

    completed_statuses = get_status_group("historical_default")
    match_flags = (
        db.query(
            Match.tournament_edition_id.label("edition_id"),
            func.sum(
                case((Match.status.in_(completed_statuses), 1), else_=0)
            ).label("completed_results"),
            func.sum(
                case(
                    (and_(Match.round == "F", Match.winner_id.isnot(None)), 1),
                    else_=0,
                )
            ).label("final_count"),
        )
        .group_by(Match.tournament_edition_id)
        .subquery()
    )
    status_case = _tournament_status_case(match_flags, today_value)
    status_priority = case(
        (status_case == "current", 0),
        (status_case == "upcoming", 1),
        else_=2,
    )
    current_date_sort = case(
        (status_case == "current", TournamentEdition.end_date),
        else_=None,
    )
    upcoming_date_sort = case(
        (status_case == "upcoming", TournamentEdition.start_date),
        else_=None,
    )
    completed_date_sort = case(
        (status_case == "completed", TournamentEdition.end_date),
        else_=None,
    )
    level_priority = case(LEVEL_PRIORITY, value=Tournament.level, else_=9)

    def _apply_filters(query):
        query = query.join(Tournament, TournamentEdition.tournament_id == Tournament.id).outerjoin(
            match_flags, match_flags.c.edition_id == TournamentEdition.id
        )
        if expanded_tour_values:
            query = query.filter(Tournament.tour.in_(expanded_tour_values))
        if gender_values:
            query = query.filter(Tournament.gender.in_(gender_values))
        if surface_values:
            query = query.filter(
                func.coalesce(TournamentEdition.surface, Tournament.surface).in_(surface_values)
            )
        if level_values:
            query = query.filter(Tournament.level.in_(level_values))
        if year_values:
            query = query.filter(TournamentEdition.year.in_(year_values))
        if tournament:
            query = query.filter(Tournament.name.ilike(f"%{tournament.strip()}%"))
        if status_values:
            query = query.filter(status_case.in_(status_values))
        return query

    total = _apply_filters(db.query(func.count(TournamentEdition.id))).scalar() or 0

    offset = (page - 1) * per_page
    rows = (
        _apply_filters(
            db.query(
                TournamentEdition,
                Tournament,
                status_case.label("status_key"),
                func.coalesce(match_flags.c.completed_results, 0).label("completed_results"),
                func.coalesce(match_flags.c.final_count, 0).label("final_count"),
            )
        )
        .options(contains_eager(TournamentEdition.tournament))
        .order_by(
            status_priority.asc(),
            current_date_sort.asc().nullslast(),
            upcoming_date_sort.asc().nullslast(),
            completed_date_sort.desc().nullslast(),
            TournamentEdition.year.desc(),
            level_priority.asc(),
            Tournament.name.asc(),
        )
        .offset(offset)
        .limit(per_page)
        .all()
    )

    edition_ids = [edition.id for edition, _, _, _, _ in rows]
    winners_map: dict[int, dict] = {}
    if edition_ids:
        finals = (
            db.query(Match.tournament_edition_id, Player.id, Player.canonical_name)
            .join(Player, Match.winner_id == Player.id)
            .filter(
                Match.tournament_edition_id.in_(edition_ids),
                Match.round == "F",
                Match.winner_id.isnot(None),
            )
            .all()
        )
        winners_map = {
            edition_id: {"id": player_id, "name": player_name}
            for edition_id, player_id, player_name in finals
        }

    from teelo.services.tournament_forecast import is_forecast_eligible_tournament

    payload = [
        _serialize_browse_edition(
            edition,
            tournament_row,
            status_key=status_key,
            completed_results=completed_results,
            final_count=final_count,
            winner=winners_map.get(edition.id),
            is_forecast_eligible=(
                status_key in ("current", "upcoming") and is_forecast_eligible_tournament(tournament_row)
            ),
        )
        for edition, tournament_row, status_key, completed_results, final_count in rows
    ]

    return JSONResponse(
        {
            "tournaments": payload,
            "total": total,
            "page": page,
            "per_page": per_page,
            "has_more": (offset + per_page) < total,
        }
    )


@router.get("/api/editions/{edition_id}/favourite")
def api_edition_favourite(edition_id: int, db: Session = Depends(get_db)):
    from teelo.services.tournament_forecast import get_top_player
    top = get_top_player(db, edition_id=edition_id)
    if not top:
        return JSONResponse({})
    return JSONResponse({"name": top["name"], "win_pct": top["win_pct"]})


@router.get("/tournaments/{tour}/{tournament_code}")
async def tournament_latest_redirect(
    tour: str,
    tournament_code: str,
    db: Session = Depends(get_db),
):
    tour_key = _normalize_tour(tour)
    latest = (
        db.query(TournamentEdition.year)
        .outerjoin(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .filter(
            Tournament.tournament_code == tournament_code,
            Tournament.tour.in_(_resolve_tour_values(tour)),
        )
        .order_by(TournamentEdition.year.desc().nullslast())
        .first()
    )
    if latest is None or latest[0] is None:
        raise HTTPException(status_code=404, detail="Tournament not found")

    latest_year = latest[0]
    return RedirectResponse(url=f"/tournaments/{tour_key.lower()}/{tournament_code}/{latest_year}")


@router.get("/tournaments/{tour}/{tournament_code}/{year}", response_class=HTMLResponse)
async def tournament_detail_page(
    request: Request,
    tour: str,
    tournament_code: str,
    year: int,
    db: Session = Depends(get_db),
):
    edition = _get_tournament_edition_or_404(db, tour, tournament_code, year)
    tournament = edition.tournament
    edition_number = (
        db.query(func.count(TournamentEdition.id))
        .filter(TournamentEdition.tournament_id == tournament.id)
        .scalar()
        or 0
    )
    surface = edition.surface or tournament.surface

    # Combined query for has_draw, has_upcoming counts
    match_stats = (
        db.query(
            func.sum(case((Match.draw_position.isnot(None), 1), else_=0)).label("draw_count"),
            func.sum(
                case((Match.status.in_(("upcoming", "scheduled")), 1), else_=0)
            ).label("upcoming_count"),
        )
        .filter(Match.tournament_edition_id == edition.id)
        .first()
    )
    has_draw = (match_stats is not None and (match_stats.draw_count or 0) > 0)
    has_upcoming = (match_stats is not None and (match_stats.upcoming_count or 0) > 0)

    draw_size = edition.draw_size
    if has_draw:
        round_sort = case(ROUND_ORDER, value=Match.round, else_=999)
        first_draw_round = (
            db.query(Match.round, func.count(Match.id).label("match_count"))
            .filter(
                Match.tournament_edition_id == edition.id,
                Match.draw_position.isnot(None),
                Match.round.in_(MAIN_DRAW_ROUNDS),
            )
            .group_by(Match.round)
            .order_by(round_sort.asc())
            .first()
        )
        if first_draw_round is not None and first_draw_round.match_count:
            draw_size = first_draw_round.match_count * 2

    final_match = (
        db.query(Match)
        .options(joinedload(Match.winner), joinedload(Match.player_a), joinedload(Match.player_b))
        .filter(Match.tournament_edition_id == edition.id, Match.round == "F")
        .order_by(
            func.coalesce(Match.match_date, Match.scheduled_date).desc().nullslast(),
            Match.id.desc(),
        )
        .first()
    )
    champion_name = None
    champion_score = None
    if final_match is not None:
        champion_score = final_match.score
        if final_match.winner is not None:
            champion_name = final_match.winner.canonical_name
        elif final_match.winner_id == final_match.player_a_id and final_match.player_a is not None:
            champion_name = final_match.player_a.canonical_name
        elif final_match.winner_id == final_match.player_b_id and final_match.player_b is not None:
            champion_name = final_match.player_b.canonical_name

    editions = (
        db.query(TournamentEdition)
        .filter(TournamentEdition.tournament_id == tournament.id)
        .order_by(TournamentEdition.year.desc())
        .all()
    )
    finals_by_edition: dict[int, Match] = {}
    edition_ids = [item.id for item in editions]
    if edition_ids:
        finals = (
            db.query(Match)
            .filter(Match.tournament_edition_id.in_(edition_ids), Match.round == "F")
            .options(
                joinedload(Match.winner),
                joinedload(Match.player_a),
                joinedload(Match.player_b),
            )
            .order_by(
                Match.tournament_edition_id.asc(),
                func.coalesce(Match.match_date, Match.scheduled_date).desc().nullslast(),
                Match.id.desc(),
            )
            .all()
        )
        for final in finals:
            if final.tournament_edition_id not in finals_by_edition:
                finals_by_edition[final.tournament_edition_id] = final

    years = [edition.year for edition in _filter_editions_with_finals(editions, finals_by_edition)]

    from teelo.services.tournament_forecast import is_forecast_eligible_tournament, get_active_run
    has_forecast = (
        is_forecast_eligible_tournament(tournament)
        and get_active_run(db, edition.id) is not None
    )

    return templates.TemplateResponse(
        "tournament_detail.html",
        {
            "request": request,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
            "tournament": tournament,
            "tour": (
                tournament.tour.lower()
                if tournament and tournament.tour
                else (tour or "").lower()
            ),
            "edition": edition,
            "edition_number": edition_number,
            "surface": surface,
            "draw_size": draw_size,
            "has_draw": has_draw,
            "has_upcoming": has_upcoming,
            "has_forecast": has_forecast,
            "champion": {"name": champion_name, "score": champion_score},
            "years": years,
        },
    )


@router.get("/api/tournaments/{tour}/{tournament_code}/{year}/matches")
async def api_tournament_matches(
    tour: str,
    tournament_code: str,
    year: int,
    status: str = Query("completed"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
):
    edition = _get_tournament_edition_or_404(db, tour, tournament_code, year)
    status_key = (status or "completed").strip().lower()
    if status_key not in {"completed", "upcoming"}:
        status_key = "completed"
    statuses = list(
        get_status_group("historical_default" if status_key == "completed" else "upcoming")
    )

    count_q = db.query(func.count(Match.id)).filter(
        Match.tournament_edition_id == edition.id,
        Match.status.in_(statuses),
    )
    total = count_q.scalar() or 0

    round_sort = case(ROUND_ORDER, value=Match.round, else_=999)
    offset = (page - 1) * per_page
    fetch_q = (
        db.query(Match)
        .filter(Match.tournament_edition_id == edition.id, Match.status.in_(statuses))
        .options(
            joinedload(Match.player_a),
            joinedload(Match.player_b),
            joinedload(Match.tournament_edition).joinedload(TournamentEdition.tournament),
        )
    )
    if status_key == "completed":
        fetch_q = fetch_q.order_by(
            round_sort.desc(),
            func.coalesce(
                Match.match_datetime,
                Match.scheduled_datetime,
                func.cast(
                    func.coalesce(Match.match_date, Match.scheduled_date),
                    Match.match_datetime.type,
                ),
            ).desc().nullslast(),
            Match.id.desc(),
        )
    else:
        fetch_q = fetch_q.order_by(*upcoming_sort_expressions())

    matches = fetch_q.offset(offset).limit(per_page).all()
    payload = [serialize_match(match) for match in matches]
    t = templates.get_template("partials/match_rows.html")
    return JSONResponse(
        {
            "matches": payload,
            "table_rows_html": t.module.render_table_rows(payload),
            "cards_html": t.module.render_cards(payload),
            "total": total,
            "page": page,
            "per_page": per_page,
            "has_more": (offset + per_page) < total,
        }
    )


@router.get("/api/tournaments/{tour}/{tournament_code}/{year}/draw")
async def api_tournament_draw(
    tour: str,
    tournament_code: str,
    year: int,
    db: Session = Depends(get_db),
):
    edition = _get_tournament_edition_or_404(db, tour, tournament_code, year)
    round_sort = case(ROUND_ORDER, value=Match.round, else_=999)
    draw_matches = (
        db.query(Match)
        .filter(
            Match.tournament_edition_id == edition.id,
            Match.draw_position.isnot(None),
            Match.round.in_(MAIN_DRAW_ROUNDS),
        )
        .options(joinedload(Match.player_a), joinedload(Match.player_b))
        .order_by(round_sort.asc(), Match.draw_position.asc(), Match.id.asc())
        .all()
    )
    if not draw_matches:
        return JSONResponse({"has_draw": False, "rounds": []})

    grouped: dict[str, list[dict]] = {}
    for match in draw_matches:
        if match.round is None or match.draw_position is None:
            continue
        player_a = {
            "id": match.player_a.id if match.player_a else match.player_a_id,
            "name": match.player_a.canonical_name if match.player_a else "TBD",
            "seed": match.player_a_seed,
        }
        player_b = {
            "id": match.player_b.id if match.player_b else match.player_b_id,
            "name": match.player_b.canonical_name if match.player_b else "TBD",
            "seed": match.player_b_seed,
        }
        grouped.setdefault(match.round, []).append(
            {
                "id": match.id,
                "round": match.round,
                "draw_position": match.draw_position,
                "player_a": player_a,
                "player_b": player_b,
                "score": match.score,
                "status": match.status,
                "winner_id": match.winner_id,
                "next_slot": None if match.round == "F" else math.ceil(match.draw_position / 2),
            }
        )

    rounds_payload: list[dict] = []
    for round_code in sorted(grouped.keys(), key=lambda rc: ROUND_ORDER.get(rc, 999)):
        rounds_payload.append(
            {
                "round": round_code,
                "label": ROUND_LABELS.get(round_code, round_code),
                "matches": sorted(grouped[round_code], key=lambda m: m["draw_position"]),
            }
        )

    return JSONResponse({"has_draw": bool(rounds_payload), "rounds": rounds_payload})


@router.get("/api/tournaments/{tour}/{tournament_code}/{year}/forecast")
async def api_tournament_forecast(
    tour: str,
    tournament_code: str,
    year: int,
    db: Session = Depends(get_db),
):
    edition = _get_tournament_edition_or_404(db, tour, tournament_code, year)
    tournament = (
        db.query(Tournament)
        .filter(Tournament.id == edition.tournament_id)
        .first()
    )
    if tournament is None:
        raise HTTPException(status_code=404, detail="Tournament not found")

    from teelo.services.tournament_forecast import compute_probabilities, is_forecast_eligible_tournament

    if not is_forecast_eligible_tournament(tournament):
        return JSONResponse({"has_forecast": False, "status": "unsupported"})

    payload = compute_probabilities(db, edition_id=edition.id)
    return JSONResponse(payload)


@router.get("/api/tournaments/{tour}/{tournament_code}/editions")
async def api_tournament_editions(
    tour: str,
    tournament_code: str,
    db: Session = Depends(get_db),
):
    tour_key = _normalize_tour(tour)
    tournament = (
        db.query(Tournament)
        .filter(
            Tournament.tournament_code == tournament_code,
            Tournament.tour.in_(_resolve_tour_values(tour)),
        )
        .first()
    )
    if tournament is None:
        raise HTTPException(status_code=404, detail="Tournament not found")

    editions = (
        db.query(TournamentEdition)
        .filter(TournamentEdition.tournament_id == tournament.id)
        .order_by(TournamentEdition.year.desc())
        .all()
    )
    edition_ids = [edition.id for edition in editions]
    finals_by_edition: dict[int, Match] = {}
    if edition_ids:
        finals = (
            db.query(Match)
            .filter(Match.tournament_edition_id.in_(edition_ids), Match.round == "F")
            .options(
                joinedload(Match.winner),
                joinedload(Match.player_a),
                joinedload(Match.player_b),
            )
            .order_by(
                Match.tournament_edition_id.asc(),
                func.coalesce(Match.match_date, Match.scheduled_date).desc().nullslast(),
                Match.id.desc(),
            )
            .all()
        )
        for final in finals:
            if final.tournament_edition_id not in finals_by_edition:
                finals_by_edition[final.tournament_edition_id] = final

    filtered_editions = _filter_editions_with_finals(editions, finals_by_edition)
    payload = _build_edition_history_payload(
        editions=filtered_editions,
        finals_by_edition=finals_by_edition,
        tournament=tournament,
        tour_key=tour_key,
        tournament_code=tournament_code,
    )

    return JSONResponse({"editions": payload})
