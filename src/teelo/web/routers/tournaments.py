from datetime import datetime
import math

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import case, func
from sqlalchemy.orm import Session, contains_eager, joinedload

from teelo.db.models import ROUND_ORDER, Match, Tournament, TournamentEdition
from teelo.db.session import get_db
from teelo.match_statuses import get_status_group
from teelo.web.app_context import templates
from teelo.web.services.match_service import serialize_match

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


def _get_tournament_edition_or_404(
    db: Session, tour: str, tournament_code: str, year: int
) -> TournamentEdition:
    tour_key = _normalize_tour(tour)
    edition = (
        db.query(TournamentEdition)
        .outerjoin(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .options(contains_eager(TournamentEdition.tournament))
        .filter(
            Tournament.tournament_code == tournament_code,
            func.upper(Tournament.tour) == tour_key,
            TournamentEdition.year == year,
        )
        .first()
    )
    if edition is None or edition.tournament is None:
        raise HTTPException(status_code=404, detail="Tournament edition not found")
    return edition


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
            func.upper(Tournament.tour) == tour_key,
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

    has_draw = (
        db.query(Match.id)
        .filter(Match.tournament_edition_id == edition.id, Match.draw_position.isnot(None))
        .first()
        is not None
    )
    has_upcoming = (
        db.query(Match.id)
        .filter(
            Match.tournament_edition_id == edition.id,
            Match.status.in_(("upcoming", "scheduled")),
        )
        .first()
        is not None
    )
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
    draw_size = edition.draw_size
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

    return templates.TemplateResponse(
        "tournament_detail.html",
        {
            "request": request,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
            "tournament": tournament,
            "tour": tournament.tour.lower() if tournament and tournament.tour else (tour or "").lower(),
            "edition": edition,
            "edition_number": edition_number,
            "surface": surface,
            "draw_size": draw_size,
            "has_draw": has_draw,
            "has_upcoming": has_upcoming,
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
            Match.match_date.desc().nullslast(),
            Match.id.desc(),
        )
    else:
        fetch_q = fetch_q.order_by(
            func.coalesce(Match.scheduled_date, Match.match_date).asc().nullslast(),
            round_sort.asc(),
            Match.id.asc(),
        )

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
            func.upper(Tournament.tour) == tour_key,
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
