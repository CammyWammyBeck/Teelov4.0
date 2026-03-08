from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import and_, case, func, or_
from sqlalchemy.orm import Session, contains_eager, defer, joinedload

from teelo.config import settings
from teelo.db.models import Match, Player, PlayerEloState, Tournament, TournamentEdition
from teelo.db.session import get_db
from teelo.match_statuses import get_status_group
from teelo.web.app_context import MATCHES_PAGE_STATUS_FILTERS, templates
from teelo.web.services.match_service import serialize_match, slugify_name

router = APIRouter()


def require_feature(feature_flag: str):
    def check_feature(request: Request):
        if not getattr(settings, feature_flag, False):
            if feature_flag == "enable_feature_matches" and request.url.path == "/":
                return RedirectResponse(url="/blog")
            raise HTTPException(status_code=404, detail="Feature not enabled")
    return check_feature


def _home_base_query(db: Session):
    home_scope_filter = or_(
        Tournament.level == "Grand Slam",
        Tournament.tour.in_(["ATP", "WTA"]),
        and_(
            Tournament.tour.in_(["CHALLENGER", "Challenger", "WTA 125", "WTA_125"]),
            Match.round.in_(["SF", "F"]),
        ),
        and_(Tournament.tour == "ITF", Match.round == "F"),
    )
    return (
        db.query(Match)
        .outerjoin(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .outerjoin(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .options(
            # Use contains_eager for tables already joined (avoids duplicate joins)
            contains_eager(Match.tournament_edition).contains_eager(TournamentEdition.tournament),
            # Players still need joinedload (not part of the explicit joins)
            joinedload(Match.player_a),
            joinedload(Match.player_b),
            # Skip heavy columns not needed by serialize_match
            defer(Match.stats),
            defer(Match.score_structured),
        )
        .filter(home_scope_filter)
    )


@router.get("/api/search")
async def api_search(
    q: str = Query("", min_length=0),
    limit: int = Query(4, ge=1, le=50),
    full: bool = Query(False),
    db: Session = Depends(get_db),
):
    query = (q or "").strip()
    if len(query) < 2:
        return JSONResponse(
            {
                "players": [],
                "tournaments": [],
                "player_count": 0,
                "player_has_more": False,
                "tournament_count": 0,
                "tournament_has_more": False,
            }
        )

    contains_pattern = f"%{query}%"
    starts_with_pattern = f"{query}%"
    word_starts_with_pattern = f"% {query}%"

    player_match_rank = case(
        (or_(Player.canonical_name.ilike(starts_with_pattern), Player.canonical_name.ilike(word_starts_with_pattern)), 0),
        else_=1,
    )
    tournament_match_rank = case(
        (or_(Tournament.name.ilike(starts_with_pattern), Tournament.name.ilike(word_starts_with_pattern)), 0),
        (or_(Tournament.city.ilike(starts_with_pattern), Tournament.city.ilike(word_starts_with_pattern)), 1),
        (or_(Tournament.country.ilike(starts_with_pattern), Tournament.country.ilike(word_starts_with_pattern)), 2),
        (Tournament.name.ilike(contains_pattern), 3),
        (Tournament.city.ilike(contains_pattern), 4),
        (Tournament.country.ilike(contains_pattern), 5),
        else_=6,
    )
    tournament_level_priority = case(
        (Tournament.level == "Grand Slam", 1),
        (Tournament.level == "Masters 1000", 2),
        (Tournament.level == "WTA 1000", 2),
        (Tournament.level == "ATP 500", 3),
        (Tournament.level == "WTA 500", 3),
        (Tournament.level == "ATP 250", 4),
        (Tournament.level == "WTA 250", 4),
        (Tournament.level == "WTA 125", 5),
        (Tournament.level == "Challenger", 6),
        (Tournament.level == "ITF", 7),
        else_=8,
    )
    player_filter = Player.canonical_name.ilike(contains_pattern)
    tournament_filter = or_(
        Tournament.name.ilike(contains_pattern),
        Tournament.city.ilike(contains_pattern),
        Tournament.country.ilike(contains_pattern),
    )

    effective_limit = 50 if full else limit
    query_limit = effective_limit + 1
    player_rows = (
        db.query(Player)
        .outerjoin(PlayerEloState, Player.id == PlayerEloState.player_id)
        .filter(player_filter)
        .order_by(
            player_match_rank.asc(),
            PlayerEloState.rating.desc().nullslast(),
            func.length(Player.canonical_name).asc(),
            Player.canonical_name.asc(),
        )
        .limit(query_limit)
        .all()
    )
    tournament_rows = (
        db.query(Tournament)
        .filter(tournament_filter)
        .order_by(
            tournament_match_rank.asc(),
            tournament_level_priority.asc(),
            func.length(Tournament.name).asc(),
            Tournament.name.asc(),
        )
        .limit(query_limit)
        .all()
    )
    has_more_players = len(player_rows) > effective_limit
    has_more_tournaments = len(tournament_rows) > effective_limit
    players = player_rows[:effective_limit]
    tournaments = tournament_rows[:effective_limit]

    player_payload = []
    for player in players:
        gender_key = (player.gender or "").strip().lower()
        tour_label = "WTA" if gender_key == "women" else "ATP"
        player_payload.append(
            {
                "id": player.id,
                "name": player.canonical_name,
                "tour": tour_label,
                "nationality_ioc": player.nationality_ioc,
                "url": f"/players/{player.id}/{slugify_name(player.canonical_name)}",
            }
        )

    tournament_payload = []
    for tournament in tournaments:
        tour_value = (tournament.tour or "").strip()
        tournament_payload.append(
            {
                "tournament_code": tournament.tournament_code,
                "name": tournament.name,
                "tour": tour_value,
                "level": tournament.level,
                "city": tournament.city,
                "country": tournament.country,
                "url": f"/tournaments/{tour_value.lower()}/{tournament.tournament_code}",
            }
        )

    return JSONResponse(
        {
            "players": player_payload,
            "tournaments": tournament_payload,
            "player_count": len(player_payload),
            "player_has_more": has_more_players,
            "tournament_count": len(tournament_payload),
            "tournament_has_more": has_more_tournaments,
        }
    )


@router.get("/api/home/upcoming")
async def home_api_upcoming(
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    upcoming = (
        _home_base_query(db)
        .filter(Match.status.in_(get_status_group("upcoming")))
        .order_by(
            func.coalesce(Match.scheduled_date, Match.match_date).asc().nullslast(),
            Match.scheduled_datetime.asc().nullslast(),
            Match.id.asc(),
        )
        .limit(10)
        .all()
    )
    upcoming_matches = [serialize_match(m) for m in upcoming]
    match_rows_module = templates.get_template("partials/match_rows.html").module
    return JSONResponse(
        {
            "matches": upcoming_matches,
            "table_html": match_rows_module.render_table_rows(upcoming_matches),
            "cards_html": match_rows_module.render_cards(upcoming_matches),
        }
    )


@router.get("/api/home/completed")
async def home_api_completed(
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    completed = (
        _home_base_query(db)
        .filter(Match.status.in_(get_status_group("historical_default")))
        .order_by(
            func.coalesce(Match.match_date, Match.scheduled_date).desc().nullslast(),
            Match.id.desc(),
        )
        .limit(10)
        .all()
    )
    completed_matches = [serialize_match(m) for m in completed]
    match_rows_module = templates.get_template("partials/match_rows.html").module
    return JSONResponse(
        {
            "matches": completed_matches,
            "table_html": match_rows_module.render_table_rows(completed_matches),
            "cards_html": match_rows_module.render_cards(completed_matches),
        }
    )


@router.get("/api/home/stats")
async def home_api_stats(
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    return JSONResponse(
        {
            "matches_total": db.query(func.count(Match.id)).scalar() or 0,
            "players_total": db.query(func.count(Player.id)).scalar() or 0,
            "editions_total": db.query(func.count(TournamentEdition.id)).scalar() or 0,
        }
    )


@router.get("/api/home")
async def home_api(
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    home_base_query = _home_base_query(db)
    upcoming = (
        home_base_query
        .filter(Match.status.in_(get_status_group("upcoming")))
        .order_by(
            func.coalesce(Match.scheduled_date, Match.match_date).asc().nullslast(),
            Match.scheduled_datetime.asc().nullslast(),
            Match.id.asc(),
        )
        .limit(10)
        .all()
    )
    completed = (
        home_base_query
        .filter(Match.status.in_(get_status_group("historical_default")))
        .order_by(
            func.coalesce(Match.match_date, Match.scheduled_date).desc().nullslast(),
            Match.id.desc(),
        )
        .limit(10)
        .all()
    )
    upcoming_matches = [serialize_match(m) for m in upcoming]
    completed_matches = [serialize_match(m) for m in completed]

    match_rows_template = templates.get_template("partials/match_rows.html")
    match_rows_module = match_rows_template.module

    return JSONResponse(
        {
            "stats": {
                "matches_total": db.query(func.count(Match.id)).scalar() or 0,
                "players_total": db.query(func.count(Player.id)).scalar() or 0,
                "editions_total": db.query(func.count(TournamentEdition.id)).scalar() or 0,
            },
            "upcoming": upcoming_matches,
            "completed": completed_matches,
            "upcoming_table_html": match_rows_module.render_table_rows(upcoming_matches),
            "upcoming_cards_html": match_rows_module.render_cards(upcoming_matches),
            "completed_table_html": match_rows_module.render_table_rows(completed_matches),
            "completed_cards_html": match_rows_module.render_cards(completed_matches),
        }
    )


@router.get("/", response_class=HTMLResponse)
async def home(
    request: Request,
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    if not settings.enable_feature_matches:
        return RedirectResponse(url="/blog")
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
        },
    )


@router.get("/search", response_class=HTMLResponse)
async def search_page(request: Request, q: str = Query("")):
    return templates.TemplateResponse(
        "search_results.html",
        {
            "request": request,
            "q": (q or "").strip(),
            "now": datetime.utcnow(),
            "current_path": request.url.path,
        },
    )


@router.get("/matches", response_class=HTMLResponse)
async def matches_page(request: Request, _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches"))):
    return templates.TemplateResponse("matches.html", {"request": request, "status_filters": MATCHES_PAGE_STATUS_FILTERS, "now": datetime.utcnow(), "current_path": request.url.path})
