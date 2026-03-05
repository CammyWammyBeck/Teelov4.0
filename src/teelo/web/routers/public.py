from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session, joinedload

from teelo.config import settings
from teelo.db.models import Match, Player, Tournament, TournamentEdition
from teelo.db.session import get_db
from teelo.match_statuses import get_status_group
from teelo.web.app_context import MATCHES_PAGE_STATUS_FILTERS, templates
from teelo.web.services.match_service import serialize_match

router = APIRouter()


def require_feature(feature_flag: str):
    def check_feature(request: Request):
        if not getattr(settings, feature_flag, False):
            if feature_flag == "enable_feature_matches" and request.url.path == "/":
                return RedirectResponse(url="/blog")
            raise HTTPException(status_code=404, detail="Feature not enabled")
    return check_feature


@router.get("/api/home")
async def home_api(
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    home_scope_filter = or_(Tournament.level == "Grand Slam", Tournament.tour.in_(["ATP", "WTA"]), and_(Tournament.tour.in_(["CHALLENGER", "Challenger", "WTA 125", "WTA_125"]), Match.round.in_(["SF", "F"])), and_(Tournament.tour == "ITF", Match.round == "F"))
    home_base_query = db.query(Match).outerjoin(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id).outerjoin(Tournament, TournamentEdition.tournament_id == Tournament.id).options(joinedload(Match.player_a), joinedload(Match.player_b), joinedload(Match.tournament_edition).joinedload(TournamentEdition.tournament)).filter(home_scope_filter)
    upcoming = home_base_query.filter(Match.status.in_(get_status_group("upcoming"))).order_by(func.coalesce(Match.scheduled_date, Match.match_date).asc().nullslast(), Match.scheduled_datetime.asc().nullslast(), Match.id.asc()).limit(10).all()
    completed = home_base_query.filter(Match.status.in_(get_status_group("historical_default"))).order_by(func.coalesce(Match.match_date, Match.scheduled_date).desc().nullslast(), Match.id.desc()).limit(10).all()
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


@router.get("/matches", response_class=HTMLResponse)
async def matches_page(request: Request, _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches"))):
    return templates.TemplateResponse("matches.html", {"request": request, "status_filters": MATCHES_PAGE_STATUS_FILTERS, "now": datetime.utcnow(), "current_path": request.url.path})
