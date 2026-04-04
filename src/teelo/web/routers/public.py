import time
from datetime import date, datetime, timedelta
from typing import Any, Optional

import frontmatter
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import and_, case, func, or_, union_all
from sqlalchemy.orm import Session, aliased, contains_eager, defer

from teelo.config import settings
from teelo.db.models import Match, ModelEvaluationSnapshot, Player, PlayerEloState, Tournament, TournamentEdition
from teelo.db.session import get_db
from teelo.match_statuses import get_status_group
from teelo.web.app_context import CONTENT_PATH, MATCHES_PAGE_STATUS_FILTERS, templates
from teelo.web.services.match_service import serialize_match, slugify_name, upcoming_sort_expressions, completed_sort_expressions

router = APIRouter()

_stats_cache: dict | None = None
_stats_cache_time: float = 0.0
_rankings_cache: dict | None = None
_rankings_cache_time: float = 0.0
_tournaments_cache: dict | None = None
_tournaments_cache_time: float = 0.0
_movers_cache: dict | None = None
_movers_cache_time: float = 0.0
_blog_cache: dict | None | bool = False  # False = not cached, None = cached empty
_blog_cache_time: float = 0.0
_STATS_CACHE_TTL: float = 300.0  # 5 minutes


def require_feature(feature_flag: str):
    def check_feature(request: Request):
        if not getattr(settings, feature_flag, False):
            if feature_flag == "enable_feature_matches" and request.url.path == "/":
                return RedirectResponse(url="/blog")
            raise HTTPException(status_code=404, detail="Feature not enabled")
    return check_feature


def _home_base_query(db: Session):
    home_scope_filter = Tournament.level.in_([
        "Grand Slam", "Masters 1000", "WTA 1000",
        "ATP 500", "WTA 500", "ATP 250", "WTA 250",
    ])
    PlayerA = aliased(Player, flat=True)
    PlayerB = aliased(Player, flat=True)

    return (
        db.query(Match)
        .outerjoin(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .outerjoin(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .outerjoin(PlayerA, Match.player_a_id == PlayerA.id)
        .outerjoin(PlayerB, Match.player_b_id == PlayerB.id)
        .options(
            # Use contains_eager for all already-joined tables (avoids extra SELECT queries)
            contains_eager(Match.tournament_edition).contains_eager(TournamentEdition.tournament),
            contains_eager(Match.player_a, alias=PlayerA),
            contains_eager(Match.player_b, alias=PlayerB),
            # Skip heavy columns not needed by serialize_match
            defer(Match.stats),
            defer(Match.score_structured),
        )
        .filter(home_scope_filter)
    )


@router.get("/api/search")
def api_search(
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
def home_api_upcoming(
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    upcoming = (
        _home_base_query(db)
        .filter(Match.status.in_(get_status_group("upcoming")))
        .order_by(*upcoming_sort_expressions())
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
def home_api_completed(
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    completed = (
        _home_base_query(db)
        .filter(Match.status.in_(get_status_group("historical_default")))
        .order_by(*completed_sort_expressions())
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


def _get_prediction_accuracy(db: Session) -> Optional[float]:
    """Return the accuracy from the latest ModelEvaluationSnapshot, or None."""
    snapshot = (
        db.query(ModelEvaluationSnapshot)
        .order_by(ModelEvaluationSnapshot.snapshot_date.desc())
        .first()
    )
    if snapshot is None or not snapshot.metrics:
        return None
    accuracy = snapshot.metrics.get("accuracy")
    if accuracy is None:
        return None
    return round(float(accuracy) * 100, 1)


@router.get("/api/home/stats")
def home_api_stats(
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    global _stats_cache, _stats_cache_time
    now = time.monotonic()
    if _stats_cache is not None and (now - _stats_cache_time) < _STATS_CACHE_TTL:
        return JSONResponse(_stats_cache)

    row = db.query(
        db.query(func.count(Match.id)).scalar_subquery().label("matches"),
        db.query(func.count(Player.id)).scalar_subquery().label("players"),
        db.query(func.count(TournamentEdition.id)).scalar_subquery().label("editions"),
        db.query(func.count(Player.nationality_ioc.distinct())).scalar_subquery().label("countries"),
        db.query(func.min(Match.match_date)).scalar_subquery().label("min_date"),
    ).one()

    days_of_data = (date.today() - row.min_date).days if row.min_date else 0

    stats = {
        "matches_total": row.matches or 0,
        "players_total": row.players or 0,
        "editions_total": row.editions or 0,
        "countries_total": row.countries or 0,
        "prediction_accuracy_pct": _get_prediction_accuracy(db),
        "days_of_data": days_of_data,
    }
    _stats_cache = stats
    _stats_cache_time = now
    return JSONResponse(stats)


@router.get("/api/home/rankings")
def home_api_rankings(
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    global _rankings_cache, _rankings_cache_time
    now = time.monotonic()
    if _rankings_cache is not None and (now - _rankings_cache_time) < _STATS_CACHE_TTL:
        return JSONResponse(_rankings_cache)

    atp_q = (
        db.query(
            Player.id.label("player_id"),
            Player.canonical_name.label("name"),
            Player.nationality_ioc,
            Player.gender,
            PlayerEloState.rating,
        )
        .join(PlayerEloState, PlayerEloState.player_id == Player.id)
        .filter(Player.gender == "men")
        .order_by(PlayerEloState.rating.desc())
        .limit(5)
    )
    wta_q = (
        db.query(
            Player.id.label("player_id"),
            Player.canonical_name.label("name"),
            Player.nationality_ioc,
            Player.gender,
            PlayerEloState.rating,
        )
        .join(PlayerEloState, PlayerEloState.player_id == Player.id)
        .filter(Player.gender == "women")
        .order_by(PlayerEloState.rating.desc())
        .limit(5)
    )
    rows = atp_q.union_all(wta_q).all()

    atp: list[dict] = []
    wta: list[dict] = []
    for row in rows:
        entry = {
            "rank": len(atp if row.gender == "men" else wta) + 1,
            "player_id": row.player_id,
            "name": row.name,
            "nationality_ioc": row.nationality_ioc,
            "rating": round(float(row.rating)),
            "url": f"/players/{row.player_id}/{slugify_name(row.name)}",
        }
        if row.gender == "men":
            atp.append(entry)
        else:
            wta.append(entry)

    payload = {
        "atp": atp,
        "wta": wta,
    }
    _rankings_cache = payload
    _rankings_cache_time = now
    return JSONResponse(payload)


LEVEL_PRIORITY = {
    "Grand Slam": 1, "Masters 1000": 2, "WTA 1000": 2,
    "ATP 500": 3, "WTA 500": 3, "ATP 250": 4, "WTA 250": 4,
}


def _badge_for_tournament(t: Tournament) -> tuple[str, str]:
    """Return (label, bg_color) matching the match_rows.html badge pattern."""
    if t.level == "Grand Slam":
        return "GSL", "bg-tour-grandslam"
    tour = (t.tour or "").strip()
    if tour in ("CHALLENGER", "Challenger"):
        return "CHL", "bg-tour-challenger"
    if tour in ("WTA 125", "WTA_125") or t.level in ("WTA 125", "WTA_125"):
        return "125", "bg-tour-250"
    if tour == "WTA":
        return "WTA", "bg-tour-wta"
    if tour == "ATP":
        return "ATP", "bg-tour-atp"
    return "UNK", "bg-tour-itf"


@router.get("/api/home/tournaments")
def home_api_tournaments(
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    global _tournaments_cache, _tournaments_cache_time
    now = time.monotonic()
    if _tournaments_cache is not None and (now - _tournaments_cache_time) < _STATS_CACHE_TTL:
        return JSONResponse(_tournaments_cache)

    today = date.today()

    candidate_rows = (
        db.query(TournamentEdition, Tournament)
        .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .filter(
            Tournament.level.in_([
                "Grand Slam", "Masters 1000", "WTA 1000",
                "ATP 500", "WTA 500", "ATP 250", "WTA 250",
            ]),
            TournamentEdition.start_date <= today + timedelta(days=14),
            TournamentEdition.end_date >= today - timedelta(days=30),
        )
        .all()
    )

    def _serialize_edition(te: TournamentEdition, t: Tournament, winner: Optional[dict] = None, is_forecast_eligible: bool = False) -> dict:
        surface = te.surface or t.surface
        badge_label, badge_color = _badge_for_tournament(t)
        result = {
            "edition_id": te.id,
            "tournament_name": t.name,
            "level": t.level,
            "surface": surface,
            "city": t.city,
            "start_date": te.start_date.isoformat() if te.start_date else None,
            "end_date": te.end_date.isoformat() if te.end_date else None,
            "year": te.year,
            "tournament_code": t.tournament_code,
            "tour": t.tour,
            "url": f"/tournaments/{t.tour.lower()}/{t.tournament_code}/{te.year}" if t.tour else None,
            "badge_label": badge_label,
            "badge_color": badge_color,
            "is_forecast_eligible": is_forecast_eligible,
        }
        if winner:
            result["winner_name"] = winner["name"]
            result["winner_id"] = winner["id"]
            result["winner_url"] = f"/players/{winner['id']}/{slugify_name(winner['name'])}"
        return result

    candidate_ids = [te.id for te, t in candidate_rows]

    completed_statuses = get_status_group("historical_default")

    editions_with_results: set[int] = set()
    editions_with_final: set[int] = set()
    if candidate_ids:
        edition_flags = (
            db.query(
                Match.tournament_edition_id,
                func.sum(case((Match.status.in_(completed_statuses), 1), else_=0)).label("has_results"),
                func.sum(case(
                    (and_(Match.round == "F", Match.winner_id.isnot(None)), 1),
                    else_=0,
                )).label("has_final"),
            )
            .filter(Match.tournament_edition_id.in_(candidate_ids))
            .group_by(Match.tournament_edition_id)
            .all()
        )
        for row in edition_flags:
            if row.has_results:
                editions_with_results.add(row.tournament_edition_id)
            if row.has_final:
                editions_with_final.add(row.tournament_edition_id)

    in_progress: list[tuple[TournamentEdition, Tournament]] = []
    completed: list[tuple[TournamentEdition, Tournament]] = []
    upcoming: list[tuple[TournamentEdition, Tournament]] = []

    for te, t in candidate_rows:
        if te.id in editions_with_final:
            completed.append((te, t))
        elif te.id in editions_with_results:
            in_progress.append((te, t))
        else:
            buffer_days = 7 if t.level == "Grand Slam" else 3
            if te.start_date and te.start_date <= today + timedelta(days=buffer_days):
                if te.end_date and te.end_date >= today - timedelta(days=1):
                    upcoming.append((te, t))
            elif te.start_date and te.start_date > today:
                upcoming.append((te, t))

    in_progress.sort(key=lambda x: LEVEL_PRIORITY.get(x[1].level, 7))
    upcoming.sort(key=lambda x: x[0].start_date or date.max)
    completed.sort(key=lambda x: x[0].end_date or date.min, reverse=True)

    in_progress = in_progress[:5]
    upcoming = upcoming[:5]
    completed = completed[:5]

    edition_ids = [te.id for te, t in completed]
    if edition_ids:
        finals = (
            db.query(Match.tournament_edition_id, Player.canonical_name, Player.id)
            .join(Player, Match.winner_id == Player.id)
            .filter(
                Match.tournament_edition_id.in_(edition_ids),
                Match.round == "F",
                Match.winner_id.isnot(None),
            )
            .all()
        )
        winners_map = {f.tournament_edition_id: {"name": f[1], "id": f[2]} for f in finals}
    else:
        winners_map = {}

    from teelo.services.tournament_forecast import is_forecast_eligible_tournament

    payload = {
        "in_progress": [_serialize_edition(te, t, is_forecast_eligible=is_forecast_eligible_tournament(t)) for te, t in in_progress],
        "upcoming": [_serialize_edition(te, t, is_forecast_eligible=is_forecast_eligible_tournament(t)) for te, t in upcoming],
        "recently_completed": [_serialize_edition(te, t, winners_map.get(te.id)) for te, t in completed],
    }
    _tournaments_cache = payload
    _tournaments_cache_time = now
    return JSONResponse(payload)


@router.get("/api/home/movers")
def home_api_movers(
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    global _movers_cache, _movers_cache_time
    now = time.monotonic()
    if _movers_cache is not None and (now - _movers_cache_time) < _STATS_CACHE_TTL:
        return JSONResponse(_movers_cache)

    cutoff = date.today() - timedelta(days=90)
    completed_statuses = get_status_group("historical_default")

    gain_a = (
        db.query(
            Match.player_a_id.label("player_id"),
            func.sum(Match.elo_post_player_a - Match.elo_pre_player_a).label("gain"),
        )
        .filter(
            Match.match_date >= cutoff,
            Match.status.in_(completed_statuses),
            Match.elo_pre_player_a.isnot(None),
            Match.elo_post_player_a.isnot(None),
        )
        .group_by(Match.player_a_id)
    )

    gain_b = (
        db.query(
            Match.player_b_id.label("player_id"),
            func.sum(Match.elo_post_player_b - Match.elo_pre_player_b).label("gain"),
        )
        .filter(
            Match.match_date >= cutoff,
            Match.status.in_(completed_statuses),
            Match.elo_pre_player_b.isnot(None),
            Match.elo_post_player_b.isnot(None),
        )
        .group_by(Match.player_b_id)
    )

    combined = union_all(gain_a, gain_b).subquery()

    top_movers = (
        db.query(
            combined.c.player_id,
            func.sum(combined.c.gain).label("total_gain"),
        )
        .group_by(combined.c.player_id)
        .order_by(func.sum(combined.c.gain).desc())
        .limit(3)
        .all()
    )

    top_ids = [row.player_id for row in top_movers]
    gains = {row.player_id: float(row.total_gain) for row in top_movers}

    if not top_ids:
        payload: list[dict] = []
        _movers_cache = payload
        _movers_cache_time = now
        return JSONResponse(payload)

    player_rows = (
        db.query(Player, PlayerEloState)
        .outerjoin(PlayerEloState, PlayerEloState.player_id == Player.id)
        .filter(Player.id.in_(top_ids))
        .all()
    )
    player_map = {p.id: (p, elo) for p, elo in player_rows}

    payload_list = []
    for pid in top_ids:
        p_elo = player_map.get(pid)
        if not p_elo:
            continue
        player, elo = p_elo
        payload_list.append({
            "player_id": pid,
            "name": player.canonical_name,
            "nationality_ioc": player.nationality_ioc,
            "rating": round(float(elo.rating)) if elo else None,
            "rating_change": round(gains[pid]),
            "gender": player.gender,
            "url": f"/players/{player.id}/{slugify_name(player.canonical_name)}",
        })

    _movers_cache = payload_list
    _movers_cache_time = now
    return JSONResponse(payload_list)


@router.get("/api/home/blog")
def home_api_blog(
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    global _blog_cache, _blog_cache_time
    now = time.monotonic()
    if _blog_cache is not False and (now - _blog_cache_time) < _STATS_CACHE_TTL:
        return JSONResponse(_blog_cache)

    blog_dir = CONTENT_PATH / "blog"
    posts = []
    if blog_dir.exists():
        for file_path in blog_dir.glob("*.md"):
            post = frontmatter.load(file_path)
            if post.get("draft", False):
                continue
            post_date = post.get("date", datetime.min)
            posts.append({
                "_date": post_date,
                "slug": file_path.stem,
                "title": post.get("title", "Untitled"),
                "date": post_date.isoformat() if hasattr(post_date, "isoformat") else str(post_date),
                "excerpt": post.get("excerpt", ""),
                "url": f"/blog/{file_path.stem}",
            })

    posts.sort(key=lambda p: p["_date"], reverse=True)
    for p in posts:
        p.pop("_date", None)

    _blog_cache = posts
    _blog_cache_time = now
    return JSONResponse(posts)


@router.get("/api/home")
def home_api(
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    home_base_query = _home_base_query(db)
    upcoming = (
        home_base_query
        .filter(Match.status.in_(get_status_group("upcoming")))
        .order_by(*upcoming_sort_expressions())
        .limit(10)
        .all()
    )
    completed = (
        home_base_query
        .filter(Match.status.in_(get_status_group("historical_default")))
        .order_by(*completed_sort_expressions())
        .limit(10)
        .all()
    )
    upcoming_matches = [serialize_match(m) for m in upcoming]
    completed_matches = [serialize_match(m) for m in completed]

    match_rows_template = templates.get_template("partials/match_rows.html")
    match_rows_module = match_rows_template.module

    global _stats_cache, _stats_cache_time
    now = time.monotonic()
    if _stats_cache is None or (now - _stats_cache_time) >= _STATS_CACHE_TTL:
        row = db.query(
            db.query(func.count(Match.id)).scalar_subquery().label("matches"),
            db.query(func.count(Player.id)).scalar_subquery().label("players"),
            db.query(func.count(TournamentEdition.id)).scalar_subquery().label("editions"),
            db.query(func.count(Player.nationality_ioc.distinct())).scalar_subquery().label("countries"),
            db.query(func.min(Match.match_date)).scalar_subquery().label("min_date"),
        ).one()
        days_of_data = (date.today() - row.min_date).days if row.min_date else 0
        _stats_cache = {
            "matches_total": row.matches or 0,
            "players_total": row.players or 0,
            "editions_total": row.editions or 0,
            "countries_total": row.countries or 0,
            "prediction_accuracy_pct": _get_prediction_accuracy(db),
            "days_of_data": days_of_data,
        }
        _stats_cache_time = now

    return JSONResponse(
        {
            "stats": _stats_cache,
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
