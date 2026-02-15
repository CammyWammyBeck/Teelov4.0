from datetime import date, datetime, timedelta
import hashlib
from pathlib import Path
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import frontmatter
import markdown
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import StarletteHTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy import and_, case, func, or_
from sqlalchemy.orm import Session, joinedload

from teelo.config import settings
from teelo.db.models import (
    AdminUser,
    Match,
    Player,
    PlayerAlias,
    PlayerEloState,
    PlayerReviewQueue,
    Tournament,
    TournamentEdition,
)
from teelo.db.session import get_db
from teelo.match_statuses import get_status_group, normalize_status_filter
from teelo.players.identity import PlayerIdentityService
from teelo.web.admin_auth import authenticate_admin, mark_admin_login

app = FastAPI(title="Teelo Ratings")
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.admin_session_secret,
    max_age=settings.admin_session_max_age_seconds,
)

# Mount static files
static_path = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_path), name="static")

# Setup templates
templates_path = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=templates_path)
content_path = Path(__file__).parent / "content"
MATCHES_PAGE_STATUS_FILTERS = get_status_group("all")

# Inject settings (for feature flags) into all templates
templates.env.globals["features"] = settings

ADMIN_SESSION_KEY = "admin_user_id"


def _current_admin_user(request: Request, db: Session) -> Optional[AdminUser]:
    admin_id = request.session.get(ADMIN_SESSION_KEY)
    if not admin_id:
        return None
    return (
        db.query(AdminUser)
        .filter(AdminUser.id == admin_id, AdminUser.is_active.is_(True))
        .first()
    )


def _require_admin(request: Request, db: Session) -> Optional[RedirectResponse]:
    if _current_admin_user(request, db) is not None:
        return None
    next_path = request.url.path
    return RedirectResponse(
        url=f"/admin/login?next={quote(next_path, safe='/')}",
        status_code=303,
    )


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Render a custom 404 page for not-found errors; fall back to default for others."""
    if exc.status_code == 404:
        return templates.TemplateResponse(
            "404.html",
            {"request": request, "now": datetime.utcnow(), "current_path": request.url.path},
            status_code=404,
        )
    # For non-404 errors, return a plain JSON-style response
    return HTMLResponse(content=str(exc.detail), status_code=exc.status_code)


def require_feature(feature_flag: str):
    """
    Dependency factory to check if a feature flag is enabled.

    Usage:
        @app.get("/rankings", dependencies=[Depends(require_feature("enable_feature_rankings"))])
    """
    def check_feature(request: Request):
        if not getattr(settings, feature_flag, False):
            # If matches (home) is disabled, redirect to blog
            if feature_flag == "enable_feature_matches" and request.url.path == "/":
                return RedirectResponse(url="/blog")

            # Otherwise, 404 Not Found
            raise HTTPException(status_code=404, detail="Feature not enabled")

    return check_feature


def get_blog_posts() -> List[Dict[str, Any]]:
    """Scan the content/blog directory for markdown files."""
    posts = []
    blog_dir = content_path / "blog"

    if not blog_dir.exists():
        return posts

    for file_path in blog_dir.glob("*.md"):
        post = frontmatter.load(file_path)

        # Skip draft posts
        if post.get("draft", False):
            continue

        posts.append({
            "slug": file_path.stem,
            "title": post.get("title", "Untitled"),
            "date": post.get("date", datetime.min),
            "author": post.get("author", "Unknown"),
            "excerpt": post.get("excerpt", ""),
            "category": post.get("category", "Blog"),
            "content": markdown.markdown(post.content, extensions=['tables']),
        })

    # Sort by date descending
    return sorted(posts, key=lambda x: x["date"], reverse=True)


@app.get("/", response_class=HTMLResponse)
async def home(
    request: Request,
    db: Session = Depends(get_db),
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches"))
):
    """
    Home page displaying upcoming and recent matches.
    """
    if not settings.enable_feature_matches:
        return RedirectResponse(url="/blog")

    # Home feed filters:
    # - All Grand Slam, ATP, WTA
    # - Challenger/WTA 125 only SF/F
    # - ITF only F
    home_scope_filter = or_(
        Tournament.level == "Grand Slam",
        Tournament.tour.in_(["ATP", "WTA"]),
        and_(
            Tournament.tour.in_(["CHALLENGER", "Challenger", "WTA 125", "WTA_125"]),
            Match.round.in_(["SF", "F"]),
        ),
        and_(
            Tournament.tour == "ITF",
            Match.round == "F",
        ),
    )

    home_base_query = (
        db.query(Match)
        .outerjoin(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .outerjoin(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .options(
            joinedload(Match.player_a),
            joinedload(Match.player_b),
            joinedload(Match.tournament_edition).joinedload(TournamentEdition.tournament),
        )
        .filter(home_scope_filter)
    )

    upcoming_matches = (
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

    completed_matches = (
        home_base_query
        .filter(Match.status.in_(get_status_group("historical_default")))
        .order_by(
            func.coalesce(Match.match_date, Match.scheduled_date).desc().nullslast(),
            Match.id.desc(),
        )
        .limit(10)
        .all()
    )

    serialized_upcoming_matches = [_serialize_match(m) for m in upcoming_matches]
    serialized_completed_matches = [_serialize_match(m) for m in completed_matches]

    stats = {
        "matches_total": db.query(func.count(Match.id)).scalar() or 0,
        "players_total": db.query(func.count(Player.id)).scalar() or 0,
        "editions_total": db.query(func.count(TournamentEdition.id)).scalar() or 0,
    }

    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "upcoming_matches": serialized_upcoming_matches,
            "completed_matches": serialized_completed_matches,
            "stats": stats,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
        },
    )


@app.get("/matches", response_class=HTMLResponse)
async def matches_page(
    request: Request,
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches")),
):
    """
    Dedicated matches page with full filtering UI.

    The page itself is a shell - match data is loaded dynamically
    via the /api/matches JSON endpoint from JavaScript.
    """
    return templates.TemplateResponse(
        "matches.html",
        {
            "request": request,
            "status_filters": MATCHES_PAGE_STATUS_FILTERS,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
        },
    )


# ==========================================================================
# JSON API Endpoints
# ==========================================================================


def _resolve_date_preset(preset: str) -> tuple[Optional[date], Optional[date]]:
    """
    Convert a date preset string into a (date_from, date_to) tuple.

    Supported presets:
    - '7d': Last 7 days
    - '30d': Last 30 days
    - '90d': Last 90 days
    - 'ytd': Year-to-date (Jan 1 of current year to today)
    - '2024', '2023', etc.: Full calendar year
    """
    today = date.today()

    if preset == "7d":
        return today - timedelta(days=7), today
    elif preset == "30d":
        return today - timedelta(days=30), today
    elif preset == "90d":
        return today - timedelta(days=90), today
    elif preset == "ytd":
        return date(today.year, 1, 1), today
    elif preset.isdigit() and len(preset) == 4:
        year = int(preset)
        return date(year, 1, 1), date(year, 12, 31)
    else:
        return None, None


_SET_SCORE_RE = re.compile(r"^(\d+)-(\d+)(\(\d+\))?$")


def _flip_score_for_display(score: Optional[str]) -> Optional[str]:
    """Flip set scores from A-perspective to B-perspective for display."""
    if not score:
        return score

    parts = score.split()
    flipped_parts: list[str] = []
    for part in parts:
        match = _SET_SCORE_RE.match(part)
        if match:
            suffix = match.group(3) or ""
            flipped_parts.append(f"{match.group(2)}-{match.group(1)}{suffix}")
        else:
            flipped_parts.append(part)
    return " ".join(flipped_parts)


def _serialize_match(match: Match) -> dict:
    """
    Serialize a Match ORM object to a JSON-friendly dict.

    Expects the match to have player_a, player_b, and
    tournament_edition.tournament relationships loaded via joinedload.
    """
    te = match.tournament_edition
    tournament = te.tournament if te else None

    # Surface can be overridden at the edition level
    surface = (te.surface if te and te.surface else None) or (
        tournament.surface if tournament else None
    )

    pa = match.player_a
    pb = match.player_b
    display_date = match.match_date or match.scheduled_date

    player_a_payload = {
        "id": pa.id if pa else match.player_a_id,
        "name": pa.canonical_name if pa else "Unknown",
        "seed": match.player_a_seed,
        "elo_pre": int(match.elo_pre_player_a) if match.elo_pre_player_a is not None else None,
        "elo_change": int(match.elo_post_player_a - match.elo_pre_player_a) if match.elo_post_player_a is not None and match.elo_pre_player_a is not None else None,
    }
    player_b_payload = {
        "id": pb.id if pb else match.player_b_id,
        "name": pb.canonical_name if pb else "Unknown",
        "seed": match.player_b_seed,
        "elo_pre": int(match.elo_pre_player_b) if match.elo_pre_player_b is not None else None,
        "elo_change": int(match.elo_post_player_b - match.elo_pre_player_b) if match.elo_post_player_b is not None and match.elo_pre_player_b is not None else None,
    }

    # Deterministically randomize display sides to avoid persistent winner-on-A bias
    # while keeping storage and winner_id semantics unchanged.
    swap_key = f"{match.id}:{match.temporal_order or 0}"
    swap_display_sides = (hashlib.blake2s(swap_key.encode("utf-8"), digest_size=1).digest()[0] & 1) == 1
    display_score = match.score
    if swap_display_sides:
        player_a_payload, player_b_payload = player_b_payload, player_a_payload
        display_score = _flip_score_for_display(display_score)

    return {
        "id": match.id,
        "tour": tournament.tour if tournament else None,
        "gender": tournament.gender if tournament else None,
        "tournament_name": tournament.name if tournament else None,
        "tournament_level": tournament.level if tournament else None,
        "surface": surface,
        "round": match.round,
        "player_a": player_a_payload,
        "player_b": player_b_payload,
        "score": display_score,
        "winner_id": match.winner_id,
        "status": match.status,
        "match_date": display_date.isoformat() if display_date else None,
        "match_date_display": display_date.strftime("%d %b %Y") if display_date else None,
        "year": display_date.year if display_date else (
            te.year if te else None
        ),
    }


@app.get("/api/matches")
async def api_matches(
    db: Session = Depends(get_db),
    tour: Optional[str] = Query(None, description="Comma-separated tours: ATP,WTA,CHALLENGER,ITF"),
    gender: Optional[str] = Query(None, description="Comma-separated genders: men,women"),
    surface: Optional[str] = Query(None, description="Comma-separated surfaces: Hard,Clay,Grass"),
    level: Optional[str] = Query(None, description="Comma-separated levels: Grand Slam,Masters 1000"),
    round: Optional[str] = Query(None, description="Comma-separated rounds: F,SF,QF,R16"),
    status: Optional[str] = Query(None, description="Comma-separated statuses (default: all finished)"),
    player: Optional[str] = Query(None, description="Player name search (partial match)"),
    player_id: Optional[int] = Query(None, description="Exact player ID (matches in either position)"),
    player_a_id: Optional[int] = Query(None, description="Head-to-head: Player A ID"),
    player_b_id: Optional[int] = Query(None, description="Head-to-head: Player B ID"),
    tournament: Optional[str] = Query(None, description="Tournament name search (partial match)"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    date_preset: Optional[str] = Query(None, description="Date preset: 7d,30d,90d,ytd,2024"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    per_page: int = Query(50, ge=1, le=100, description="Results per page"),
):
    """
    Paginated, filterable match data as JSON.

    Supports combining multiple filters. All comma-separated params allow
    selecting multiple values (OR within category, AND across categories).
    """
    # Start building the query with eager-loaded relationships
    # We use outerjoin for tournament data (some matches may not have it yet)
    # and joinedload to actually populate the ORM relationships
    query = (
        db.query(Match)
        .outerjoin(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .outerjoin(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .options(
            joinedload(Match.player_a),
            joinedload(Match.player_b),
            joinedload(Match.tournament_edition).joinedload(TournamentEdition.tournament),
        )
    )

    # --- Status filter ---
    # Default: historical result statuses (cancelled excluded unless explicit)
    raw_statuses = status.split(",") if status else None
    status_list = normalize_status_filter(raw_statuses, default_group="historical_default")
    query = query.filter(Match.status.in_(status_list))

    # --- Tour filter ---
    if tour:
        raw_tour_values = [t.strip() for t in tour.split(",") if t.strip()]
        tour_aliases = {
            "ATP": ["ATP"],
            "WTA": ["WTA"],
            "ITF": ["ITF"],
            "CHALLENGER": ["CHALLENGER", "Challenger"],
            "WTA_125": ["WTA_125", "WTA 125"],
            "ATP_CHALLENGER": ["CHALLENGER", "Challenger"],
        }
        tour_values: List[str] = []
        for raw_value in raw_tour_values:
            normalized = raw_value.upper().replace(" ", "_")
            tour_values.extend(tour_aliases.get(normalized, [raw_value]))

        query = query.filter(Tournament.tour.in_(sorted(set(tour_values))))

    # --- Gender filter ---
    if gender:
        gender_list = [g.strip().lower() for g in gender.split(",") if g.strip()]
        gender_list = [g for g in gender_list if g in {"men", "women"}]
        if gender_list:
            query = query.filter(Tournament.gender.in_(sorted(set(gender_list))))

    # --- Surface filter ---
    if surface:
        surface_list = [s.strip() for s in surface.split(",")]
        # Surface can be on the edition (override) or the tournament (default)
        query = query.filter(
            or_(
                TournamentEdition.surface.in_(surface_list),
                and_(
                    TournamentEdition.surface.is_(None),
                    Tournament.surface.in_(surface_list),
                ),
            )
        )

    # --- Level filter ---
    if level:
        level_list = [l.strip() for l in level.split(",")]
        query = query.filter(Tournament.level.in_(level_list))

    # --- Round filter ---
    if round:
        round_list = [r.strip() for r in round.split(",")]
        query = query.filter(Match.round.in_(round_list))

    # --- Player filters ---
    if player_a_id and player_b_id:
        # Head-to-head mode: find matches between these two players (either order)
        query = query.filter(
            or_(
                and_(Match.player_a_id == player_a_id, Match.player_b_id == player_b_id),
                and_(Match.player_a_id == player_b_id, Match.player_b_id == player_a_id),
            )
        )
    elif player_id:
        # Single player: find all their matches
        query = query.filter(
            or_(Match.player_a_id == player_id, Match.player_b_id == player_id)
        )
    elif player:
        # Name search: partial match on either player's canonical name
        # Create aliased references for the two player joins
        player_pattern = f"%{player}%"
        player_a_alias = db.query(Player.id).filter(
            Player.canonical_name.ilike(player_pattern)
        ).subquery()
        query = query.filter(
            or_(
                Match.player_a_id.in_(db.query(player_a_alias.c.id)),
                Match.player_b_id.in_(db.query(player_a_alias.c.id)),
            )
        )

    # --- Tournament name filter ---
    if tournament:
        query = query.filter(Tournament.name.ilike(f"%{tournament}%"))

    # --- Date filters ---
    resolved_from = None
    resolved_to = None

    # Date preset takes priority if both preset and explicit dates are given
    if date_preset:
        resolved_from, resolved_to = _resolve_date_preset(date_preset)

    # Explicit dates override preset (or supplement if preset didn't resolve)
    if date_from:
        try:
            resolved_from = date.fromisoformat(date_from)
        except ValueError:
            pass
    if date_to:
        try:
            resolved_to = date.fromisoformat(date_to)
        except ValueError:
            pass

    if resolved_from:
        query = query.filter(Match.match_date >= resolved_from)
    if resolved_to:
        query = query.filter(Match.match_date <= resolved_to)

    # --- Get total count (before pagination) ---
    total = query.count()

    # --- Ordering and pagination ---
    query = query.order_by(
        Match.match_date.desc().nullslast(),
        Match.temporal_order.desc().nullslast(),
        Match.id.desc(),
    )
    offset = (page - 1) * per_page
    matches = query.offset(offset).limit(per_page).all()
    serialized_matches = [_serialize_match(m) for m in matches]

    match_rows_template = templates.get_template("partials/match_rows.html")
    table_rows_html = match_rows_template.module.render_table_rows(serialized_matches)
    cards_html = match_rows_template.module.render_cards(serialized_matches)

    return JSONResponse({
        "matches": serialized_matches,
        "table_rows_html": table_rows_html,
        "cards_html": cards_html,
        "total": total,
        "page": page,
        "per_page": per_page,
        "has_more": (offset + per_page) < total,
    })


@app.get("/api/players/search")
async def api_players_search(
    db: Session = Depends(get_db),
    q: str = Query(..., min_length=2, description="Search query (min 2 characters)"),
    limit: int = Query(8, ge=1, le=20, description="Max results"),
):
    """
    Search players by name for autocomplete.

    Searches both canonical names and aliases for broader matching.
    Results are deduplicated and ordered with prefix matches first.
    """
    pattern = f"%{q}%"

    # Search canonical names
    name_matches = (
        db.query(Player)
        .filter(Player.canonical_name.ilike(pattern))
        .limit(limit)
        .all()
    )

    # Also search aliases for broader matching
    alias_matches = (
        db.query(Player)
        .join(PlayerAlias, PlayerAlias.player_id == Player.id)
        .filter(PlayerAlias.alias.ilike(pattern))
        .limit(limit)
        .all()
    )

    # Deduplicate by player ID, preserving order
    seen_ids = set()
    all_players = []
    for p in name_matches + alias_matches:
        if p.id not in seen_ids:
            seen_ids.add(p.id)
            all_players.append(p)

    # Sort: prefix matches on canonical_name first, then alphabetical
    def sort_key(p):
        name_lower = p.canonical_name.lower()
        q_lower = q.lower()
        is_prefix = name_lower.startswith(q_lower)
        return (0 if is_prefix else 1, name_lower)

    all_players.sort(key=sort_key)

    return JSONResponse({
        "players": [
            {
                "id": p.id,
                "name": p.canonical_name,
                "nationality": p.nationality_ioc,
            }
            for p in all_players[:limit]
        ]
    })


@app.get("/rankings", response_class=HTMLResponse)
async def rankings_page(
    request: Request,
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_rankings")),
):
    """
    Rankings page showing men's and women's ELO rankings side by side.

    The page is a shell — ranking data is loaded dynamically
    via the /api/rankings JSON endpoint from JavaScript.
    """
    return templates.TemplateResponse(
        "rankings.html",
        {
            "request": request,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
        },
    )


@app.get("/api/rankings")
async def api_rankings(
    db: Session = Depends(get_db),
    gender: str = Query(..., description="Player gender: 'men' or 'women'"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    per_page: int = Query(50, ge=1, le=100, description="Results per page"),
    include_inactive: bool = Query(False, description="Include players with no match in the last 6 months"),
):
    """
    Paginated ELO rankings as JSON.

    Players are assigned to a gender based on the tournaments they've played in,
    since the Player model doesn't have a gender field. Uses a subquery to find
    all players who have appeared in tournaments of the requested gender.

    By default, only "active" players (last match within 6 months) are shown.
    Pass include_inactive=true to include all ranked players.
    """
    # Validate gender parameter
    gender_param = gender.strip().lower()
    if gender_param not in ("men", "women"):
        return JSONResponse(
            {"error": "gender must be 'men' or 'women'"},
            status_code=400,
        )

    # Build per-player match counts by tournament gender.
    # A player must be majority in requested gender to appear in that ranking.
    events_a = (
        db.query(
            Match.player_a_id.label("pid"),
            Tournament.gender.label("gender"),
        )
        .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .filter(Tournament.gender.in_(("men", "women")))
    )
    events_b = (
        db.query(
            Match.player_b_id.label("pid"),
            Tournament.gender.label("gender"),
        )
        .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .filter(Tournament.gender.in_(("men", "women")))
    )
    gender_events = events_a.union_all(events_b).subquery()
    gender_counts = (
        db.query(
            gender_events.c.pid.label("pid"),
            func.sum(
                case((gender_events.c.gender == "men", 1), else_=0)
            ).label("men_matches"),
            func.sum(
                case((gender_events.c.gender == "women", 1), else_=0)
            ).label("women_matches"),
        )
        .group_by(gender_events.c.pid)
        .subquery()
    )

    # Main query: join Player + PlayerEloState, filter by gender, rank by ELO
    query = (
        db.query(Player, PlayerEloState)
        .join(PlayerEloState, PlayerEloState.player_id == Player.id)
        .join(gender_counts, gender_counts.c.pid == Player.id)
    )

    if gender_param == "men":
        query = query.filter(gender_counts.c.men_matches > gender_counts.c.women_matches)
    else:
        query = query.filter(gender_counts.c.women_matches > gender_counts.c.men_matches)

    # By default, exclude inactive players (no match in the last 6 months)
    if not include_inactive:
        six_months_ago = date.today() - timedelta(days=183)
        query = query.filter(PlayerEloState.last_match_date >= six_months_ago)

    query = query.order_by(PlayerEloState.rating.desc(), Player.canonical_name.asc())

    total = query.count()
    offset = (page - 1) * per_page
    results = query.offset(offset).limit(per_page).all()

    # Serialize players with rank computed from offset
    players_data = []
    for i, (player, elo_state) in enumerate(results):
        last_date = elo_state.last_match_date
        players_data.append({
            "rank": offset + i + 1,
            "id": player.id,
            "name": player.canonical_name,
            "nationality": player.nationality_ioc,
            "rating": int(elo_state.rating),
            "match_count": elo_state.match_count,
            "career_peak": int(elo_state.career_peak),
            "last_match_date": last_date.isoformat() if last_date else None,
            "last_match_display": last_date.strftime("%d %b %Y") if last_date else None,
        })

    # Pre-render table rows HTML using the ranking_rows partial
    ranking_rows_template = templates.get_template("partials/ranking_rows.html")
    table_rows_html = ranking_rows_template.module.render_ranking_rows(players_data)

    return JSONResponse({
        "players": players_data,
        "table_rows_html": table_rows_html,
        "total": total,
        "page": page,
        "per_page": per_page,
        "has_more": (offset + per_page) < total,
    })


@app.get("/blog", response_class=HTMLResponse)
async def blog_list(request: Request):
    """List all blog posts."""
    posts = get_blog_posts()
    # Collect unique categories for the filter UI
    categories = sorted(set(p["category"] for p in posts if p["category"]))
    return templates.TemplateResponse(
        "blog_list.html",
        {
            "request": request,
            "posts": posts,
            "categories": categories,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
        },
    )


@app.get("/blog/{slug}", response_class=HTMLResponse)
async def blog_detail(request: Request, slug: str):
    """Display a single blog post."""
    blog_dir = content_path / "blog"
    file_path = blog_dir / f"{slug}.md"

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Post not found")

    post = frontmatter.load(file_path)

    # Draft posts are not publicly accessible
    if post.get("draft", False):
        raise HTTPException(status_code=404, detail="Post not found")

    html_content = markdown.markdown(post.content, extensions=['tables'])

    post_data = {
        "slug": slug,
        "title": post.get("title", "Untitled"),
        "date": post.get("date", datetime.min),
        "author": post.get("author", "Unknown"),
        "category": post.get("category", "Blog"),
        "content": html_content,
    }

    return templates.TemplateResponse(
        "blog_post.html",
        {
            "request": request,
            "post": post_data,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
        },
    )


@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_page(
    request: Request,
    db: Session = Depends(get_db),
    next: str = Query("/admin"),
):
    if _current_admin_user(request, db):
        return RedirectResponse(url="/admin", status_code=303)

    return templates.TemplateResponse(
        "admin_login.html",
        {
            "request": request,
            "next": next if next.startswith("/") else "/admin",
            "error": None,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
        },
    )


@app.post("/admin/login", response_class=HTMLResponse)
async def admin_login_submit(
    request: Request,
    db: Session = Depends(get_db),
):
    form = await request.form()
    username = str(form.get("username", "")).strip().lower()
    password = str(form.get("password", ""))
    next_path = str(form.get("next", "/admin"))
    if not next_path.startswith("/"):
        next_path = "/admin"

    admin = authenticate_admin(db, username, password)
    if not admin:
        return templates.TemplateResponse(
            "admin_login.html",
            {
                "request": request,
                "next": next_path,
                "error": "Invalid username or password.",
                "now": datetime.utcnow(),
                "current_path": request.url.path,
            },
            status_code=401,
        )

    request.session[ADMIN_SESSION_KEY] = admin.id
    mark_admin_login(db, admin)
    db.commit()
    return RedirectResponse(url=next_path, status_code=303)


@app.post("/admin/logout")
async def admin_logout(request: Request):
    request.session.pop(ADMIN_SESSION_KEY, None)
    return RedirectResponse(url="/admin/login", status_code=303)


@app.get("/admin", response_class=HTMLResponse)
async def admin_home(
    request: Request,
    db: Session = Depends(get_db),
):
    redirect = _require_admin(request, db)
    if redirect:
        return redirect

    pending_count = (
        db.query(func.count(PlayerReviewQueue.id))
        .filter(PlayerReviewQueue.status == "pending")
        .scalar()
        or 0
    )
    admin = _current_admin_user(request, db)
    return templates.TemplateResponse(
        "admin_home.html",
        {
            "request": request,
            "admin": admin,
            "pending_count": pending_count,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
        },
    )


def _player_detail_map(db: Session, player_ids: set[int]) -> dict[int, dict]:
    if not player_ids:
        return {}

    players = db.query(Player).filter(Player.id.in_(player_ids)).all()
    alias_counts = dict(
        db.query(PlayerAlias.player_id, func.count(PlayerAlias.id))
        .filter(PlayerAlias.player_id.in_(player_ids))
        .group_by(PlayerAlias.player_id)
        .all()
    )
    elo_states = {
        state.player_id: state
        for state in db.query(PlayerEloState).filter(
            PlayerEloState.player_id.in_(player_ids)
        ).all()
    }

    counts_a = dict(
        db.query(Match.player_a_id, func.count(Match.id))
        .filter(Match.player_a_id.in_(player_ids))
        .group_by(Match.player_a_id)
        .all()
    )
    counts_b = dict(
        db.query(Match.player_b_id, func.count(Match.id))
        .filter(Match.player_b_id.in_(player_ids))
        .group_by(Match.player_b_id)
        .all()
    )

    details: dict[int, dict] = {}
    for player in players:
        state = elo_states.get(player.id)
        details[player.id] = {
            "id": player.id,
            "name": player.canonical_name,
            "atp_id": player.atp_id,
            "wta_id": player.wta_id,
            "itf_id": player.itf_id,
            "nationality": player.nationality_ioc,
            "alias_count": int(alias_counts.get(player.id, 0)),
            "match_count": int(counts_a.get(player.id, 0)) + int(counts_b.get(player.id, 0)),
            "elo_rating": int(state.rating) if state and state.rating is not None else None,
            "last_match_date": (
                state.last_match_date.strftime("%Y-%m-%d")
                if state and state.last_match_date
                else None
            ),
        }
    return details


@app.get("/admin/duplicates", response_class=HTMLResponse)
async def admin_duplicates_queue(
    request: Request,
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
):
    redirect = _require_admin(request, db)
    if redirect:
        return redirect

    base_query = db.query(PlayerReviewQueue).filter(PlayerReviewQueue.status == "pending")
    total = base_query.count()
    items = (
        base_query.order_by(PlayerReviewQueue.created_at.asc(), PlayerReviewQueue.id.asc())
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )

    player_ids: set[int] = set()
    for item in items:
        for pid in (
            item.suggested_player_1_id,
            item.suggested_player_2_id,
            item.suggested_player_3_id,
        ):
            if pid:
                player_ids.add(pid)
    details_map = _player_detail_map(db, player_ids)

    queue_rows = []
    for item in items:
        suggestions = []
        for idx, pid in enumerate(
            [
                item.suggested_player_1_id,
                item.suggested_player_2_id,
                item.suggested_player_3_id,
            ],
            start=1,
        ):
            if not pid:
                continue
            confidence = getattr(item, f"suggested_player_{idx}_confidence")
            suggestion = details_map.get(pid)
            if not suggestion:
                continue
            suggestions.append(
                {
                    "player": suggestion,
                    "confidence": float(confidence) if confidence is not None else None,
                }
            )

        queue_rows.append(
            {
                "id": item.id,
                "scraped_name": item.scraped_name,
                "scraped_source": item.scraped_source,
                "scraped_external_id": item.scraped_external_id,
                "match_external_id": item.match_external_id,
                "tournament_name": item.tournament_name,
                "created_at": item.created_at.strftime("%Y-%m-%d %H:%M"),
                "suggestions": suggestions,
            }
        )

    notice = request.query_params.get("notice")
    admin = _current_admin_user(request, db)
    return templates.TemplateResponse(
        "admin_duplicates.html",
        {
            "request": request,
            "admin": admin,
            "queue_rows": queue_rows,
            "total": total,
            "page": page,
            "per_page": per_page,
            "has_more": (page * per_page) < total,
            "notice": notice,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
        },
    )


def _admin_action_redirect(
    message: str,
    page: Optional[str],
    per_page: Optional[str],
) -> RedirectResponse:
    query = f"notice={quote(message, safe='')}"
    if page and page.isdigit():
        query += f"&page={page}"
    if per_page and per_page.isdigit():
        query += f"&per_page={per_page}"
    return RedirectResponse(url=f"/admin/duplicates?{query}", status_code=303)


@app.post("/admin/duplicates/{review_id}/match")
async def admin_duplicate_match(
    request: Request,
    review_id: int,
    db: Session = Depends(get_db),
):
    redirect = _require_admin(request, db)
    if redirect:
        return redirect

    form = await request.form()
    player_id_raw = str(form.get("player_id", "")).strip()
    page = str(form.get("page", "")).strip()
    per_page = str(form.get("per_page", "")).strip()
    if not player_id_raw.isdigit():
        return _admin_action_redirect("Invalid player id.", page, per_page)

    admin = _current_admin_user(request, db)
    identity = PlayerIdentityService(db)
    identity.resolve_review_item(
        review_id=review_id,
        action="match",
        player_id=int(player_id_raw),
        resolved_by=admin.username if admin else "admin",
    )
    return _admin_action_redirect(f"Matched review #{review_id}.", page, per_page)


@app.post("/admin/duplicates/{review_id}/create")
async def admin_duplicate_create(
    request: Request,
    review_id: int,
    db: Session = Depends(get_db),
):
    redirect = _require_admin(request, db)
    if redirect:
        return redirect

    form = await request.form()
    page = str(form.get("page", "")).strip()
    per_page = str(form.get("per_page", "")).strip()
    admin = _current_admin_user(request, db)
    identity = PlayerIdentityService(db)
    identity.resolve_review_item(
        review_id=review_id,
        action="create",
        resolved_by=admin.username if admin else "admin",
    )
    return _admin_action_redirect(f"Created player for review #{review_id}.", page, per_page)


@app.post("/admin/duplicates/{review_id}/ignore")
async def admin_duplicate_ignore(
    request: Request,
    review_id: int,
    db: Session = Depends(get_db),
):
    redirect = _require_admin(request, db)
    if redirect:
        return redirect

    form = await request.form()
    page = str(form.get("page", "")).strip()
    per_page = str(form.get("per_page", "")).strip()
    admin = _current_admin_user(request, db)
    identity = PlayerIdentityService(db)
    identity.resolve_review_item(
        review_id=review_id,
        action="ignore",
        resolved_by=admin.username if admin else "admin",
    )
    return _admin_action_redirect(f"Ignored review #{review_id}.", page, per_page)


# Only for debugging
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("teelo.web.main:app", host="0.0.0.0", port=8000, reload=True)
