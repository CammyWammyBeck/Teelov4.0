from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import frontmatter
import markdown
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import StarletteHTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session, joinedload

from teelo.config import settings
from teelo.db.models import Match, Player, PlayerAlias, Tournament, TournamentEdition
from teelo.db.session import get_db

app = FastAPI(title="Teelo Ratings")

# Mount static files
static_path = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_path), name="static")

# Setup templates
templates_path = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=templates_path)
content_path = Path(__file__).parent / "content"

# Inject settings (for feature flags) into all templates
templates.env.globals["features"] = settings


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
    Home page displaying recent matches.
    """
    if not settings.enable_feature_matches:
        return RedirectResponse(url="/blog")

    # Query recent completed matches
    matches = (
        db.query(Match)
        .options(
            joinedload(Match.player_a),
            joinedload(Match.player_b),
            joinedload(Match.tournament_edition).joinedload(TournamentEdition.tournament),
        )
        .filter(Match.status.in_(["completed", "retired", "walkover", "default"]))
        .order_by(Match.match_date.desc().nullslast())
        .limit(50)
        .all()
    )

    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "matches": matches,
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

    return {
        "id": match.id,
        "tour": tournament.tour if tournament else None,
        "gender": tournament.gender if tournament else None,
        "tournament_name": tournament.name if tournament else None,
        "tournament_level": tournament.level if tournament else None,
        "surface": surface,
        "round": match.round,
        "player_a": {
            "id": pa.id if pa else match.player_a_id,
            "name": pa.canonical_name if pa else "Unknown",
            "seed": match.player_a_seed,
        },
        "player_b": {
            "id": pb.id if pb else match.player_b_id,
            "name": pb.canonical_name if pb else "Unknown",
            "seed": match.player_b_seed,
        },
        "score": match.score,
        "winner_id": match.winner_id,
        "status": match.status,
        "match_date": match.match_date.isoformat() if match.match_date else None,
        "year": match.match_date.year if match.match_date else (
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
    # Default: all finished match types
    if status:
        status_list = [s.strip() for s in status.split(",")]
    else:
        status_list = ["completed", "retired", "walkover", "default"]
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
    query = query.order_by(Match.match_date.desc().nullslast(), Match.id.desc())
    offset = (page - 1) * per_page
    matches = query.offset(offset).limit(per_page).all()

    return JSONResponse({
        "matches": [_serialize_match(m) for m in matches],
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

# Only for debugging
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("teelo.web.main:app", host="0.0.0.0", port=8000, reload=True)
