from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import frontmatter
import markdown
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exceptions import StarletteHTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, joinedload

from teelo.config import settings
from teelo.db.models import Match, Player, Tournament, TournamentEdition
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
        posts.append({
            "slug": file_path.stem,
            "title": post.get("title", "Untitled"),
            "date": post.get("date", datetime.min),
            "author": post.get("author", "Unknown"),
            "excerpt": post.get("excerpt", ""),
            "category": post.get("category", ""),
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
        .filter(Match.status == "completed")
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


@app.get("/blog", response_class=HTMLResponse)
async def blog_list(request: Request):
    """List all blog posts."""
    posts = get_blog_posts()
    return templates.TemplateResponse(
        "blog_list.html",
        {
            "request": request,
            "posts": posts,
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
    html_content = markdown.markdown(post.content, extensions=['tables'])

    post_data = {
        "slug": slug,
        "title": post.get("title", "Untitled"),
        "date": post.get("date", datetime.min),
        "author": post.get("author", "Unknown"),
        "category": post.get("category", ""),
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
