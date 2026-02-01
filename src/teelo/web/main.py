from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import frontmatter
import markdown
from fastapi import Depends, FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, joinedload

from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

import frontmatter
import markdown
from fastapi import Depends, FastAPI, Request, HTTPException
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
            "content": markdown.markdown(post.content, extensions=['tables']),
        })
    
    # Sort by date descending
    return sorted(posts, key=lambda x: x["date"], reverse=True)


@app.get("/", response_class=HTMLResponse)
async def home(
    request: Request, 
    db: Session = Depends(get_db),
    # Check feature flag first. If matches are disabled, this might redirect or raise 404.
    # We return the result of the dependency check if it's a Response object (like RedirectResponse)
    _feature_check: Optional[Any] = Depends(require_feature("enable_feature_matches"))
):
    """
    Home page displaying recent matches.
    """
    # If the dependency returned a RedirectResponse (because feature is disabled),
    # we need to return it here. FastAPI dependencies usually don't return values 
    # that interrupt the request flow unless they raise exceptions, but we can 
    # check the result manually if needed, or rely on the exception strategy.
    
    # However, the cleaner way in FastAPI for redirects inside dependencies 
    # is to raise an HTTPException with a redirect, or just handle logic here.
    # Let's simplify: check the flag explicitly for the redirect case.
    if not settings.enable_feature_matches:
        return RedirectResponse(url="/blog")

    # Query recent completed matches
    # We join everything needed for display to avoid N+1 queries
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
        "content": html_content,
    }
    
    return templates.TemplateResponse(
        "blog_post.html",
        {
            "request": request,
            "post": post_data,
            "now": datetime.utcnow(),
        },
    )

# Only for debugging
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("teelo.web.main:app", host="0.0.0.0", port=8000, reload=True)
