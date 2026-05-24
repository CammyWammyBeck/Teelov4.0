"""Route handlers for the admin tweet activity dashboard."""
from __future__ import annotations

from datetime import datetime

from fastapi import Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from teelo.db.session import get_db
from teelo.web.app_context import templates
from teelo.web.services.main_handlers import _current_admin_user, _require_admin
from teelo.web.services.tweet_activity_service import (
    content_item_count,
    get_content_item_by_key,
    list_content_items,
)

# =============================================================================
# Tweet / Social Content Activity
# =============================================================================

async def admin_tweet_activity(
    request: Request,
    page: int = Query(1, ge=1),
    channel: str | None = Query(None),
    content_type: str | None = Query(None),
    status: str | None = Query(None),
    q: str | None = Query(None),
    db: Session = Depends(get_db),
):
    redirect = _require_admin(request, db)
    if redirect:
        return redirect

    per_page = 50
    offset = (page - 1) * per_page

    total = content_item_count(
        db,
        channel=channel,
        content_type=content_type,
        status=status,
        query=q,
    )
    items = list_content_items(
        db,
        channel=channel,
        content_type=content_type,
        status=status,
        query=q,
        limit=per_page,
        offset=offset,
    )
    total_pages = max(1, (total + per_page - 1) // per_page)

    admin = _current_admin_user(request, db)
    return templates.TemplateResponse(
        "admin_tweet_activity.html",
        {
            "request": request,
            "admin": admin,
            "items": items,
            "page": page,
            "total_pages": total_pages,
            "total": total,
            "channel": channel,
            "content_type": content_type,
            "status": status,
            "q": q,
            "now": datetime.utcnow(),
            "current_path": request.url.path,
        },
    )


async def admin_tweet_activity_detail(
    request: Request,
    content_key: str,
    db: Session = Depends(get_db),
):
    redirect = _require_admin(request, db)
    if redirect:
        return redirect

    item = get_content_item_by_key(db, content_key)
    if not item:
        raise HTTPException(status_code=404, detail=f"Content item '{content_key}' not found")

    admin = _current_admin_user(request, db)
    return templates.TemplateResponse(
        "admin_tweet_activity_detail.html",
        {
            "request": request,
            "admin": admin,
            "item": item,
            "current_path": request.url.path,
        },
    )
