# =============================================================================
# Tweet / Social Content Activity
# =============================================================================

async def admin_tweet_activity(
    request: Request,
    page: int = Query(1, ge=1),
    channel: Optional[str] = Query(None),
    content_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    redirect = _require_admin(request, db)
    if redirect:
        return redirect

    per_page = 50
    offset = (page - 1) * per_page

    total = content_item_count(db, channel=channel, content_type=content_type, status=status)
    items = list_content_items(
        db,
        channel=channel,
        content_type=content_type,
        status=status,
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