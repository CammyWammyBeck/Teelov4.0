from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session, contains_eager, joinedload

from teelo.db.models import Match, MatchFeatures, Player, PlayerAlias, Tournament, TournamentEdition
from teelo.db.session import get_db
from teelo.features import build_registry, default_preset_for_feature_set
from teelo.match_statuses import normalize_status_filter
from teelo.web.app_context import templates
from teelo.web.services.feature_display import (
    build_feature_groups,
    format_feature_value,
    swap_feature_sides,
)
from teelo.web.services.match_service import resolve_date_preset, serialize_match

router = APIRouter()


@router.get('/api/matches')
async def api_matches(db: Session = Depends(get_db), tour: Optional[str] = Query(None), gender: Optional[str] = Query(None), surface: Optional[str] = Query(None), level: Optional[str] = Query(None), round: Optional[str] = Query(None), status: Optional[str] = Query(None), player: Optional[str] = Query(None), player_id: Optional[int] = Query(None), player_a_id: Optional[int] = Query(None), player_b_id: Optional[int] = Query(None), tournament: Optional[str] = Query(None), date_from: Optional[str] = Query(None), date_to: Optional[str] = Query(None), date_preset: Optional[str] = Query(None), page: int = Query(1, ge=1), per_page: int = Query(50, ge=1, le=100)):
    # Track whether we need tournament/edition joins for filtering
    needs_tournament_join = bool(tour or gender or level or tournament or surface)
    needs_edition_join = bool(surface) or needs_tournament_join

    def _apply_joins(q, *, for_count=False):
        if needs_edition_join or not for_count:
            q = q.outerjoin(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        if needs_tournament_join or not for_count:
            q = q.outerjoin(Tournament, TournamentEdition.tournament_id == Tournament.id)
        return q

    raw_statuses = status.split(",") if status else None
    statuses = normalize_status_filter(raw_statuses)

    # Build shared filter predicates (applied to both count and fetch queries)
    def _apply_filters(q, player_subquery=None):
        q = q.filter(Match.status.in_(statuses))
        if tour:
            q = q.filter(Tournament.tour.in_([t.strip() for t in tour.split(',')]))
        if gender:
            q = q.filter(Tournament.gender.in_([g.strip().lower() for g in gender.split(',') if g.strip()]))
        if surface:
            surface_list = [s.strip() for s in surface.split(',')]
            q = q.filter(or_(TournamentEdition.surface.in_(surface_list), and_(TournamentEdition.surface.is_(None), Tournament.surface.in_(surface_list))))
        if level:
            q = q.filter(Tournament.level.in_([l.strip() for l in level.split(',')]))
        if round:
            q = q.filter(Match.round.in_([r.strip() for r in round.split(',')]))
        if player_a_id and player_b_id:
            q = q.filter(or_(and_(Match.player_a_id == player_a_id, Match.player_b_id == player_b_id), and_(Match.player_a_id == player_b_id, Match.player_b_id == player_a_id)))
        elif player_id:
            q = q.filter(or_(Match.player_a_id == player_id, Match.player_b_id == player_id))
        elif player_subquery is not None:
            q = q.filter(or_(Match.player_a_id.in_(db.query(player_subquery.c.id)), Match.player_b_id.in_(db.query(player_subquery.c.id))))
        if tournament:
            q = q.filter(Tournament.name.ilike(f"%{tournament}%"))
        if resolved_from:
            q = q.filter(Match.match_date >= resolved_from)
        if resolved_to:
            q = q.filter(Match.match_date <= resolved_to)
        return q

    resolved_from, resolved_to = (None, None)
    if date_preset:
        resolved_from, resolved_to = resolve_date_preset(date_preset)
    if date_from:
        try: resolved_from = date.fromisoformat(date_from)
        except ValueError: pass
    if date_to:
        try: resolved_to = date.fromisoformat(date_to)
        except ValueError: pass

    player_subquery = None
    if player and not player_id and not (player_a_id and player_b_id):
        player_pattern = f"%{player}%"
        player_subquery = db.query(Player.id).filter(Player.canonical_name.ilike(player_pattern)).subquery()

    # Efficient count query: only join what the active filters need
    count_q = _apply_joins(db.query(func.count(Match.id)), for_count=True)
    total = _apply_filters(count_q, player_subquery).scalar()

    # Fetch query: always join for eager loading
    fetch_q = db.query(Match).outerjoin(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id).outerjoin(Tournament, TournamentEdition.tournament_id == Tournament.id).options(joinedload(Match.player_a), joinedload(Match.player_b), contains_eager(Match.tournament_edition).contains_eager(TournamentEdition.tournament))
    fetch_q = _apply_filters(fetch_q, player_subquery)

    offset = (page - 1) * per_page
    matches = fetch_q.order_by(Match.match_date.desc().nullslast(), Match.temporal_order.desc().nullslast(), Match.id.desc()).offset(offset).limit(per_page).all()
    payload = [serialize_match(m) for m in matches]
    t = templates.get_template('partials/match_rows.html')
    return JSONResponse({"matches": payload, "table_rows_html": t.module.render_table_rows(payload), "cards_html": t.module.render_cards(payload), "total": total, "page": page, "per_page": per_page, "has_more": (offset + per_page) < total})


@router.get('/api/players/search')
async def api_players_search(db: Session = Depends(get_db), q: str = Query(..., min_length=2), limit: int = Query(8, ge=1, le=20)):
    pattern = f"%{q}%"
    name_matches = db.query(Player).filter(Player.canonical_name.ilike(pattern)).limit(limit).all()
    alias_matches = db.query(Player).join(PlayerAlias, PlayerAlias.player_id == Player.id).filter(PlayerAlias.alias.ilike(pattern)).limit(limit).all()
    seen_ids = set(); all_players = []
    for p in name_matches + alias_matches:
        if p.id not in seen_ids:
            seen_ids.add(p.id); all_players.append(p)
    all_players.sort(key=lambda p: (0 if p.canonical_name.lower().startswith(q.lower()) else 1, p.canonical_name.lower()))
    return JSONResponse({"players": [{"id": p.id, "name": p.canonical_name, "nationality": p.nationality_ioc} for p in all_players[:limit]]})


@router.get('/matches/{match_id}')
async def match_detail(match_id: int, request: Request, db: Session = Depends(get_db)):
    match = (
        db.query(Match)
        .options(
            joinedload(Match.player_a),
            joinedload(Match.player_b),
            joinedload(Match.tournament_edition).joinedload(TournamentEdition.tournament),
        )
        .filter(Match.id == match_id)
        .first()
    )
    if not match:
        return templates.TemplateResponse("404.html", {"request": request}, status_code=404)

    match_data = serialize_match(match)

    # Load feature data (most recent feature set)
    match_features = (
        db.query(MatchFeatures)
        .options(joinedload(MatchFeatures.feature_set))
        .filter(MatchFeatures.match_id == match_id)
        .order_by(MatchFeatures.computed_at.desc())
        .first()
    )

    feature_groups = []
    feature_set_info = None
    if match_features and match_features.features:
        feature_set = match_features.feature_set
        feature_set_info = {
            "name": feature_set.name,
            "version": feature_set.version,
            "description": feature_set.description,
        }
        preset = default_preset_for_feature_set(feature_set.name)
        registry = build_registry(preset)
        grouped = registry.grouped_features()
        neutral = registry.neutral_groups()
        features = match_features.features
        if match_data.get("swap_display_sides"):
            features = swap_feature_sides(features)
        feature_groups = build_feature_groups(
            features, grouped, neutral,
        )

    return templates.TemplateResponse("match_detail.html", {
        "request": request,
        "match": match_data,
        "feature_groups": feature_groups,
        "feature_set": feature_set_info,
        "format_value": format_feature_value,
        "now": datetime.utcnow(),
        "current_path": request.url.path,
    })
