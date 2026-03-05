from datetime import datetime
from typing import Any, Optional
from urllib.parse import quote

from fastapi import Request
from fastapi.responses import RedirectResponse
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from teelo.db.models import AdminUser, Match, Player, PlayerAlias, PlayerEloState
from teelo.web.app_context import ADMIN_SESSION_KEY
from teelo.web.services.match_service import round_elo


def current_admin_user(request: Request, db: Session) -> Optional[AdminUser]:
    admin_id = request.session.get(ADMIN_SESSION_KEY)
    if not admin_id:
        return None
    return db.query(AdminUser).filter(AdminUser.id == admin_id, AdminUser.is_active.is_(True)).first()


def require_admin(request: Request, db: Session) -> Optional[RedirectResponse]:
    if current_admin_user(request, db) is not None:
        return None
    return RedirectResponse(url=f"/admin/login?next={quote(request.url.path, safe='/')}", status_code=303)


def invalid_match_conditions() -> dict[str, Any]:
    return {
        "missing_players": or_(Match.player_a_id.is_(None), Match.player_b_id.is_(None)),
        "same_players": and_(Match.player_a_id.isnot(None), Match.player_a_id == Match.player_b_id),
        "winner_not_in_players": and_(Match.winner_id.isnot(None), Match.winner_id.notin_([Match.player_a_id, Match.player_b_id])),
    }


def invalid_match_filter() -> Any:
    return or_(*invalid_match_conditions().values())


def invalid_match_reasons(match: Match) -> list[str]:
    reasons = []
    if match.player_a_id is None or match.player_b_id is None:
        reasons.append("Missing player ids")
    if match.player_a_id is not None and match.player_a_id == match.player_b_id:
        reasons.append("Player A equals Player B")
    if match.winner_id is not None and match.winner_id not in {match.player_a_id, match.player_b_id}:
        reasons.append("Winner is not Player A or Player B")
    return reasons


def invalid_matches_redirect(message: str, page: Optional[str], per_page: Optional[str]) -> RedirectResponse:
    query = f"notice={quote(message, safe='')}"
    if page and page.isdigit():
        query += f"&page={page}"
    if per_page and per_page.isdigit():
        query += f"&per_page={per_page}"
    return RedirectResponse(url=f"/admin/invalid-matches?{query}", status_code=303)


def admin_action_redirect(message: str, page: Optional[str], per_page: Optional[str]) -> RedirectResponse:
    query = f"notice={quote(message, safe='')}"
    if page and page.isdigit():
        query += f"&page={page}"
    if per_page and per_page.isdigit():
        query += f"&per_page={per_page}"
    return RedirectResponse(url=f"/admin/duplicates?{query}", status_code=303)
