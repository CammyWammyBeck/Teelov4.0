from datetime import date, timedelta
from typing import Any, Optional

from sqlalchemy import and_, case, func, or_
from sqlalchemy.orm import Session

from teelo.db.models import Match, Player, PlayerEloState, PlayerSurfaceEloState, Tournament, TournamentEdition
from teelo.match_statuses import get_status_group
from teelo.web.services.match_service import round_elo
from teelo.web.services.ranking_service import build_gender_counts_subquery


def resolve_history_range(range_value: str) -> Optional[date]:
    today = date.today()
    normalized = (range_value or "1y").strip().lower()
    if normalized == "30d":
        return today - timedelta(days=30)
    if normalized == "90d":
        return today - timedelta(days=90)
    if normalized == "1y":
        return today - timedelta(days=365)
    if normalized == "3y":
        return today - timedelta(days=365 * 3)
    if normalized == "career":
        return None
    return today - timedelta(days=365)


def build_player_profile_payload(player: Player) -> dict[str, Any]:
    birth_date = player.birth_date
    age_years = None
    if birth_date:
        today = date.today()
        age_years = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    return {"id": player.id, "name": player.canonical_name, "nationality": player.nationality_ioc, "birth_date": birth_date.isoformat() if birth_date else None, "age_years": age_years, "hand": player.hand, "backhand": player.backhand, "height_cm": player.height_cm, "turned_pro_year": player.turned_pro_year}


def build_player_overall_elo_payload(db: Session, player: Player, player_id: int) -> dict[str, Any]:
    elo_state = db.query(PlayerEloState).filter(PlayerEloState.player_id == player_id).first()
    overall_rank = None
    if elo_state:
        better_count = db.query(func.count(PlayerEloState.id)).join(Player, Player.id == PlayerEloState.player_id).filter(or_(PlayerEloState.rating > elo_state.rating, and_(PlayerEloState.rating == elo_state.rating, Player.canonical_name < player.canonical_name))).scalar() or 0
        overall_rank = int(better_count) + 1
    return {"rating": round_elo(elo_state.rating) if elo_state else None, "rank": overall_rank, "career_peak": round_elo(elo_state.career_peak) if elo_state else None, "match_count": int(elo_state.match_count) if elo_state else 0, "last_match_date": elo_state.last_match_date.isoformat() if elo_state and elo_state.last_match_date else None}


def build_player_surface_elo_payload(db: Session, player_id: int) -> list[dict[str, Any]]:
    rows = db.query(PlayerSurfaceEloState).filter(PlayerSurfaceEloState.player_id == player_id).order_by(PlayerSurfaceEloState.rating.desc(), PlayerSurfaceEloState.surface.asc()).all()
    return [{"surface": row.surface, "rating": round_elo(row.rating), "rank": None, "career_peak": round_elo(row.career_peak), "match_count": int(row.match_count), "last_match_date": row.last_match_date.isoformat() if row.last_match_date else None} for row in rows]


def compute_record_block(db: Session, player_id: int, statuses: list[str], date_from: Optional[date] = None) -> dict:
    query = db.query(func.count(Match.id).label("played"), func.sum(case((Match.winner_id == player_id, 1), else_=0)).label("wins")).filter(Match.status.in_(statuses), Match.winner_id.isnot(None), or_(Match.player_a_id == player_id, Match.player_b_id == player_id))
    if date_from is not None:
        query = query.filter(Match.match_date >= date_from)
    row = query.one()
    played = int(row.played or 0)
    wins = int(row.wins or 0)
    return {"wins": wins, "losses": max(played - wins, 0), "played": played}


def build_player_records_payload(db: Session, player_id: int) -> dict[str, Any]:
    statuses = get_status_group("historical_default")
    return {
        "career": compute_record_block(db, player_id, statuses),
        "last_52_weeks": compute_record_block(db, player_id, statuses, date.today() - timedelta(days=364)),
    }
