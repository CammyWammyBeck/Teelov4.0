from datetime import date, timedelta

from sqlalchemy import and_, case, func, or_
from sqlalchemy.orm import Session

from teelo.db.models import Match, Player, PlayerEloState, PlayerSurfaceEloState, Tournament, TournamentEdition
from teelo.web.services.match_service import round_elo


def build_gender_counts_subquery(db: Session):
    events_a = (
        db.query(Match.player_a_id.label("pid"), Tournament.gender.label("gender"))
        .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .filter(Tournament.gender.in_(("men", "women")))
    )
    events_b = (
        db.query(Match.player_b_id.label("pid"), Tournament.gender.label("gender"))
        .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .filter(Tournament.gender.in_(("men", "women")))
    )
    gender_events = events_a.union_all(events_b).subquery()
    return (
        db.query(
            gender_events.c.pid.label("pid"),
            func.sum(case((gender_events.c.gender == "men", 1), else_=0)).label("men_matches"),
            func.sum(case((gender_events.c.gender == "women", 1), else_=0)).label("women_matches"),
        )
        .group_by(gender_events.c.pid)
        .subquery()
    )


def serialize_rankings(results, resolved_surface, offset):
    players_data = []
    for i, row in enumerate(results):
        if resolved_surface is None:
            player, elo_state = row
            last_date = elo_state.last_match_date
        else:
            player, surface_elo_state, overall_elo_state = row
            elo_state = surface_elo_state
            last_date = overall_elo_state.last_match_date
        players_data.append({
            "rank": offset + i + 1,
            "id": player.id,
            "name": player.canonical_name,
            "nationality": player.nationality_ioc,
            "rating": round_elo(elo_state.rating),
            "match_count": elo_state.match_count,
            "career_peak": round_elo(elo_state.career_peak),
            "last_match_date": last_date.isoformat() if last_date else None,
            "last_match_display": last_date.strftime("%d %b %Y") if last_date else None,
        })
    return players_data
