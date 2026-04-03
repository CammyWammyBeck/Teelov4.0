from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, joinedload

from teelo.db.models import Match, Tournament, TournamentEdition
from teelo.elo.boost import calculate_k_boost
from teelo.elo.calculator import calculate_fast
from teelo.elo.constants import get_level_code
from teelo.elo.decay import apply_inactivity_decay
from teelo.elo.margin import calculate_margin_multiplier
from teelo.elo.params_store import get_active_elo_params
from teelo.elo.pipeline import initial_elo_for_level_code
from teelo.elo.updater import resolve_surface_bucket
from teelo.features.engine import _compute_games, _compute_score_summary, _expected_win_probability
from teelo.features.state import H2HRecord, MatchContext, MatchRecord, PlayerState


SYNTHETIC_SCORE_STRUCTURED_A_WINS: list[dict[str, int]] = [
    {"a": 6, "b": 4},
    {"a": 6, "b": 4},
]


def player_state_to_json(state: PlayerState) -> dict[str, Any]:
    def _to_jsonable_mapping(obj: Any) -> dict[str, Any]:
        # H2H records historically used NamedTuple (H2HRecord); tolerate
        # dataclasses and already-serialized dicts too.
        if obj is None:
            return {}
        if isinstance(obj, dict):
            return obj
        if is_dataclass(obj):
            return asdict(obj)
        if hasattr(obj, "_asdict"):
            return dict(obj._asdict())
        try:
            return dict(obj)  # type: ignore[arg-type]
        except Exception as e:  # pragma: no cover
            raise TypeError(f"Unsupported record type for JSON serialization: {type(obj)!r}") from e

    return {
        "player_id": state.player_id,
        "elo_current": state.elo_current,
        "elo_peak": state.elo_peak,
        "elo_history": list(state.elo_history),
        "surface_elo": dict(state.surface_elo),
        "surface_elo_peak": dict(state.surface_elo_peak),
        "matches": [m._asdict() for m in list(state.matches)],
        "first_match_date": state.first_match_date.isoformat() if state.first_match_date else None,
        "last_match_date": state.last_match_date.isoformat() if state.last_match_date else None,
        "wins_total": state.wins_total,
        "losses_total": state.losses_total,
        "surface_wins": dict(state.surface_wins),
        "surface_losses": dict(state.surface_losses),
        "level_wins": dict(state.level_wins),
        "level_losses": dict(state.level_losses),
        "h2h": {str(k): [_to_jsonable_mapping(r) for r in v] for k, v in state.h2h.items()},

        "tournament_matches": dict(state.tournament_matches),
        "tournament_wins": dict(state.tournament_wins),
        "tournament_losses": dict(state.tournament_losses),
        "clutch_score": state.clutch_score,
        "country_record": dict(state.country_record),
        "region_record": dict(state.region_record),
    }


def player_state_from_json(payload: dict[str, Any]) -> PlayerState:
    state = PlayerState(player_id=int(payload["player_id"]))
    state.elo_current = float(payload.get("elo_current", 1500.0))
    state.elo_peak = float(payload.get("elo_peak", state.elo_current))
    for temporal_order, elo in payload.get("elo_history", []) or []:
        state.elo_history.append((int(temporal_order), float(elo)))
    state.surface_elo.update({k: float(v) for k, v in (payload.get("surface_elo") or {}).items()})
    state.surface_elo_peak.update(
        {k: float(v) for k, v in (payload.get("surface_elo_peak") or {}).items()}
    )

    for m in payload.get("matches", []) or []:
        # MatchRecord keys are stable in code; tolerate missing optional keys.
        state.matches.append(MatchRecord(**m))

    fmd = payload.get("first_match_date")
    lmd = payload.get("last_match_date")
    state.first_match_date = date.fromisoformat(fmd) if fmd else None
    state.last_match_date = date.fromisoformat(lmd) if lmd else None

    state.wins_total = int(payload.get("wins_total", 0))
    state.losses_total = int(payload.get("losses_total", 0))

    state.surface_wins.update(payload.get("surface_wins") or {})
    state.surface_losses.update(payload.get("surface_losses") or {})
    state.level_wins.update(payload.get("level_wins") or {})
    state.level_losses.update(payload.get("level_losses") or {})

    # H2H keys are serialized as strings.
    for opp_id_str, records in (payload.get("h2h") or {}).items():
        try:
            opp_id = int(opp_id_str)
        except (TypeError, ValueError):
            continue
        for r in records or []:
            if isinstance(r, H2HRecord):
                state.h2h[opp_id].append(r)
            else:
                # tolerate dict payloads
                state.h2h[opp_id].append(H2HRecord(**r))

    state.tournament_matches.update(payload.get("tournament_matches") or {})
    state.tournament_wins.update(payload.get("tournament_wins") or {})
    state.tournament_losses.update(payload.get("tournament_losses") or {})

    state.clutch_score = payload.get("clutch_score")
    state.country_record.update(payload.get("country_record") or {})
    state.region_record.update(payload.get("region_record") or {})
    return state


def build_base_states_for_players(
    session: Session,
    *,
    edition: TournamentEdition,
    player_ids: set[int],
) -> dict[int, PlayerState]:
    """Build PlayerState objects from historical terminal matches up to this edition.

    This replays terminal matches involving the draw players, excluding any matches
    in this edition. It is intentionally scoped to a small player set.
    """

    if not player_ids:
        return {}

    tournament = session.execute(
        select(Tournament).where(Tournament.id == edition.tournament_id)
    ).scalar_one()
    level_code = get_level_code(tournament.level, tournament.tour)

    # Load terminal matches for these players, excluding current edition.
    terminal_statuses = ("completed", "retired", "walkover", "default")
    q = (
        session.query(Match)
        .options(joinedload(Match.tournament_edition).joinedload(TournamentEdition.tournament))
        .filter(Match.status.in_(terminal_statuses))
        .filter(Match.winner_id.isnot(None))
        .filter(Match.tournament_edition_id.isnot(None))
        .filter(Match.tournament_edition_id != edition.id)
        .filter(or_(Match.player_a_id.in_(player_ids), Match.player_b_id.in_(player_ids)))
        .order_by(Match.temporal_order.asc(), Match.id.asc())
    )

    states: dict[int, PlayerState] = {pid: PlayerState(player_id=pid) for pid in player_ids}

    for match in q.all():
        # Determine match context fields from joined tournament.
        te = match.tournament_edition
        t = te.tournament if te else None
        surface = (te.surface if te else None) or (t.surface if t else None)
        lvl_code = get_level_code(t.level, t.tour) if t else level_code

        # Update both players if we track them.
        for is_a in (True, False):
            pid = match.player_a_id if is_a else match.player_b_id
            if pid not in states:
                continue
            opp_id = match.player_b_id if is_a else match.player_a_id
            state = states[pid]

            games_a, games_b = _compute_games(match.score_structured)
            sets_a, sets_b, tba, tbb = _compute_score_summary(match.score_structured)
            player_won = match.winner_id == pid

            if is_a:
                games_won, games_lost = games_a, games_b
                sets_won, sets_lost = sets_a, sets_b
                elo_pre = float(match.elo_pre_player_a or state.elo_current)
                elo_post = float(match.elo_post_player_a or state.elo_current)
                opp_elo = float(match.elo_pre_player_b or 1500.0)
            else:
                games_won, games_lost = games_b, games_a
                sets_won, sets_lost = sets_b, sets_a
                elo_pre = float(match.elo_pre_player_b or state.elo_current)
                elo_post = float(match.elo_post_player_b or state.elo_current)
                opp_elo = float(match.elo_pre_player_a or 1500.0)

            # Synthetic flags are derived for real matches.
            deciding_set_played = (sets_a + sets_b) >= 3 and abs(sets_a - sets_b) == 1
            straight_sets = player_won and (sets_lost == 0) and sets_won >= 2
            close_match = deciding_set_played or (tba + tbb) > 0

            first_set_lost = False
            if match.score_structured and len(match.score_structured) > 0:
                first_set = match.score_structured[0]
                fs_me = first_set.get("a" if is_a else "b", 0)
                fs_opp = first_set.get("b" if is_a else "a", 0)
                first_set_lost = fs_me < fs_opp

            expected = _expected_win_probability(elo_pre, opp_elo)

            record = MatchRecord(
                temporal_order=match.temporal_order or 0,
                won=player_won,
                surface=surface,
                level_code=lvl_code,
                games_won=games_won,
                games_lost=games_lost,
                tournament_edition_id=match.tournament_edition_id,
                tournament_id=t.id if t else None,
                match_date=match.match_date,
                opponent_id=opp_id,
                opponent_elo=opp_elo,
                expected_win_prob=expected,
                sets_won=sets_won,
                sets_lost=sets_lost,
                tiebreaks_played=tba + tbb,
                tiebreaks_won=tba if is_a else tbb,
                deciding_set_played=deciding_set_played,
                straight_sets=straight_sets,
                close_match=close_match,
                first_set_lost=first_set_lost,
                country_ioc=getattr(t, "country_ioc", None) if t else None,
            )
            state.update(record, elo_post=elo_post, surface_elo_post=None)

    return states


def derive_winner_state(
    session: Session,
    *,
    winner_pre: PlayerState,
    loser_pre: PlayerState,
    ctx: MatchContext,
    tournament: Tournament,
    edition: TournamentEdition,
    winner_is_a: bool,
    score_structured: list[dict[str, int]] | None = None,
) -> tuple[PlayerState, PlayerState]:
    """Apply a match result (real or synthetic) to two pre-match states.

    - Hypothetical matches use the synthetic defaults unless score_structured is provided.
    - Hypothetical ELO updates use the same calculation path as EloUpdater (params/decay/boost/margin).

    Returns: (winner_post, loser_post)
    """

    # Copy states (avoid mutating input)
    import copy

    winner = copy.deepcopy(winner_pre)
    loser = copy.deepcopy(loser_pre)

    score_structured = score_structured or SYNTHETIC_SCORE_STRUCTURED_A_WINS
    # Orient score so that "A" in score corresponds to the winner.
    if not winner_is_a:
        score_structured = [{"a": s["b"], "b": s["a"]} for s in score_structured]

    # ELO calculations
    params, _version = get_active_elo_params(session)

    # Resolve surface bucket consistent with ELO updater.
    surface_bucket = resolve_surface_bucket(
        edition.surface,
        tournament.surface,
        tournament.indoor_outdoor,
    )

    match_date = ctx.match_date or edition.start_date or date.today()

    def _days_since(last: date | None) -> float | None:
        if last is None:
            return None
        return float((match_date - last).days)

    winner_days = _days_since(winner.last_match_date)
    loser_days = _days_since(loser.last_match_date)

    winner_rating = apply_inactivity_decay(
        winner.elo_current,
        days_since_last_match=winner_days or 0.0,
        decay_rate=params.decay_rate,
        decay_start_days=params.decay_start_days,
        target_rating=initial_elo_for_level_code(params, get_level_code(tournament.level, tournament.tour)),
    )
    loser_rating = apply_inactivity_decay(
        loser.elo_current,
        days_since_last_match=loser_days or 0.0,
        decay_rate=params.decay_rate,
        decay_start_days=params.decay_start_days,
        target_rating=initial_elo_for_level_code(params, get_level_code(tournament.level, tournament.tour)),
    )

    margin = calculate_margin_multiplier(
        score_structured,
        winner="A",
        margin_base=params.margin_base,
        margin_scale=params.margin_scale,
    )

    # Base K and S are based on tournament level_code
    level_code = get_level_code(tournament.level, tournament.tour)
    base_k = params.get_k(level_code)
    s = params.get_s(level_code)

    # Boost factors based on match counts and inactivity.
    winner_boost = calculate_k_boost(
        len(winner.matches),
        winner_days,
        new_threshold=params.new_threshold,
        new_boost=params.new_boost,
        returning_days=params.returning_days,
        returning_boost=params.returning_boost,
    )
    loser_boost = calculate_k_boost(
        len(loser.matches),
        loser_days,
        new_threshold=params.new_threshold,
        new_boost=params.new_boost,
        returning_days=params.returning_days,
        returning_boost=params.returning_boost,
    )

    k_w = float(base_k) * float(margin.multiplier) * float(winner_boost)
    k_l = float(base_k) * float(margin.multiplier) * float(loser_boost)

    new_w, new_l, expected_winner_prob = calculate_fast(
        winner_rating,
        loser_rating,
        winner="A",
        k_a=k_w,
        k_b=k_l,
        s=float(s),
    )

    # Build MatchRecord updates for both players.
    games_w, games_l = _compute_games(score_structured)
    sets_w, sets_l, tb_w, tb_l = _compute_score_summary(score_structured)

    record_w = MatchRecord(
        temporal_order=ctx.temporal_order or 0,
        won=True,
        surface=surface_bucket,
        level_code=level_code,
        games_won=games_w,
        games_lost=games_l,
        tournament_edition_id=edition.id,
        tournament_id=tournament.id,
        match_date=match_date,
        opponent_id=loser.player_id,
        opponent_elo=loser_rating,
        expected_win_prob=float(expected_winner_prob),
        sets_won=sets_w,
        sets_lost=sets_l,
        tiebreaks_played=0,
        tiebreaks_won=0,
        deciding_set_played=False,
        straight_sets=True,
        close_match=False,
        first_set_lost=False,
        country_ioc=tournament.country_ioc,
    )
    record_l = MatchRecord(
        temporal_order=ctx.temporal_order or 0,
        won=False,
        surface=surface_bucket,
        level_code=level_code,
        games_won=games_l,
        games_lost=games_w,
        tournament_edition_id=edition.id,
        tournament_id=tournament.id,
        match_date=match_date,
        opponent_id=winner.player_id,
        opponent_elo=winner_rating,
        expected_win_prob=float(expected_winner_prob),
        sets_won=sets_l,
        sets_lost=sets_w,
        tiebreaks_played=0,
        tiebreaks_won=0,
        deciding_set_played=False,
        straight_sets=False,
        close_match=False,
        first_set_lost=False,
        country_ioc=tournament.country_ioc,
    )

    winner.update(record_w, elo_post=float(new_w), surface_elo_post=None)
    loser.update(record_l, elo_post=float(new_l), surface_elo_post=None)

    return winner, loser
