from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import structlog
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session, joinedload

from teelo.db.models import (
    Match,
    Player,
    Tournament,
    TournamentEdition,
    TournamentForecastNode,
    TournamentForecastRun,
)
from teelo.draw import ROUND_PROGRESSION, get_expected_matches_in_round, get_feeder_positions, get_next_round
from teelo.elo.constants import get_level_code
from teelo.features.state import MatchContext
from teelo.match_statuses import get_status_group
from teelo.ml.versioning import latest_feature_set
from teelo.services.forecast_prediction import (
    ForecastModel,
    build_features_for_states,
    load_forecast_model,
    predict_probability_a,
    reuse_or_predict_real_match,
)
from teelo.services.forecast_state_builder import (
    build_base_states_for_players,
    derive_winner_state,
    player_state_from_json,
    player_state_to_json,
)

logger = structlog.get_logger(__name__)


def _json_sanitize(value: Any) -> Any:
    """Recursively coerce common Python objects into JSON-serialisable primitives.

    This is used for persisting `features_json` into JSONB. Real-world feature payloads may
    include `date`/`datetime` objects (eg match_date in nested match lists), which PostgreSQL's
    JSON encoder (via SQLAlchemy) will reject.

    Keep this intentionally small/surgical: only normalise non-JSON primitives.
    """

    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    # Datetime-like.
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    # Containers.
    if isinstance(value, dict):
        return {str(k): _json_sanitize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_sanitize(v) for v in value]
    if isinstance(value, tuple) or isinstance(value, set):
        return [_json_sanitize(v) for v in value]

    # Last resort: attempt to stringify; better than hard-failing a forecast run.
    # (We still preserve structure everywhere else.)
    return str(value)


MAIN_DRAW_ROUNDS: tuple[str, ...] = ("R128", "R64", "R32", "R16", "QF", "SF", "F")


def is_forecast_eligible_tournament(tournament: Tournament) -> bool:
    """Eligibility filter (locked v1): ATP/WTA main tour only."""

    tour = (tournament.tour or "").strip().upper()
    if tour not in {"ATP", "WTA"}:
        return False

    # Explicit exclusions even if fields are ambiguous.
    if tournament.level in {"Challenger", "WTA 125", "ITF", "ITF Men", "ITF Women"}:
        return False

    if tour == "ATP" and tournament.level in {"Grand Slam", "Masters 1000", "ATP 500", "ATP 250", "ATP Finals"}:
        return True
    if tour == "WTA" and tournament.level in {"Grand Slam", "WTA 1000", "WTA 500", "WTA 250", "WTA Finals"}:
        return True

    # Default deny.
    return False


def determine_entry_round(session: Session, edition: TournamentEdition) -> str | None:
    """Determine the effective entry round from existing main-draw matches.

    We prefer DB reality over `edition.draw_size` because draw_size can be stale/mis-set after
    migrations, and some scraped editions may only contain matches starting from a later round.

    Returns the earliest round (by ROUND_PROGRESSION ordering) that exists with a draw_position.
    Falls back to the draw_size-based mapping if no matches exist yet.
    """

    # Prefer DB reality: earliest round that exists.
    rounds = (
        session.query(Match.round)
        .filter(
            Match.tournament_edition_id == edition.id,
            Match.draw_position.isnot(None),
            Match.round.in_(ROUND_PROGRESSION),
        )
        .distinct()
        .all()
    )
    existing = [r[0] for r in rounds if r and r[0] in ROUND_PROGRESSION]
    if existing:
        return min(existing, key=lambda rc: ROUND_PROGRESSION.index(rc))

    from teelo.draw import get_first_round_for_draw_size

    if edition.draw_size is None:
        return None
    return get_first_round_for_draw_size(int(edition.draw_size))


def is_draw_forecast_ready(session: Session, edition: TournamentEdition) -> bool:
    """Return True once SECOND-round slots are structurally resolvable.

    Rule: forecast should trigger once every slot in the second round has both incoming sides known.

    This does *not* require first-round matches to be completed.

    Implementation detail:
    - A side is "known" if its feeder slot in the entry round exists as a Match with both players.
      Byes/auto-advances are expected to be materialised as synthetic entry-round matches
      (player vs BYE), so they count as known.
    - A fully populated second-round match also counts as resolvable (covers pre-populated /
      bye-vs-bye style edges).
    """

    tournament = session.execute(
        select(Tournament).where(Tournament.id == edition.tournament_id)
    ).scalar_one()

    if not is_forecast_eligible_tournament(tournament):
        return False

    entry_round = determine_entry_round(session, edition)
    if entry_round is None:
        return False
    next_round = get_next_round(entry_round)
    if next_round is None:
        return False

    expected_next = get_expected_matches_in_round(next_round)
    if expected_next is None:
        return False

    entry_matches = (
        session.query(Match)
        .filter(
            Match.tournament_edition_id == edition.id,
            Match.round == entry_round,
            Match.draw_position.isnot(None),
            Match.player_a_id.isnot(None),
            Match.player_b_id.isnot(None),
        )
        .all()
    )
    entry_known_positions = {int(m.draw_position) for m in entry_matches if m.draw_position is not None}

    next_matches = (
        session.query(Match)
        .filter(
            Match.tournament_edition_id == edition.id,
            Match.round == next_round,
            Match.draw_position.isnot(None),
            Match.player_a_id.isnot(None),
            Match.player_b_id.isnot(None),
        )
        .all()
    )
    next_full_positions = {int(m.draw_position) for m in next_matches if m.draw_position is not None}

    for pos in range(1, expected_next + 1):
        if pos in next_full_positions:
            continue

        f1, f2 = get_feeder_positions(pos)
        if f1 not in entry_known_positions or f2 not in entry_known_positions:
            return False

    return True


def _hash_dict(payload: Any) -> str:
    raw = repr(payload).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:64]


def compute_structure_signature(session: Session, edition: TournamentEdition, model: ForecastModel) -> str:
    entry_round = determine_entry_round(session, edition)
    rounds = ROUND_PROGRESSION[ROUND_PROGRESSION.index(entry_round) :] if entry_round in ROUND_PROGRESSION else list(MAIN_DRAW_ROUNDS)

    matches = (
        session.query(Match)
        .filter(
            Match.tournament_edition_id == edition.id,
            Match.draw_position.isnot(None),
            Match.round.in_(rounds),
        )
        .order_by(Match.round.asc(), Match.draw_position.asc(), Match.id.asc())
        .all()
    )
    structure = [
        {
            "round": m.round,
            "pos": m.draw_position,
            "a": m.player_a_id,
            "b": m.player_b_id,
        }
        for m in matches
    ]
    payload = {
        "edition_id": edition.id,
        "structure": structure,
        "feature_set": model.feature_set_name,
        "model_version": model.model_version,
    }
    return _hash_dict(payload)


def compute_state_signature(session: Session, edition: TournamentEdition) -> str:
    # Results/statuses matter for probabilities.
    entry_round = determine_entry_round(session, edition)
    rounds = ROUND_PROGRESSION[ROUND_PROGRESSION.index(entry_round) :] if entry_round in ROUND_PROGRESSION else list(MAIN_DRAW_ROUNDS)

    matches = (
        session.query(Match)
        .filter(
            Match.tournament_edition_id == edition.id,
            Match.draw_position.isnot(None),
            Match.round.in_(rounds),
        )
        .order_by(Match.round.asc(), Match.draw_position.asc(), Match.id.asc())
        .all()
    )
    payload = {
        "edition_id": edition.id,
        "states": [
            {
                "id": m.id,
                "round": m.round,
                "pos": m.draw_position,
                "status": m.status,
                "winner_id": m.winner_id,
            }
            for m in matches
        ],
    }
    return _hash_dict(payload)


def get_active_run(session: Session, edition_id: int) -> TournamentForecastRun | None:
    return (
        session.query(TournamentForecastRun)
        .filter(
            TournamentForecastRun.tournament_edition_id == edition_id,
            TournamentForecastRun.is_active.is_(True),
            TournamentForecastRun.status == "ready",
        )
        .order_by(TournamentForecastRun.completed_at.desc().nullslast(), TournamentForecastRun.id.desc())
        .first()
    )


def build_forecast_run(
    session: Session,
    *,
    edition_id: int,
    force: bool = False,
    build_reason: str = "initial",
) -> TournamentForecastRun:
    edition = session.execute(
        select(TournamentEdition)
        .options(joinedload(TournamentEdition.tournament))
        .where(TournamentEdition.id == edition_id)
    ).scalar_one()
    tournament = edition.tournament

    if not is_forecast_eligible_tournament(tournament):
        raise ValueError("Tournament not eligible for forecast")

    model = load_forecast_model(feature_set_name=latest_feature_set())

    structure_sig = compute_structure_signature(session, edition, model)
    state_sig = compute_state_signature(session, edition)

    existing = (
        session.query(TournamentForecastRun)
        .filter(
            TournamentForecastRun.tournament_edition_id == edition_id,
            TournamentForecastRun.is_active.is_(True),
            TournamentForecastRun.status == "ready",
        )
        .first()
    )
    if existing and (not force) and existing.structure_signature == structure_sig:
        # Keep state_signature updated for result-only recompute.
        if existing.state_signature != state_sig:
            existing.state_signature = state_sig
            session.add(existing)
        return existing

    # Stale old active runs.
    session.query(TournamentForecastRun).filter(
        TournamentForecastRun.tournament_edition_id == edition_id,
        TournamentForecastRun.is_active.is_(True),
    ).update({TournamentForecastRun.is_active: False, TournamentForecastRun.status: "stale"})

    run = TournamentForecastRun(
        tournament_edition_id=edition_id,
        status="building",
        build_reason=build_reason,
        structure_signature=structure_sig,
        state_signature=state_sig,
        feature_set_name=model.feature_set_name,
        model_version=model.model_version,
        is_active=False,
        started_at=datetime.utcnow(),
    )
    session.add(run)
    session.flush()

    try:
        _materialise_nodes(session, run=run, edition=edition, tournament=tournament, model=model)
        run.status = "ready"
        run.is_active = True
        run.completed_at = datetime.utcnow()
        run.error_text = None
        session.add(run)
        session.flush()
        return run
    except Exception as exc:
        run.status = "failed"
        run.is_active = False
        run.completed_at = datetime.utcnow()
        run.error_text = str(exc)
        session.add(run)
        session.flush()
        raise


def _materialise_nodes(
    session: Session,
    *,
    run: TournamentForecastRun,
    edition: TournamentEdition,
    tournament: Tournament,
    model: ForecastModel,
) -> None:
    """Build nodes round-by-round.

    Correctness notes:
    - Scenario node identity is *path-specific*: node uniqueness includes (left_parent_node_id, right_parent_node_id).
    - Winner state propagation is also path-specific: the same player reaching the same slot via different parent
      nodes can carry a different PlayerState, so downstream matchups must not collapse those states early.
    """

    @dataclass(frozen=True)
    class _Outcome:
        player_id: int
        parent_node_id: int
        state: Any  # PlayerState

    # Load all existing main-draw matches for this edition.
    matches = (
        session.query(Match)
        .filter(
            Match.tournament_edition_id == edition.id,
            Match.round.in_(MAIN_DRAW_ROUNDS),
            Match.draw_position.isnot(None),
        )
        .options(joinedload(Match.player_a), joinedload(Match.player_b))
        .all()
    )
    match_by_slot: dict[tuple[str, int], Match] = {
        (m.round, int(m.draw_position)): m
        for m in matches
        if m.round and m.draw_position is not None
    }

    # Identify all players currently in the main draw.
    player_ids: set[int] = set()
    for m in matches:
        if m.player_a_id is not None:
            player_ids.add(int(m.player_a_id))
        if m.player_b_id is not None:
            player_ids.add(int(m.player_b_id))

    base_states = build_base_states_for_players(session, edition=edition, player_ids=player_ids)

    # Winner outcome distributions per slot.
    # Each entry represents: "player X wins this slot via parent node Y, with resulting post-win state".
    winner_outcomes: dict[tuple[str, int], list[_Outcome]] = {}

    # Node ids per slot (for actual slots this is unique; for scenario slots there are many nodes).
    actual_node_id_by_slot: dict[tuple[str, int], int] = {}

    entry_round = determine_entry_round(session, edition)
    if entry_round is None:
        raise ValueError("Cannot determine entry round for forecast build")

    rounds_to_build = ROUND_PROGRESSION[ROUND_PROGRESSION.index(entry_round) :]

    # Round-by-round processing.
    for round_code in rounds_to_build:
        expected = get_expected_matches_in_round(round_code)
        if expected is None:
            continue

        for pos in range(1, expected + 1):
            slot = (round_code, pos)
            match = match_by_slot.get(slot)

            if match is not None:
                # Actual known matchup.
                if round_code == entry_round:
                    pre_a = base_states.get(match.player_a_id)
                    pre_b = base_states.get(match.player_b_id)
                    left_parent_id = None
                    right_parent_id = None
                else:
                    prev_round = ROUND_PROGRESSION[ROUND_PROGRESSION.index(round_code) - 1]
                    f1, f2 = get_feeder_positions(pos)

                    left_slot = (prev_round, f1)
                    right_slot = (prev_round, f2)

                    left_outs = winner_outcomes.get(left_slot) or []
                    right_outs = winner_outcomes.get(right_slot) or []

                    def _maybe_unique_parent_id(outs: list[_Outcome], player_id: int | None) -> int | None:
                        if player_id is None:
                            return None
                        ids = [int(o.parent_node_id) for o in outs if o.player_id == int(player_id) and o.parent_node_id]
                        uniq = set(ids)
                        return ids[0] if len(uniq) == 1 else None

                    # Real-world draws can have byes / missing upstream matches in DB.
                    # If the prior slot was an actual node, use that. Otherwise, only attach a parent id
                    # when the winner-path is unambiguous; if ambiguous, leave it None (actual nodes don't
                    # require path-specific parent identity to be useful downstream).
                    left_parent_id = actual_node_id_by_slot.get(left_slot) or _maybe_unique_parent_id(left_outs, match.player_a_id)
                    right_parent_id = actual_node_id_by_slot.get(right_slot) or _maybe_unique_parent_id(right_outs, match.player_b_id)

                    pre_a = next((o.state for o in left_outs if o.player_id == match.player_a_id), None)
                    pre_b = next((o.state for o in right_outs if o.player_id == match.player_b_id), None)
                    pre_a = pre_a or base_states.get(match.player_a_id)
                    pre_b = pre_b or base_states.get(match.player_b_id)

                if pre_a is None or pre_b is None:
                    raise ValueError(f"Missing base state for match players in {slot}")

                ctx = _make_match_context(edition, tournament, match, round_code)

                # Ensure real match prediction is present via normal pipeline.
                _features, pred_a, model_version = reuse_or_predict_real_match(
                    session,
                    match,
                    model=model,
                    ctx=ctx,
                    state_a=pre_a,
                    state_b=pre_b,
                )

                node = TournamentForecastNode(
                    forecast_run_id=run.id,
                    round=round_code,
                    draw_position=pos,
                    player_a_id=match.player_a_id,
                    player_b_id=match.player_b_id,
                    left_parent_node_id=left_parent_id,
                    right_parent_node_id=right_parent_id,
                    source_match_id=match.id,
                    node_type="actual",
                    generation_depth=0,
                    feature_set_name=model.feature_set_name,
                    prediction_model_version=model_version,
                    player_a_state_json=_json_sanitize(player_state_to_json(pre_a)),
                    player_b_state_json=_json_sanitize(player_state_to_json(pre_b)),
                    features_json=_json_sanitize(_features),
                    prediction_a=float(pred_a),
                    predicted_at=datetime.utcnow(),
                )
                session.add(node)
                session.flush()
                actual_node_id_by_slot[slot] = int(node.id)

                # Determine post-win outcomes for this slot.
                outs: list[_Outcome] = []
                if match.status in get_status_group("historical_default") and match.winner_id:
                    # Completed: deterministic.
                    if match.winner_id == match.player_a_id:
                        w_post, _l_post = derive_winner_state(
                            session,
                            winner_pre=pre_a,
                            loser_pre=pre_b,
                            ctx=ctx,
                            tournament=tournament,
                            edition=edition,
                            winner_is_a=True,
                            score_structured=match.score_structured,
                        )
                        outs.append(_Outcome(int(match.player_a_id), int(node.id), w_post))
                    else:
                        w_post, _l_post = derive_winner_state(
                            session,
                            winner_pre=pre_b,
                            loser_pre=pre_a,
                            ctx=ctx,
                            tournament=tournament,
                            edition=edition,
                            winner_is_a=False,
                            score_structured=match.score_structured,
                        )
                        outs.append(_Outcome(int(match.player_b_id), int(node.id), w_post))
                else:
                    # Unresolved: branch both ways using synthetic defaults.
                    w_post_a, _ = derive_winner_state(
                        session,
                        winner_pre=pre_a,
                        loser_pre=pre_b,
                        ctx=ctx,
                        tournament=tournament,
                        edition=edition,
                        winner_is_a=True,
                        score_structured=None,
                    )
                    w_post_b, _ = derive_winner_state(
                        session,
                        winner_pre=pre_b,
                        loser_pre=pre_a,
                        ctx=ctx,
                        tournament=tournament,
                        edition=edition,
                        winner_is_a=False,
                        score_structured=None,
                    )
                    outs.append(_Outcome(int(match.player_a_id), int(node.id), w_post_a))
                    outs.append(_Outcome(int(match.player_b_id), int(node.id), w_post_b))

                winner_outcomes[slot] = outs
                continue

            # No actual match exists: create scenario nodes for this slot.
            if round_code == entry_round:
                # Entry round must be known; readiness should prevent this.
                continue

            prev_round = ROUND_PROGRESSION[ROUND_PROGRESSION.index(round_code) - 1]
            f1, f2 = get_feeder_positions(pos)
            left_slot = (prev_round, f1)
            right_slot = (prev_round, f2)
            left_outs = winner_outcomes.get(left_slot)
            right_outs = winner_outcomes.get(right_slot)

            if not left_outs or not right_outs:
                # Not resolvable => fail hard (all-or-nothing).
                raise ValueError(f"Unresolvable slot {slot}: missing feeder winners")

            ctx = _make_synthetic_context(edition, tournament, round_code)

            outs: list[_Outcome] = []
            # Cross-product matchup nodes keyed by parent-path outcomes.
            for lo in left_outs:
                for ro in right_outs:
                    if lo.player_id == ro.player_id:
                        continue

                    features = build_features_for_states(
                        state_a=lo.state,
                        state_b=ro.state,
                        ctx=ctx,
                        feature_set_name=model.feature_set_name,
                    )
                    pred_a = predict_probability_a(model, features)

                    node = TournamentForecastNode(
                        forecast_run_id=run.id,
                        round=round_code,
                        draw_position=pos,
                        player_a_id=int(lo.player_id),
                        player_b_id=int(ro.player_id),
                        left_parent_node_id=int(lo.parent_node_id),
                        right_parent_node_id=int(ro.parent_node_id),
                        source_match_id=None,
                        node_type="scenario",
                        generation_depth=0,
                        feature_set_name=model.feature_set_name,
                        prediction_model_version=model.model_version,
                        player_a_state_json=_json_sanitize(player_state_to_json(lo.state)),
                        player_b_state_json=_json_sanitize(player_state_to_json(ro.state)),
                        features_json=_json_sanitize(features),
                        prediction_a=float(pred_a),
                        predicted_at=datetime.utcnow(),
                    )
                    session.add(node)
                    session.flush()

                    # Propagate path-specific post-win state for both winner branches.
                    w_post_a, _ = derive_winner_state(
                        session,
                        winner_pre=lo.state,
                        loser_pre=ro.state,
                        ctx=ctx,
                        tournament=tournament,
                        edition=edition,
                        winner_is_a=True,
                        score_structured=None,
                    )
                    w_post_b, _ = derive_winner_state(
                        session,
                        winner_pre=ro.state,
                        loser_pre=lo.state,
                        ctx=ctx,
                        tournament=tournament,
                        edition=edition,
                        winner_is_a=False,
                        score_structured=None,
                    )
                    outs.append(_Outcome(int(lo.player_id), int(node.id), w_post_a))
                    outs.append(_Outcome(int(ro.player_id), int(node.id), w_post_b))

            winner_outcomes[slot] = outs


def _make_synthetic_context(edition: TournamentEdition, tournament: Tournament, round_code: str) -> MatchContext:
    level_code = get_level_code(tournament.level, tournament.tour)
    match_date = edition.start_date
    return MatchContext(
        match_id=0,
        match_date=match_date,
        surface=edition.surface or tournament.surface,
        level_code=level_code,
        tour=tournament.tour,
        gender=tournament.gender,
        round=round_code,
        year=edition.year,
        seed_a=None,
        seed_b=None,
        temporal_order=None,
        tournament_edition_id=edition.id,
        tournament_id=tournament.id,
        match_date_estimated=True,
        tournament_country_ioc=tournament.country_ioc,
        player_a_nationality=None,
        player_b_nationality=None,
    )


def _make_match_context(edition: TournamentEdition, tournament: Tournament, match: Match, round_code: str) -> MatchContext:
    level_code = get_level_code(tournament.level, tournament.tour)
    return MatchContext(
        match_id=match.id,
        match_date=match.match_date or match.scheduled_date or edition.start_date,
        surface=edition.surface or tournament.surface,
        level_code=level_code,
        tour=tournament.tour,
        gender=tournament.gender,
        round=round_code,
        year=edition.year,
        seed_a=match.player_a_seed,
        seed_b=match.player_b_seed,
        temporal_order=match.temporal_order,
        tournament_edition_id=edition.id,
        tournament_id=tournament.id,
        match_date_estimated=bool(match.match_date_estimated),
        tournament_country_ioc=tournament.country_ioc,
        player_a_nationality=getattr(match.player_a, "nationality_ioc", None),
        player_b_nationality=getattr(match.player_b, "nationality_ioc", None),
    )


@dataclass
class ForecastProbabilities:
    reach: dict[int, dict[str, float]]
    title: dict[int, float]


def compute_probabilities(session: Session, *, edition_id: int) -> dict[str, Any]:
    run = get_active_run(session, edition_id)
    if run is None:
        return {"has_forecast": False, "status": "not_built"}

    edition = session.execute(
        select(TournamentEdition).options(joinedload(TournamentEdition.tournament)).where(TournamentEdition.id == edition_id)
    ).scalar_one()
    tournament = edition.tournament

    nodes = (
        session.query(TournamentForecastNode)
        .filter(TournamentForecastNode.forecast_run_id == run.id)
        .all()
    )

    # Load actual matches for completed/winner info.
    match_ids = [n.source_match_id for n in nodes if n.source_match_id]
    matches_by_id: dict[int, Match] = {}
    if match_ids:
        for m in session.query(Match).filter(Match.id.in_(match_ids)).all():
            matches_by_id[m.id] = m

    # Group nodes by slot.
    nodes_by_slot: dict[tuple[str, int], list[TournamentForecastNode]] = defaultdict(list)
    for n in nodes:
        nodes_by_slot[(n.round, int(n.draw_position))].append(n)

    # Winner (reach-next-round) probability mass per slot, keyed by *path*.
    #
    # entrant_mass[(round, pos)][(player_id, parent_node_id)] = probability that player wins this slot
    # via the specific parent node path, where parent_node_id is the node id for the match that was won.
    entrant_mass: dict[tuple[str, int], dict[tuple[int, int], float]] = {}

    # Iterate rounds in order.
    for round_code in MAIN_DRAW_ROUNDS:
        expected = get_expected_matches_in_round(round_code)
        if expected is None:
            continue

        for pos in range(1, expected + 1):
            slot = (round_code, pos)
            slot_nodes = nodes_by_slot.get(slot, [])
            if not slot_nodes:
                continue

            # If there is an actual node, treat matchup as fixed and authoritative.
            actual_node = next(
                (n for n in slot_nodes if n.node_type == "actual" and n.source_match_id),
                None,
            )
            if actual_node is not None:
                m = matches_by_id.get(actual_node.source_match_id)
                if m is not None and m.status in get_status_group("historical_default") and m.winner_id:
                    entrant_mass[slot] = {(int(m.winner_id), int(actual_node.id)): 1.0}
                else:
                    pa = float(actual_node.prediction_a or 0.5)
                    entrant_mass[slot] = {
                        (int(actual_node.player_a_id), int(actual_node.id)): pa,
                        (int(actual_node.player_b_id), int(actual_node.id)): 1.0 - pa,
                    }
                continue

            # Scenario slot: compute based on feeder entrant masses *by parent path*.
            prev_round = ROUND_PROGRESSION[ROUND_PROGRESSION.index(round_code) - 1]
            f1, f2 = get_feeder_positions(pos)

            left_slot = (prev_round, f1)
            right_slot = (prev_round, f2)
            left_mass = entrant_mass.get(left_slot, {})
            right_mass = entrant_mass.get(right_slot, {})
            if not left_mass or not right_mass:
                raise ValueError(f"Missing feeder masses for {slot}")

            acc: dict[tuple[int, int], float] = defaultdict(float)
            for n in slot_nodes:
                pa = float(n.prediction_a or 0.5)

                # Occurrence probability comes from the exact parent-node keyed paths.
                lp = int(n.left_parent_node_id or 0)
                rp = int(n.right_parent_node_id or 0)
                if lp <= 0 or rp <= 0:
                    # Scenario nodes must be path-specific; without parent ids we cannot allocate mass correctly.
                    continue

                p_left = float(left_mass.get((int(n.player_a_id), lp), 0.0))
                p_right = float(right_mass.get((int(n.player_b_id), rp), 0.0))
                p_occurs = p_left * p_right
                if p_occurs <= 0:
                    continue

                acc[(int(n.player_a_id), int(n.id))] += p_occurs * pa
                acc[(int(n.player_b_id), int(n.id))] += p_occurs * (1.0 - pa)

            entrant_mass[slot] = dict(acc)
    # Title is winner of final slot.
    title_mass_by_path = entrant_mass.get(("F", 1), {})
    title_mass: dict[int, float] = defaultdict(float)
    for (pid, _node_id), p in title_mass_by_path.items():
        title_mass[int(pid)] += float(p)

    # Reach-round probs: occupancy in a round is winning previous round.
    reach: dict[int, dict[str, float]] = defaultdict(dict)
    round_reach_codes = ["R64", "R32", "R16", "QF", "SF", "F"]

    for rc in round_reach_codes:
        prev = ROUND_PROGRESSION[ROUND_PROGRESSION.index(rc) - 1]
        expected_prev = get_expected_matches_in_round(prev)
        if expected_prev is None:
            continue
        total_by_player: dict[int, float] = defaultdict(float)
        for pos in range(1, expected_prev + 1):
            mass = entrant_mass.get((prev, pos), {})
            for (pid, _node_id), p in mass.items():
                total_by_player[int(pid)] += float(p)
        # In a properly formed draw, each player can only occupy one slot in a round.
        for pid, p in total_by_player.items():
            reach[pid][f"reach_{rc.lower()}"] = float(p)

    for pid, p in title_mass.items():
        reach[pid]["win_title"] = float(p)

    # Attach player names.
    player_rows = session.query(Player).filter(Player.id.in_(list(reach.keys()))).all()
    name_by_id = {p.id: p.canonical_name for p in player_rows}

    players_payload = []
    for pid, probs in sorted(reach.items(), key=lambda kv: kv[1].get("win_title", 0.0), reverse=True):
        players_payload.append({"player_id": pid, "name": name_by_id.get(pid, str(pid)), **probs})

    return {
        "has_forecast": True,
        "forecast_run": {
            "id": run.id,
            "status": run.status,
            "structure_signature": run.structure_signature,
            "state_signature": run.state_signature,
            "feature_set_name": run.feature_set_name,
            "model_version": run.model_version,
            "generated_at": run.completed_at.isoformat() if run.completed_at else None,
        },
        "players": players_payload,
        "warnings": [],
    }
