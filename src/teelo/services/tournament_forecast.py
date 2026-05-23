from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import and_, select
from sqlalchemy.orm import Session, joinedload

from teelo.db.models import (
    Match,
    Player,
    Tournament,
    TournamentEdition,
    TournamentForecastNode,
    TournamentForecastRun,
)
from teelo.draw import (
    ROUND_PROGRESSION,
    get_expected_matches_in_round,
    get_feeder_positions,
    get_next_round,
)
from teelo.match_statuses import get_status_group

logger = structlog.get_logger(__name__)

if TYPE_CHECKING:
    from teelo.services.forecast_prediction import ForecastModel


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
FORECAST_EXCLUDED_MATCH_STATUSES: set[str] = {"cancelled"}


def is_forecast_eligible_tournament(tournament: Tournament) -> bool:
    """Eligibility filter (locked v1): ATP/WTA main tour only."""

    tour = (tournament.tour or "").strip().upper()
    if tour not in {"ATP", "WTA"}:
        return False

    # Explicit exclusions even if fields are ambiguous.
    if tournament.level in {"Challenger", "WTA 125", "ITF", "ITF Men", "ITF Women"}:
        return False

    if tour == "ATP" and tournament.level in {
        "Grand Slam",
        "Masters 1000",
        "ATP 500",
        "ATP 250",
        "ATP Finals",
    }:
        return True
    if tour == "WTA" and tournament.level in {
        "Grand Slam",
        "WTA 1000",
        "WTA 500",
        "WTA 250",
        "WTA Finals",
    }:
        return True

    # Default deny.
    return False


def _forecast_match_filter() -> Any:
    """SQL predicate for matches that should participate in a live forecast bracket."""

    return and_(
        Match.draw_position.isnot(None),
        Match.round.in_(MAIN_DRAW_ROUNDS),
        Match.player_a_id.isnot(None),
        Match.player_b_id.isnot(None),
        Match.status.notin_(FORECAST_EXCLUDED_MATCH_STATUSES),
    )


def _load_forecast_matches(session: Session, edition_id: int) -> list[Match]:
    """Load the active draw rows used for forecast structure.

    Scrapers can leave cancelled placeholder rows behind when qualifiers or late draw
    replacements are resolved. Those rows share the same (round, draw_position) as the real
    matchup and must not feed forecast generation or probability aggregation.
    """

    return (
        session.query(Match)
        .filter(
            Match.tournament_edition_id == edition_id,
            _forecast_match_filter(),
        )
        .options(joinedload(Match.player_a), joinedload(Match.player_b))
        .order_by(Match.round.asc(), Match.draw_position.asc(), Match.id.desc())
        .all()
    )


def _load_forecast_matches_by_slot(
    session: Session, edition_id: int
) -> dict[tuple[str, int], Match]:
    matches_by_slot: dict[tuple[str, int], Match] = {}
    for match in _load_forecast_matches(session, edition_id):
        key = (match.round, int(match.draw_position))
        matches_by_slot.setdefault(key, match)
    return matches_by_slot


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
            _forecast_match_filter(),
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
    """Return True once the entry round has been scraped, making all second-round slots resolvable.

    Rule: forecast triggers once the entry round has at least one match with both players known.
    Any entry-round draw positions without a Match record are assumed to be byes — the bracket
    structure guarantees those slots are resolved (the bye player advances automatically).

    NOTE: We assume a partial entry-round scrape is not possible — if any entry-round matches
    exist, we treat the full entry round as complete. A future improvement would be to track
    the expected real-match count from the scraper (draw_size minus bracket bye count) and
    compare, but draw_size is not reliably scraped today.
    """

    tournament = session.execute(
        select(Tournament).where(Tournament.id == edition.tournament_id)
    ).scalar_one()

    if not is_forecast_eligible_tournament(tournament):
        return False

    entry_round = determine_entry_round(session, edition)
    if entry_round is None:
        return False
    if get_next_round(entry_round) is None:
        return False

    expected_entry_matches = get_expected_matches_in_round(entry_round)
    if tournament.level == "Grand Slam" and expected_entry_matches is not None:
        distinct_entry_positions = (
            session.query(Match.draw_position)
            .filter(
                Match.tournament_edition_id == edition.id,
                Match.round == entry_round,
                _forecast_match_filter(),
            )
            .distinct()
            .count()
        )
        if distinct_entry_positions < expected_entry_matches:
            return False

    entry_match_count = (
        session.query(Match)
        .filter(
            Match.tournament_edition_id == edition.id,
            Match.round == entry_round,
            _forecast_match_filter(),
        )
        .count()
    )
    return entry_match_count > 0


def _hash_dict(payload: Any) -> str:
    raw = repr(payload).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:64]


def compute_structure_signature(session: Session, edition: TournamentEdition, model) -> str:
    entry_round = determine_entry_round(session, edition)
    rounds = (
        ROUND_PROGRESSION[ROUND_PROGRESSION.index(entry_round) :]
        if entry_round in ROUND_PROGRESSION
        else list(MAIN_DRAW_ROUNDS)
    )

    matches = [
        m for m in _load_forecast_matches_by_slot(session, edition.id).values() if m.round in rounds
    ]
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
    rounds = (
        ROUND_PROGRESSION[ROUND_PROGRESSION.index(entry_round) :]
        if entry_round in ROUND_PROGRESSION
        else list(MAIN_DRAW_ROUNDS)
    )

    matches = [
        m for m in _load_forecast_matches_by_slot(session, edition.id).values() if m.round in rounds
    ]
    payload = {
        "edition_id": edition.id,
        "states": [
            {
                "id": m.id,
                "round": m.round,
                "pos": m.draw_position,
                "status": m.status,
                "winner_id": m.winner_id,
                "prediction_a": float(m.prediction_a) if m.prediction_a is not None else None,
                "prediction_model_version": m.prediction_model_version,
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
        .order_by(
            TournamentForecastRun.completed_at.desc().nullslast(), TournamentForecastRun.id.desc()
        )
        .first()
    )


def build_forecast_run(
    session: Session,
    *,
    edition_id: int,
    force: bool = False,
    build_reason: str = "initial",
) -> TournamentForecastRun:
    # ML stack imports — only needed for building, not reading forecasts.
    from teelo.ml.versioning import latest_feature_set
    from teelo.services.forecast_prediction import ForecastModel, load_forecast_model  # noqa: F401

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
    if (
        existing
        and (not force)
        and existing.structure_signature == structure_sig
        and existing.state_signature == state_sig
    ):
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

    from teelo.services.forecast_prediction import (
        build_features_for_states,
        predict_probability_a,
        reuse_or_predict_real_match,
    )
    from teelo.services.forecast_state_builder import (
        build_base_states_for_players,
        derive_winner_state,
        player_state_to_json,
    )

    @dataclass(frozen=True)
    class _Outcome:
        player_id: int
        parent_node_id: int
        state: Any  # PlayerState

    # Load all existing main-draw matches for this edition.
    match_by_slot = _load_forecast_matches_by_slot(session, edition.id)
    matches = list(match_by_slot.values())

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

                    def _maybe_unique_parent_id(
                        outs: list[_Outcome], player_id: int | None
                    ) -> int | None:
                        if player_id is None:
                            return None
                        ids = [
                            int(o.parent_node_id)
                            for o in outs
                            if o.player_id == int(player_id) and o.parent_node_id
                        ]
                        uniq = set(ids)
                        return ids[0] if len(uniq) == 1 else None

                    # Real-world draws can have byes / missing upstream matches in DB.
                    # If the prior slot was an actual node, use that. Otherwise, only attach a parent id
                    # when the winner-path is unambiguous; if ambiguous, leave it None (actual nodes don't
                    # require path-specific parent identity to be useful downstream).
                    left_parent_id = actual_node_id_by_slot.get(
                        left_slot
                    ) or _maybe_unique_parent_id(left_outs, match.player_a_id)
                    right_parent_id = actual_node_id_by_slot.get(
                        right_slot
                    ) or _maybe_unique_parent_id(right_outs, match.player_b_id)

                    pre_a = next(
                        (o.state for o in left_outs if o.player_id == match.player_a_id), None
                    )
                    pre_b = next(
                        (o.state for o in right_outs if o.player_id == match.player_b_id), None
                    )
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

            # Deduplicate outcomes by player_id: collapse multiple paths to the same
            # player into one representative outcome. Without this, the cross-product
            # grows exponentially (e.g. 128×128 = 16,384 nodes for the F when R16 is
            # fully unresolved). Path-specific ELO drift between scenarios is small
            # (~20-30 pts on a 1500+ scale), so the accuracy impact is minimal.
            deduped: dict[int, _Outcome] = {}
            for out in outs:
                deduped[out.player_id] = out  # last path wins; arbitrary but consistent
            winner_outcomes[slot] = list(deduped.values())


def _make_synthetic_context(edition: TournamentEdition, tournament: Tournament, round_code: str):
    from teelo.elo.constants import get_level_code
    from teelo.features.state import MatchContext

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


def _make_match_context(
    edition: TournamentEdition, tournament: Tournament, match: Match, round_code: str
):
    from teelo.elo.constants import get_level_code
    from teelo.features.state import MatchContext

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


def sync_forecast_nodes(session: Session, *, edition_id: int) -> int:
    """Promote scenario nodes to actual nodes when a real match now exists for them.

    Called hourly after results and predictions are processed. When a QF result
    comes in, the SF match becomes a known real match in the draw. This function
    links the corresponding scenario node to that match (source_match_id +
    node_type="actual") so that compute_probabilities reads match.prediction_a
    directly — keeping P(win current round) == P(reach next round) throughout
    the day without a full forecast rebuild.

    Returns the number of nodes updated.
    """
    run = get_active_run(session, edition_id)
    if run is None:
        return 0

    # Only unlinked scenario nodes need processing.
    nodes = (
        session.query(TournamentForecastNode)
        .filter(
            TournamentForecastNode.forecast_run_id == run.id,
            TournamentForecastNode.node_type == "scenario",
            TournamentForecastNode.source_match_id.is_(None),
        )
        .all()
    )
    if not nodes:
        return 0

    # Load all active draw matches for this edition.
    matches = _load_forecast_matches(session, edition_id)

    # Index by (round, draw_position, frozenset of player ids) for O(1) lookup.
    match_index: dict[tuple[str, int, frozenset], Match] = {}
    for m in matches:
        key = (m.round, int(m.draw_position), frozenset([int(m.player_a_id), int(m.player_b_id)]))
        match_index[key] = m

    updated = 0
    for node in nodes:
        key = (
            node.round,
            int(node.draw_position),
            frozenset([int(node.player_a_id), int(node.player_b_id)]),
        )
        match = match_index.get(key)
        if match is None:
            continue

        node.source_match_id = match.id
        node.node_type = "actual"
        session.add(node)
        updated += 1

    return updated


def get_top_player(session: Session, *, edition_id: int) -> dict[str, Any] | None:
    """Return the favourite player and their win probability, or None if no forecast."""
    try:
        result = compute_probabilities(session, edition_id=edition_id)
    except ValueError:
        logger.warning("forecast.top_player_unavailable", edition_id=edition_id)
        return None
    if not result.get("has_forecast"):
        return None
    players = result.get("players") or []
    if not players:
        return None
    top = players[0]
    win_pct = top.get("win_title")
    if win_pct is None:
        return None
    return {"name": top["name"], "win_pct": round(win_pct * 100, 1)}


def compute_probabilities(session: Session, *, edition_id: int) -> dict[str, Any]:
    run = get_active_run(session, edition_id)
    if run is None:
        return {"has_forecast": False, "status": "not_built"}

    session.execute(
        select(TournamentEdition)
        .options(joinedload(TournamentEdition.tournament))
        .where(TournamentEdition.id == edition_id)
    ).scalar_one()

    # Important: only load the lightweight columns needed for probability aggregation.
    # Loading full ORM nodes pulls large JSON state blobs/features for every scenario node,
    # which can make forecast reads unacceptably slow for larger draws.
    node_rows = session.execute(
        select(
            TournamentForecastNode.id,
            TournamentForecastNode.round,
            TournamentForecastNode.draw_position,
            TournamentForecastNode.player_a_id,
            TournamentForecastNode.player_b_id,
            TournamentForecastNode.source_match_id,
            TournamentForecastNode.node_type,
            TournamentForecastNode.prediction_a,
        ).where(TournamentForecastNode.forecast_run_id == run.id)
    ).all()

    # Load actual matches for completed/winner info (again, only the fields we use).
    match_ids = [int(row.source_match_id) for row in node_rows if row.source_match_id]
    matches_by_id: dict[int, dict[str, Any]] = {}
    if match_ids:
        for m in session.execute(
            select(Match.id, Match.status, Match.winner_id, Match.prediction_a).where(
                Match.id.in_(match_ids)
            )
        ):
            matches_by_id[int(m.id)] = {
                "status": m.status,
                "winner_id": m.winner_id,
                "prediction_a": m.prediction_a,
            }

    # Group nodes by slot.
    nodes_by_slot: dict[tuple[str, int], list[Any]] = defaultdict(list)
    for row in node_rows:
        nodes_by_slot[(row.round, int(row.draw_position))].append(row)

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
                (
                    n
                    for n in slot_nodes
                    if n.node_type == "actual"
                    and n.source_match_id
                    and (
                        (m := matches_by_id.get(int(n.source_match_id))) is not None
                        and m["status"] not in FORECAST_EXCLUDED_MATCH_STATUSES
                    )
                ),
                None,
            )
            if actual_node is not None:
                m = matches_by_id.get(int(actual_node.source_match_id))
                if (
                    m is not None
                    and m["status"] in get_status_group("historical_default")
                    and m["winner_id"]
                ):
                    entrant_mass[slot] = {(int(m["winner_id"]), int(actual_node.id)): 1.0}
                else:
                    pa = float(
                        m["prediction_a"]
                        if m is not None and m["prediction_a"] is not None
                        else actual_node.prediction_a or 0.5
                    )
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

            # Aggregate feeder masses by player_id. In a bracket tournament each player
            # occupies exactly one slot per round, so summing across parent-node paths
            # gives the correct total probability without double-counting.
            left_by_player: dict[int, float] = defaultdict(float)
            for (pid, _nid), p in left_mass.items():
                left_by_player[pid] += p
            right_by_player: dict[int, float] = defaultdict(float)
            for (pid, _nid), p in right_mass.items():
                right_by_player[pid] += p

            acc: dict[tuple[int, int], float] = defaultdict(float)
            for n in slot_nodes:
                pa = float(n.prediction_a or 0.5)
                p_left = left_by_player.get(int(n.player_a_id), 0.0)
                p_right = right_by_player.get(int(n.player_b_id), 0.0)
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
    for pid, probs in sorted(
        reach.items(), key=lambda kv: kv[1].get("win_title", 0.0), reverse=True
    ):
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
