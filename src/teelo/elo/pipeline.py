"""
ELO pipeline — orchestrates rating computation across all matches.

Two modes of operation:

1. **run_fast()**: Float-only computation for Optuna optimization.
   No database writes, returns expected probabilities for log-loss.

2. **run_full()**: Full computation with database writes.
   Stores EloRating records, marks career peaks, uses Decimal for final values.

Both modes apply the same modifiers on top of standard ELO:
- Margin-of-victory scaling (bigger wins → bigger rating changes)
- Inactivity decay (pulls inactive players toward 1500)
- New/returning player K-boost (faster convergence for new players)
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from teelo.elo.boost import calculate_k_boost
from teelo.elo.calculator import calculate_fast
from teelo.elo.constants import DEFAULT_ELO, LEVEL_TO_CODE, MARGIN_DEFAULTS, DECAY_DEFAULTS, BOOST_DEFAULTS, get_level_code
from teelo.elo.decay import apply_inactivity_decay
from teelo.elo.margin import calculate_margin_multiplier


def date_from_temporal_order(temporal_order: int) -> date | None:
    """
    Extract the date encoded in a temporal_order integer.

    temporal_order format: YYYYMMDD_EEEEE_RR (as a single integer).
    The leading 8 digits encode the date as YYYYMMDD.

    Returns None if the date is the far-future fallback (9999-12-31).
    """
    date_int = temporal_order // 10_000_000
    year = date_int // 10000
    month = (date_int % 10000) // 100
    day = date_int % 100

    # Skip the far-future fallback used for undated matches
    if year >= 9999:
        return None

    try:
        return date(year, month, day)
    except ValueError:
        return None


@dataclass
class EloParams:
    """
    All optimizable ELO parameters in one object.

    Passed to EloPipeline to control the rating system behavior.
    During Optuna optimization, each trial creates an EloParams
    from the suggested hyperparameters.
    """
    # Base K-factors per tournament level code (F, C, A, M, G)
    K_F: float = 183.0
    K_C: float = 137.0
    K_A: float = 108.0
    K_M: float = 107.0
    K_G: float = 116.0

    # Base S-factors (spread) per tournament level code
    S_F: float = 1241.0
    S_C: float = 1441.0
    S_A: float = 1670.0
    S_M: float = 1809.0
    S_G: float = 1428.0

    # Women's K-factors (WF, WC, WA, WM, WG)
    # Separate from men's because all women's matches are best-of-3
    K_WF: float = 183.0
    K_WC: float = 137.0
    K_WA: float = 108.0
    K_WM: float = 107.0
    K_WG: float = 116.0

    # Women's S-factors
    S_WF: float = 1241.0
    S_WC: float = 1441.0
    S_WA: float = 1670.0
    S_WM: float = 1809.0
    S_WG: float = 1428.0

    # Margin-of-victory parameters
    margin_base: float = MARGIN_DEFAULTS["margin_base"]
    margin_scale: float = MARGIN_DEFAULTS["margin_scale"]

    # Inactivity decay parameters
    decay_rate: float = DECAY_DEFAULTS["decay_rate"]
    decay_start_days: float = DECAY_DEFAULTS["decay_start_days"]

    # New player K-boost parameters
    new_threshold: int = BOOST_DEFAULTS["new_threshold"]
    new_boost: float = BOOST_DEFAULTS["new_boost"]

    # Returning player K-boost parameters
    returning_days: float = BOOST_DEFAULTS["returning_days"]
    returning_boost: float = BOOST_DEFAULTS["returning_boost"]

    def get_k(self, level_code: str) -> float:
        """Get base K-factor for a tournament level code."""
        return getattr(self, f"K_{level_code}", self.K_A)

    def get_s(self, level_code: str) -> float:
        """Get S-factor for a tournament level code."""
        return getattr(self, f"S_{level_code}", self.S_A)


@dataclass
class _PlayerState:
    """Internal tracking of a player's current state during pipeline run."""
    rating: float = float(DEFAULT_ELO)
    match_count: int = 0
    last_match_date: Optional[date] = None
    career_peak: float = float(DEFAULT_ELO)


class EloPipeline:
    """
    Orchestrates ELO computation across all matches in temporal order.

    Usage (optimization):
        params = EloParams(K_F=190, S_F=1200, ...)
        pipeline = EloPipeline(params)
        probs = pipeline.run_fast(matches)
        # Compute log-loss from probs

    Usage (final calculation):
        pipeline = EloPipeline(best_params)
        pipeline.run_full(session)
    """

    def __init__(self, params: EloParams):
        self.params = params

    def run_fast(self, matches: list[dict]) -> list[float]:
        """
        Fast float-only ELO computation for optimization.

        Processes all matches in order, applying decay, boost, margin,
        and standard ELO. No database writes.

        Args:
            matches: List of match dicts from load_matches_for_elo(),
                     already sorted by temporal_order.

        Returns:
            List of expected winner probabilities (one per match),
            used to compute log-loss for optimization.
        """
        players: dict[int, _PlayerState] = {}
        probs: list[float] = []
        params = self.params

        for m in matches:
            pid_a = m["player_a_id"]
            pid_b = m["player_b_id"]
            level_code = m["level_code"]
            match_date = m["match_date"]
            score_structured = m.get("score_structured")
            winner_id = m["winner_id"]

            # Determine winner label
            if winner_id == pid_a:
                winner = "A"
            elif winner_id == pid_b:
                winner = "B"
            else:
                # Skip matches where winner doesn't match either player
                continue

            # Initialize player states if first time seen
            if pid_a not in players:
                players[pid_a] = _PlayerState()
            if pid_b not in players:
                players[pid_b] = _PlayerState()

            state_a = players[pid_a]
            state_b = players[pid_b]

            # --- Step 1: Apply inactivity decay ---
            if state_a.last_match_date is not None and match_date is not None:
                days_a = (match_date - state_a.last_match_date).days
                state_a.rating = apply_inactivity_decay(
                    state_a.rating, days_a,
                    decay_rate=params.decay_rate,
                    decay_start_days=params.decay_start_days,
                )
            else:
                days_a = None

            if state_b.last_match_date is not None and match_date is not None:
                days_b = (match_date - state_b.last_match_date).days
                state_b.rating = apply_inactivity_decay(
                    state_b.rating, days_b,
                    decay_rate=params.decay_rate,
                    decay_start_days=params.decay_start_days,
                )
            else:
                days_b = None

            # --- Step 2: Calculate K-boost per player ---
            boost_a = calculate_k_boost(
                state_a.match_count,
                float(days_a) if days_a is not None else None,
                new_threshold=params.new_threshold,
                new_boost=params.new_boost,
                returning_days=params.returning_days,
                returning_boost=params.returning_boost,
            )
            boost_b = calculate_k_boost(
                state_b.match_count,
                float(days_b) if days_b is not None else None,
                new_threshold=params.new_threshold,
                new_boost=params.new_boost,
                returning_days=params.returning_days,
                returning_boost=params.returning_boost,
            )

            # --- Step 3: Get base K and S ---
            base_k = params.get_k(level_code)
            s = params.get_s(level_code)

            # --- Step 4: Calculate margin multiplier ---
            if score_structured:
                margin_result = calculate_margin_multiplier(
                    score_structured, winner,
                    margin_base=params.margin_base,
                    margin_scale=params.margin_scale,
                )
                margin_mult = float(margin_result.multiplier)
            else:
                margin_mult = 1.0

            # --- Step 5: Effective K per player ---
            k_a = base_k * margin_mult * boost_a
            k_b = base_k * margin_mult * boost_b

            # --- Step 6: Calculate new ratings ---
            new_a, new_b, exp_winner_prob = calculate_fast(
                state_a.rating, state_b.rating, winner, k_a, k_b, s,
            )

            # --- Step 7: Update state ---
            state_a.rating = new_a
            state_b.rating = new_b
            state_a.match_count += 1
            state_b.match_count += 1
            if match_date is not None:
                state_a.last_match_date = match_date
                state_b.last_match_date = match_date

            # Clamp probability for log-loss stability
            probs.append(max(1e-7, min(1.0 - 1e-7, exp_winner_prob)))

        return probs

    def run_full(self, session: Session) -> int:
        """
        Full ELO computation with database writes.

        Same logic as run_fast but writes EloRating records in batches
        and tracks career peaks.

        Args:
            session: SQLAlchemy session for DB operations.

        Returns:
            Number of EloRating records created.
        """
        from teelo.db.models import EloRating, Match

        matches = load_matches_for_elo(session)
        players: dict[int, _PlayerState] = {}
        params = self.params
        batch: list[EloRating] = []
        batch_size = 1000
        total_records = 0

        for m in matches:
            pid_a = m["player_a_id"]
            pid_b = m["player_b_id"]
            level_code = m["level_code"]
            match_date = m["match_date"]
            match_id = m["match_id"]
            score_structured = m.get("score_structured")
            winner_id = m["winner_id"]

            if winner_id == pid_a:
                winner = "A"
            elif winner_id == pid_b:
                winner = "B"
            else:
                continue

            if pid_a not in players:
                players[pid_a] = _PlayerState()
            if pid_b not in players:
                players[pid_b] = _PlayerState()

            state_a = players[pid_a]
            state_b = players[pid_b]

            # Apply inactivity decay
            if state_a.last_match_date is not None and match_date is not None:
                days_a = (match_date - state_a.last_match_date).days
                state_a.rating = apply_inactivity_decay(
                    state_a.rating, days_a,
                    decay_rate=params.decay_rate,
                    decay_start_days=params.decay_start_days,
                )
            else:
                days_a = None

            if state_b.last_match_date is not None and match_date is not None:
                days_b = (match_date - state_b.last_match_date).days
                state_b.rating = apply_inactivity_decay(
                    state_b.rating, days_b,
                    decay_rate=params.decay_rate,
                    decay_start_days=params.decay_start_days,
                )
            else:
                days_b = None

            # K-boost per player
            boost_a = calculate_k_boost(
                state_a.match_count,
                float(days_a) if days_a is not None else None,
                new_threshold=params.new_threshold,
                new_boost=params.new_boost,
                returning_days=params.returning_days,
                returning_boost=params.returning_boost,
            )
            boost_b = calculate_k_boost(
                state_b.match_count,
                float(days_b) if days_b is not None else None,
                new_threshold=params.new_threshold,
                new_boost=params.new_boost,
                returning_days=params.returning_days,
                returning_boost=params.returning_boost,
            )

            base_k = params.get_k(level_code)
            s = params.get_s(level_code)

            if score_structured:
                margin_result = calculate_margin_multiplier(
                    score_structured, winner,
                    margin_base=params.margin_base,
                    margin_scale=params.margin_scale,
                )
                margin_mult = float(margin_result.multiplier)
            else:
                margin_mult = 1.0

            k_a = base_k * margin_mult * boost_a
            k_b = base_k * margin_mult * boost_b

            # Store before-ratings for DB records
            rating_before_a = state_a.rating
            rating_before_b = state_b.rating

            new_a, new_b, _ = calculate_fast(
                state_a.rating, state_b.rating, winner, k_a, k_b, s,
            )

            state_a.rating = new_a
            state_b.rating = new_b
            state_a.match_count += 1
            state_b.match_count += 1
            if match_date is not None:
                state_a.last_match_date = match_date
                state_b.last_match_date = match_date

            # Track career peaks
            is_peak_a = new_a > state_a.career_peak
            if is_peak_a:
                state_a.career_peak = new_a
            is_peak_b = new_b > state_b.career_peak
            if is_peak_b:
                state_b.career_peak = new_b

            # Create EloRating records for both players
            rating_date = match_date or date(9999, 12, 31)
            batch.append(EloRating(
                player_id=pid_a,
                match_id=match_id,
                rating_before=Decimal(str(round(rating_before_a, 2))),
                rating_after=Decimal(str(round(new_a, 2))),
                rating_date=rating_date,
                is_career_peak=is_peak_a,
            ))
            batch.append(EloRating(
                player_id=pid_b,
                match_id=match_id,
                rating_before=Decimal(str(round(rating_before_b, 2))),
                rating_after=Decimal(str(round(new_b, 2))),
                rating_date=rating_date,
                is_career_peak=is_peak_b,
            ))
            total_records += 2

            # Flush in batches
            if len(batch) >= batch_size:
                session.bulk_save_objects(batch)
                session.flush()
                batch.clear()

        # Flush remaining
        if batch:
            session.bulk_save_objects(batch)
            session.flush()

        return total_records


def load_matches_for_elo(session: Session) -> list[dict]:
    """
    Load all completed matches for ELO computation.

    Single query joining Match → TournamentEdition → Tournament to get
    the tournament level. Returns lightweight dicts sorted by temporal_order
    for fast iteration.

    Args:
        session: SQLAlchemy session

    Returns:
        List of dicts with keys: match_id, player_a_id, player_b_id,
        winner_id, level_code, match_date, score_structured, temporal_order
    """
    from teelo.db.models import Match, TournamentEdition, Tournament

    # Query completed matches with tournament level info
    stmt = (
        select(
            Match.id,
            Match.player_a_id,
            Match.player_b_id,
            Match.winner_id,
            Match.match_date,
            Match.score_structured,
            Match.temporal_order,
            Tournament.level,
            Tournament.tour,
        )
        .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
        .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
        .where(Match.status.in_(["completed", "retired"]))
        .where(Match.winner_id.isnot(None))
        .where(Match.temporal_order.isnot(None))
        .order_by(Match.temporal_order)
    )

    rows = session.execute(stmt).all()

    matches = []
    for row in rows:
        # Map tournament level + tour to level code
        # Women's tours get "W" prefix (e.g., "WG" instead of "G")
        level_code = get_level_code(row.level, row.tour)

        # Use match_date if available, otherwise extract from temporal_order
        match_date = row.match_date
        if match_date is None and row.temporal_order is not None:
            match_date = date_from_temporal_order(row.temporal_order)

        matches.append({
            "match_id": row.id,
            "player_a_id": row.player_a_id,
            "player_b_id": row.player_b_id,
            "winner_id": row.winner_id,
            "level_code": level_code,
            "match_date": match_date,
            "score_structured": row.score_structured,
            "temporal_order": row.temporal_order,
        })

    return matches
