"""Incremental inline ELO updates during ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from teelo.db.models import Match, PlayerEloState
from teelo.elo.boost import calculate_k_boost
from teelo.elo.calculator import calculate_fast
from teelo.elo.decay import apply_inactivity_decay
from teelo.elo.margin import calculate_margin_multiplier
from teelo.elo.params_store import get_active_elo_params
from teelo.elo.pipeline import EloParams, date_from_temporal_order

TERMINAL_STATUSES = {"completed", "retired", "walkover", "default"}


@dataclass
class LiveEloContext:
    params: EloParams
    params_version: str


class LiveEloUpdater:
    """Handles pre-match snapshots and post-match ELO updates."""

    def __init__(self, params: EloParams, params_version: str):
        self.params = params
        self.params_version = params_version
        self._state_cache: dict[int, PlayerEloState] = {}
        self._locked_state_ids: set[int] = set()

    @classmethod
    def from_session(cls, session: Session) -> "LiveEloUpdater":
        params, version = get_active_elo_params(session)
        return cls(params=params, params_version=version)

    def ensure_pre_match_snapshot(self, session: Session, match: Match, force: bool = False) -> bool:
        """Store latest player ELO values on a pending match."""
        if not force and match.elo_pre_player_a is not None and match.elo_pre_player_b is not None:
            return False

        state_a = self._get_or_create_state(session, match.player_a_id)
        state_b = self._get_or_create_state(session, match.player_b_id)

        match.elo_pre_player_a = state_a.rating
        match.elo_pre_player_b = state_b.rating
        if not match.elo_params_version:
            match.elo_params_version = self.params_version
        return True

    def apply_completed_match(
        self,
        session: Session,
        match: Match,
        level_code: str,
        *,
        for_update: bool = True,
    ) -> bool:
        """Apply ELO update for a completed match when in chronological order."""
        if match.status not in TERMINAL_STATUSES or not match.winner_id or not match.temporal_order:
            return False

        state_a = self._get_or_create_state(session, match.player_a_id, for_update=for_update)
        state_b = self._get_or_create_state(session, match.player_b_id, for_update=for_update)

        if self._is_out_of_order(match, state_a, state_b):
            match.elo_needs_recompute = True
            return False

        match_date = match.match_date or date_from_temporal_order(match.temporal_order)
        before_a = float(match.elo_pre_player_a or state_a.rating)
        before_b = float(match.elo_pre_player_b or state_b.rating)

        rating_a = self._apply_decay(before_a, state_a, match_date)
        rating_b = self._apply_decay(before_b, state_b, match_date)

        days_a = self._days_since(state_a.last_match_date, match_date)
        days_b = self._days_since(state_b.last_match_date, match_date)

        boost_a = calculate_k_boost(
            state_a.match_count,
            float(days_a) if days_a is not None else None,
            new_threshold=self.params.new_threshold,
            new_boost=self.params.new_boost,
            returning_days=self.params.returning_days,
            returning_boost=self.params.returning_boost,
        )
        boost_b = calculate_k_boost(
            state_b.match_count,
            float(days_b) if days_b is not None else None,
            new_threshold=self.params.new_threshold,
            new_boost=self.params.new_boost,
            returning_days=self.params.returning_days,
            returning_boost=self.params.returning_boost,
        )

        base_k = self.params.get_k(level_code)
        s = self.params.get_s(level_code)

        winner = "A" if match.winner_id == match.player_a_id else "B"
        if match.score_structured:
            margin_result = calculate_margin_multiplier(
                match.score_structured,
                winner,
                margin_base=self.params.margin_base,
                margin_scale=self.params.margin_scale,
            )
            margin_mult = float(margin_result.multiplier)
        else:
            margin_mult = 1.0

        k_a = base_k * margin_mult * boost_a
        k_b = base_k * margin_mult * boost_b

        new_a, new_b, _ = calculate_fast(rating_a, rating_b, winner, k_a, k_b, s)
        new_a = round(new_a, 2)
        new_b = round(new_b, 2)

        state_a.rating = Decimal(str(new_a))
        state_b.rating = Decimal(str(new_b))
        state_a.match_count += 1
        state_b.match_count += 1
        state_a.last_temporal_order = match.temporal_order
        state_b.last_temporal_order = match.temporal_order
        if match_date is not None:
            state_a.last_match_date = match_date
            state_b.last_match_date = match_date
        state_a.career_peak = Decimal(str(max(float(state_a.career_peak), new_a)))
        state_b.career_peak = Decimal(str(max(float(state_b.career_peak), new_b)))

        match.elo_pre_player_a = Decimal(str(round(rating_a, 2)))
        match.elo_pre_player_b = Decimal(str(round(rating_b, 2)))
        match.elo_post_player_a = Decimal(str(new_a))
        match.elo_post_player_b = Decimal(str(new_b))
        match.elo_params_version = self.params_version
        match.elo_processed_at = datetime.utcnow()
        match.elo_needs_recompute = False
        return True

    def _is_out_of_order(self, match: Match, state_a: PlayerEloState, state_b: PlayerEloState) -> bool:
        if state_a.last_temporal_order and match.temporal_order < state_a.last_temporal_order:
            return True
        if state_b.last_temporal_order and match.temporal_order < state_b.last_temporal_order:
            return True
        return False

    def _apply_decay(self, rating: float, state: PlayerEloState, match_date: date | None) -> float:
        if state.last_match_date is None or match_date is None:
            return rating
        days = (match_date - state.last_match_date).days
        return apply_inactivity_decay(
            rating,
            days,
            decay_rate=self.params.decay_rate,
            decay_start_days=self.params.decay_start_days,
        )

    @staticmethod
    def _days_since(last_date: date | None, current_date: date | None) -> int | None:
        if last_date is None or current_date is None:
            return None
        return (current_date - last_date).days

    def prime_states(
        self,
        session: Session,
        player_ids: Iterable[int],
        *,
        for_update: bool = False,
    ) -> None:
        """Preload all player states for a batch and lazily create missing ones."""
        unique_ids = {int(player_id) for player_id in player_ids}
        if not unique_ids:
            return

        missing_ids = [player_id for player_id in unique_ids if player_id not in self._state_cache]
        if not missing_ids:
            return

        query = session.query(PlayerEloState).filter(PlayerEloState.player_id.in_(missing_ids))
        if for_update:
            query = query.with_for_update()
        for state in query.all():
            self._state_cache[state.player_id] = state
            if for_update:
                self._locked_state_ids.add(state.player_id)

        unresolved = [player_id for player_id in missing_ids if player_id not in self._state_cache]
        if unresolved:
            self._bulk_create_states(session, unresolved)
            query = session.query(PlayerEloState).filter(PlayerEloState.player_id.in_(unresolved))
            if for_update:
                query = query.with_for_update()
            for state in query.all():
                self._state_cache[state.player_id] = state
                if for_update:
                    self._locked_state_ids.add(state.player_id)

    def _get_or_create_state(
        self,
        session: Session,
        player_id: int,
        *,
        for_update: bool = False,
    ) -> PlayerEloState:
        state = self._state_cache.get(player_id)
        if state is None:
            query = session.query(PlayerEloState).filter(PlayerEloState.player_id == player_id)
            if for_update:
                query = query.with_for_update()
            state = query.first()
            if state is None:
                state = self._create_state(session, player_id)
            self._state_cache[player_id] = state
            if for_update:
                self._locked_state_ids.add(player_id)
            return state

        if for_update and player_id not in self._locked_state_ids:
            state = (
                session.query(PlayerEloState)
                .filter(PlayerEloState.player_id == player_id)
                .with_for_update()
                .first()
            ) or state
            self._state_cache[player_id] = state
            self._locked_state_ids.add(player_id)

        return state

    @staticmethod
    def _create_state(session: Session, player_id: int) -> PlayerEloState:
        # Concurrency-safe lazy init: insert default state unless another worker
        # already inserted the same player_id.
        stmt = (
            insert(PlayerEloState)
            .values(
                player_id=player_id,
                rating=Decimal("1500.00"),
                career_peak=Decimal("1500.00"),
            )
            .on_conflict_do_nothing(index_elements=[PlayerEloState.player_id])
            .returning(PlayerEloState.id)
        )
        inserted_id = session.execute(stmt).scalar_one_or_none()
        if inserted_id is not None:
            state = session.get(PlayerEloState, inserted_id)
            if state is not None:
                return state

        existing = (
            session.query(PlayerEloState)
            .filter(PlayerEloState.player_id == player_id)
            .first()
        )
        if existing is not None:
            return existing

        raise RuntimeError(f"Could not create/load PlayerEloState for player_id={player_id}")

    @staticmethod
    def _bulk_create_states(session: Session, player_ids: list[int]) -> None:
        if not player_ids:
            return
        rows = [
            {
                "player_id": int(player_id),
                "rating": Decimal("1500.00"),
                "career_peak": Decimal("1500.00"),
            }
            for player_id in player_ids
        ]
        stmt = (
            insert(PlayerEloState)
            .values(rows)
            .on_conflict_do_nothing(index_elements=[PlayerEloState.player_id])
        )
        session.execute(stmt)
