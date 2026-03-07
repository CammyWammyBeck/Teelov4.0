"""Activity feature group."""

from __future__ import annotations

from datetime import timedelta

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, PlayerState


class ActivityFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "activity"

    def feature_names(self) -> list[str]:
        return [
            "days_since_last_a",
            "days_since_last_b",
            "days_since_debut_a",
            "days_since_debut_b",
            "matches_4w_a",
            "matches_4w_b",
            "matches_8w_a",
            "matches_8w_b",
            "matches_16w_a",
            "matches_16w_b",
            "games_last_match_a",
            "games_last_match_b",
            "games_tournament_a",
            "games_tournament_b",
            "seed_a",
            "seed_b",
            "seed_diff",
            "both_seeded",
        ]

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        def _days_since(base_date, ref_date) -> float | None:
            if ref_date is None or base_date is None:
                return None
            return float((ref_date - base_date).days)

        def _matches_in_days(state: PlayerState, window_days: int) -> float | None:
            if ctx.match_date is None:
                return None
            cutoff = ctx.match_date - timedelta(days=window_days)
            count = sum(
                1
                for record in state.matches
                if record.match_date is not None and record.match_date >= cutoff
            )
            return float(count)

        def _games_last_match(state: PlayerState) -> float | None:
            if not state.matches:
                return None
            record = state.matches[-1]
            return float(record.games_won + record.games_lost)

        def _games_tournament(state: PlayerState) -> float:
            if ctx.tournament_edition_id is None:
                return 0.0
            return float(
                sum(
                    record.games_won + record.games_lost
                    for record in state.matches
                    if record.tournament_edition_id == ctx.tournament_edition_id
                )
            )

        seed_a = float(ctx.seed_a) if ctx.seed_a is not None else None
        seed_b = float(ctx.seed_b) if ctx.seed_b else None
        seed_diff = (seed_a - seed_b) if (seed_a is not None and seed_b is not None) else None

        return {
            "days_since_last_a": _days_since(state_a.last_match_date, ctx.match_date),
            "days_since_last_b": _days_since(state_b.last_match_date, ctx.match_date),
            "days_since_debut_a": _days_since(state_a.first_match_date, ctx.match_date),
            "days_since_debut_b": _days_since(state_b.first_match_date, ctx.match_date),
            "matches_4w_a": _matches_in_days(state_a, 28),
            "matches_4w_b": _matches_in_days(state_b, 28),
            "matches_8w_a": _matches_in_days(state_a, 56),
            "matches_8w_b": _matches_in_days(state_b, 56),
            "matches_16w_a": _matches_in_days(state_a, 112),
            "matches_16w_b": _matches_in_days(state_b, 112),
            "games_last_match_a": _games_last_match(state_a),
            "games_last_match_b": _games_last_match(state_b),
            "games_tournament_a": _games_tournament(state_a),
            "games_tournament_b": _games_tournament(state_b),
            "seed_a": seed_a,
            "seed_b": seed_b,
            "seed_diff": seed_diff,
            "both_seeded": 1.0 if (seed_a is not None and seed_b is not None) else 0.0,
        }
