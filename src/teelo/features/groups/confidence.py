"""Confidence and missingness companion features."""

from __future__ import annotations

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, PlayerState

_WINDOW = 8


class ConfidenceFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "confidence"

    def feature_names(self) -> list[str]:
        return [
            "surface_elo_observed_a",
            "surface_elo_observed_b",
            "surface_elo_default_a",
            "surface_elo_default_b",
            "surface_elo_match_count_a",
            "surface_elo_match_count_b",
            "opponent_quality_sample_count_8_a",
            "opponent_quality_sample_count_8_b",
            "tournament_history_sample_count_a",
            "tournament_history_sample_count_b",
            "h2h_sample_count",
            "h2h_surface_sample_count",
            "match_date_estimated_flag",
        ]

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        h2h_records = state_a.h2h.get(state_b.player_id, [])
        h2h_surface_records = [
            record for record in h2h_records if ctx.surface is not None and record.surface == ctx.surface
        ]
        return {
            "surface_elo_observed_a": 1.0 if state_a.has_observed_surface_elo(ctx.surface) else 0.0,
            "surface_elo_observed_b": 1.0 if state_b.has_observed_surface_elo(ctx.surface) else 0.0,
            "surface_elo_default_a": 0.0 if state_a.has_observed_surface_elo(ctx.surface) else 1.0,
            "surface_elo_default_b": 0.0 if state_b.has_observed_surface_elo(ctx.surface) else 1.0,
            "surface_elo_match_count_a": self._surface_match_count(state_a, ctx.surface),
            "surface_elo_match_count_b": self._surface_match_count(state_b, ctx.surface),
            "opponent_quality_sample_count_8_a": self._opponent_quality_sample_count(state_a),
            "opponent_quality_sample_count_8_b": self._opponent_quality_sample_count(state_b),
            "tournament_history_sample_count_a": self._tournament_history_sample_count(state_a, ctx.tournament_id),
            "tournament_history_sample_count_b": self._tournament_history_sample_count(state_b, ctx.tournament_id),
            "h2h_sample_count": float(len(h2h_records)),
            "h2h_surface_sample_count": float(len(h2h_surface_records)),
            "match_date_estimated_flag": 1.0 if ctx.match_date_estimated else 0.0,
        }

    def _surface_match_count(self, state: PlayerState, surface: str | None) -> float:
        if surface is None:
            return 0.0
        wins = state.surface_wins.get(surface, 0)
        losses = state.surface_losses.get(surface, 0)
        return float(wins + losses)

    def _opponent_quality_sample_count(self, state: PlayerState) -> float:
        recent = list(state.matches)[-_WINDOW:]
        return float(sum(1 for record in recent if record.expected_win_prob is not None))

    def _tournament_history_sample_count(self, state: PlayerState, tournament_id: int | None) -> float:
        if tournament_id is None:
            return 0.0
        return float(state.tournament_matches.get(tournament_id, 0))
