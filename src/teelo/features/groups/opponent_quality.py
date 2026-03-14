"""Opponent-quality-adjusted form features."""

from __future__ import annotations

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, MatchRecord, PlayerState

_WINDOW = 8


def _recent_records(state: PlayerState, limit: int = _WINDOW) -> list[MatchRecord]:
    return list(state.matches)[-limit:]


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


class OpponentQualityFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "opponent_quality"

    def feature_names(self) -> list[str]:
        return [
            "opp_elo_avg_8_a",
            "opp_elo_avg_8_b",
            "opp_surface_elo_avg_8_a",
            "opp_surface_elo_avg_8_b",
            "wins_vs_higher_elo_8_a",
            "wins_vs_higher_elo_8_b",
            "losses_vs_lower_elo_8_a",
            "losses_vs_lower_elo_8_b",
            "elo_overperf_8_a",
            "elo_overperf_8_b",
        ]

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        _ = ctx
        return {
            "opp_elo_avg_8_a": self._opponent_elo_average(state_a),
            "opp_elo_avg_8_b": self._opponent_elo_average(state_b),
            "opp_surface_elo_avg_8_a": self._opponent_surface_elo_average(state_a),
            "opp_surface_elo_avg_8_b": self._opponent_surface_elo_average(state_b),
            "wins_vs_higher_elo_8_a": self._wins_vs_higher(state_a),
            "wins_vs_higher_elo_8_b": self._wins_vs_higher(state_b),
            "losses_vs_lower_elo_8_a": self._losses_vs_lower(state_a),
            "losses_vs_lower_elo_8_b": self._losses_vs_lower(state_b),
            "elo_overperf_8_a": self._elo_overperformance(state_a),
            "elo_overperf_8_b": self._elo_overperformance(state_b),
        }

    def _opponent_elo_average(self, state: PlayerState) -> float | None:
        records = [r.opponent_elo for r in _recent_records(state) if r.opponent_elo is not None]
        return _average(records)

    def _opponent_surface_elo_average(self, state: PlayerState) -> float | None:
        records = [
            r.opponent_surface_elo
            for r in _recent_records(state)
            if r.opponent_surface_elo is not None
        ]
        return _average(records)

    def _wins_vs_higher(self, state: PlayerState) -> float | None:
        records = _recent_records(state)
        if not records:
            return None
        return float(
            sum(
                1
                for record in records
                if record.won and record.expected_win_prob is not None and record.expected_win_prob < 0.5
            )
        )

    def _losses_vs_lower(self, state: PlayerState) -> float | None:
        records = _recent_records(state)
        if not records:
            return None
        return float(
            sum(
                1
                for record in records
                if (not record.won)
                and record.expected_win_prob is not None
                and record.expected_win_prob > 0.5
            )
        )

    def _elo_overperformance(self, state: PlayerState) -> float | None:
        residuals = [
            (1.0 if record.won else 0.0) - record.expected_win_prob
            for record in _recent_records(state)
            if record.expected_win_prob is not None
        ]
        return _average(residuals)
