"""Score-derived dominance and clutch features."""

from __future__ import annotations

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, MatchRecord, PlayerState

_WINDOW = 8
_MIN_RATE_SAMPLE = 3


def _recent_records(state: PlayerState, limit: int = _WINDOW) -> list[MatchRecord]:
    return list(state.matches)[-limit:]


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _rate(numerator: float, denominator: int) -> float | None:
    if denominator < _MIN_RATE_SAMPLE:
        return None
    return numerator / denominator


class DominanceFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "dominance"

    def feature_names(self) -> list[str]:
        return [
            "game_diff_avg_8_a",
            "game_diff_avg_8_b",
            "set_diff_avg_8_a",
            "set_diff_avg_8_b",
            "straight_sets_rate_8_a",
            "straight_sets_rate_8_b",
            "deciding_set_rate_8_a",
            "deciding_set_rate_8_b",
            "tiebreak_rate_8_a",
            "tiebreak_rate_8_b",
            "tiebreak_win_rate_8_a",
            "tiebreak_win_rate_8_b",
            "close_match_rate_8_a",
            "close_match_rate_8_b",
        ]

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        _ = ctx
        return {
            "game_diff_avg_8_a": self._game_diff_average(state_a),
            "game_diff_avg_8_b": self._game_diff_average(state_b),
            "set_diff_avg_8_a": self._set_diff_average(state_a),
            "set_diff_avg_8_b": self._set_diff_average(state_b),
            "straight_sets_rate_8_a": self._straight_sets_rate(state_a),
            "straight_sets_rate_8_b": self._straight_sets_rate(state_b),
            "deciding_set_rate_8_a": self._deciding_set_rate(state_a),
            "deciding_set_rate_8_b": self._deciding_set_rate(state_b),
            "tiebreak_rate_8_a": self._tiebreak_rate(state_a),
            "tiebreak_rate_8_b": self._tiebreak_rate(state_b),
            "tiebreak_win_rate_8_a": self._tiebreak_win_rate(state_a),
            "tiebreak_win_rate_8_b": self._tiebreak_win_rate(state_b),
            "close_match_rate_8_a": self._close_match_rate(state_a),
            "close_match_rate_8_b": self._close_match_rate(state_b),
        }

    def _records(self, state: PlayerState) -> list[MatchRecord]:
        return _recent_records(state)

    def _game_diff_average(self, state: PlayerState) -> float | None:
        records = self._records(state)
        return _average([float(r.games_won - r.games_lost) for r in records])

    def _set_diff_average(self, state: PlayerState) -> float | None:
        records = self._records(state)
        return _average([float(r.sets_won - r.sets_lost) for r in records])

    def _straight_sets_rate(self, state: PlayerState) -> float | None:
        records = self._records(state)
        return _rate(sum(1.0 for r in records if r.straight_sets), len(records))

    def _deciding_set_rate(self, state: PlayerState) -> float | None:
        records = self._records(state)
        return _rate(sum(1.0 for r in records if r.deciding_set_played), len(records))

    def _tiebreak_rate(self, state: PlayerState) -> float | None:
        records = self._records(state)
        return _rate(sum(1.0 for r in records if r.tiebreaks_played > 0), len(records))

    def _tiebreak_win_rate(self, state: PlayerState) -> float | None:
        records = self._records(state)
        denominator = sum(r.tiebreaks_played for r in records)
        if len(records) < _MIN_RATE_SAMPLE or denominator == 0:
            return None
        numerator = sum(r.tiebreaks_won for r in records)
        return numerator / denominator

    def _close_match_rate(self, state: PlayerState) -> float | None:
        records = self._records(state)
        return _rate(sum(1.0 for r in records if r.close_match), len(records))
