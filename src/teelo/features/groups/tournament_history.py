"""Tournament-specific history features."""

from __future__ import annotations

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, PlayerState

class TournamentHistoryFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "tournament_history"

    def feature_names(self) -> list[str]:
        return [
            "tournament_match_count_a",
            "tournament_match_count_b",
            "tournament_win_rate_a",
            "tournament_win_rate_b",
        ]

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        tournament_id = ctx.tournament_id
        count_a = float(state_a.tournament_matches.get(tournament_id, 0)) if tournament_id is not None else 0.0
        count_b = float(state_b.tournament_matches.get(tournament_id, 0)) if tournament_id is not None else 0.0
        return {
            "tournament_match_count_a": count_a,
            "tournament_match_count_b": count_b,
            "tournament_win_rate_a": self._win_rate(state_a, tournament_id),
            "tournament_win_rate_b": self._win_rate(state_b, tournament_id),
        }

    def _win_rate(self, state: PlayerState, tournament_id: int | None) -> float | None:
        if tournament_id is None:
            return None
        total = state.tournament_matches.get(tournament_id, 0)
        if total == 0:
            return 0.5
        wins = state.tournament_wins.get(tournament_id, 0)
        return wins / total
