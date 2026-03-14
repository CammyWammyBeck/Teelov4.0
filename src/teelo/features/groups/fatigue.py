"""Fatigue and workload differential features."""

from __future__ import annotations

from datetime import timedelta

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, MatchRecord, PlayerState

_WINDOW_DAYS = 7


def _recent_records(state: PlayerState, ctx: MatchContext) -> list[MatchRecord]:
    if ctx.match_date is None:
        return []
    cutoff = ctx.match_date - timedelta(days=_WINDOW_DAYS)
    return [
        record
        for record in state.matches
        if record.match_date is not None and record.match_date >= cutoff
    ]


def _games_sum(records: list[MatchRecord]) -> float:
    return float(sum(record.games_won + record.games_lost for record in records))


def _tournament_records(state: PlayerState, ctx: MatchContext) -> list[MatchRecord]:
    if ctx.tournament_edition_id is None:
        return []
    return [
        record
        for record in state.matches
        if record.tournament_edition_id == ctx.tournament_edition_id
    ]


class FatigueFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "fatigue"

    def feature_names(self) -> list[str]:
        return [
            "rest_days_a",
            "rest_days_b",
            "rest_days_diff_ab",
            "matches_7d_diff_ab",
            "games_7d_diff_ab",
            "matches_this_tournament_diff_ab",
            "games_this_tournament_diff_ab",
        ]

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        rest_days_a = self._rest_days(state_a, ctx)
        rest_days_b = self._rest_days(state_b, ctx)
        recent_a = _recent_records(state_a, ctx)
        recent_b = _recent_records(state_b, ctx)
        tournament_a = _tournament_records(state_a, ctx)
        tournament_b = _tournament_records(state_b, ctx)

        return {
            "rest_days_a": rest_days_a,
            "rest_days_b": rest_days_b,
            "rest_days_diff_ab": (
                rest_days_a - rest_days_b
                if rest_days_a is not None and rest_days_b is not None
                else None
            ),
            "matches_7d_diff_ab": float(len(recent_a) - len(recent_b)),
            "games_7d_diff_ab": _games_sum(recent_a) - _games_sum(recent_b),
            "matches_this_tournament_diff_ab": float(len(tournament_a) - len(tournament_b)),
            "games_this_tournament_diff_ab": _games_sum(tournament_a) - _games_sum(tournament_b),
        }

    def _rest_days(self, state: PlayerState, ctx: MatchContext) -> float | None:
        if state.last_match_date is None or ctx.match_date is None:
            return None
        return float((ctx.match_date - state.last_match_date).days)
