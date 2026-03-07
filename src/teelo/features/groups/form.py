"""Form feature group."""

from __future__ import annotations

from datetime import date, timedelta

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, MatchRecord, PlayerState

_WINDOWS = (4, 8, 16, 32, 64, 128, 256, 512)


def _win_rate_in_window(
    state: PlayerState, match_date: date, weeks: int
) -> tuple[float | None, float]:
    cutoff = match_date - timedelta(weeks=weeks)
    records: list[MatchRecord] = list(state.matches)

    wins = 0
    total = 0
    for record in records:
        if record.match_date is None or record.match_date < cutoff:
            continue
        total += 1
        if record.won:
            wins += 1

    win_rate = (wins / total) if total >= 5 else None
    return win_rate, float(total)


def _surface_win_rate(state: PlayerState, surface: str | None) -> float | None:
    if surface is None:
        return None

    wins = state.surface_wins[surface]
    losses = state.surface_losses[surface]
    total = wins + losses
    if total < 5:
        return None
    return wins / total


def _level_win_rate(state: PlayerState, level_code: str) -> float | None:
    wins = state.level_wins[level_code]
    losses = state.level_losses[level_code]
    total = wins + losses
    if total < 5:
        return None
    return wins / total


def _career_win_rate(state: PlayerState) -> float | None:
    total = state.wins_total + state.losses_total
    if total < 10:
        return None
    return state.wins_total / total


class FormFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "form"

    def feature_names(self) -> list[str]:
        names: list[str] = []
        for weeks in _WINDOWS:
            names.append(f"win_rate_{weeks}w_a")
            names.append(f"win_rate_{weeks}w_b")
            names.append(f"match_count_{weeks}w_a")
            names.append(f"match_count_{weeks}w_b")

        names.extend(
            [
                "surface_win_rate_a",
                "surface_win_rate_b",
                "level_win_rate_a",
                "level_win_rate_b",
                "career_win_rate_a",
                "career_win_rate_b",
            ]
        )
        return names

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        features: dict[str, float | None] = {}

        for weeks in _WINDOWS:
            if ctx.match_date is None:
                features[f"win_rate_{weeks}w_a"] = None
                features[f"win_rate_{weeks}w_b"] = None
                features[f"match_count_{weeks}w_a"] = None
                features[f"match_count_{weeks}w_b"] = None
                continue

            win_rate_a, match_count_a = _win_rate_in_window(state_a, ctx.match_date, weeks)
            win_rate_b, match_count_b = _win_rate_in_window(state_b, ctx.match_date, weeks)
            features[f"win_rate_{weeks}w_a"] = win_rate_a
            features[f"win_rate_{weeks}w_b"] = win_rate_b
            features[f"match_count_{weeks}w_a"] = match_count_a
            features[f"match_count_{weeks}w_b"] = match_count_b

        features["surface_win_rate_a"] = _surface_win_rate(state_a, ctx.surface)
        features["surface_win_rate_b"] = _surface_win_rate(state_b, ctx.surface)
        features["level_win_rate_a"] = _level_win_rate(state_a, ctx.level_code)
        features["level_win_rate_b"] = _level_win_rate(state_b, ctx.level_code)
        features["career_win_rate_a"] = _career_win_rate(state_a)
        features["career_win_rate_b"] = _career_win_rate(state_b)

        return features
