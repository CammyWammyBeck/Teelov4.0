"""ELO-based feature groups."""

from __future__ import annotations

from statistics import variance

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, PlayerState


def _surface_elo(state: PlayerState, ctx: MatchContext) -> float | None:
    if ctx.surface is None:
        return None
    return state.surface_elo.get(ctx.surface, 1500.0)


def _elo_momentum(state: PlayerState, window: int = 8) -> float | None:
    history = list(state.elo_history)
    if len(history) < 2:
        return None

    window_entries = history[-window:]
    first_elo = window_entries[0][1]
    last_elo = window_entries[-1][1]
    return last_elo - first_elo


def _elo_delta_variance(state: PlayerState, window: int) -> float | None:
    history = list(state.elo_history)
    window_entries = history[-window:]
    if len(window_entries) < 3:
        return None

    elo_values = [entry[1] for entry in window_entries]
    deltas = [current - previous for previous, current in zip(elo_values, elo_values[1:])]
    return variance(deltas)


class EloCoreFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "elo_core"

    def feature_names(self) -> list[str]:
        return [
            "elo_a",
            "elo_b",
            "elo_diff",
            "surface_elo_a",
            "surface_elo_b",
            "surface_elo_diff",
            "peak_elo_a",
            "peak_elo_b",
            "peak_ratio_a",
            "peak_ratio_b",
            "surface_gap_a",
            "surface_gap_b",
            "off_surface_elo_a",
            "off_surface_elo_b",
        ]

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        elo_a = state_a.elo_current
        elo_b = state_b.elo_current

        surface_elo_a = _surface_elo(state_a, ctx)
        surface_elo_b = _surface_elo(state_b, ctx)

        if surface_elo_a is None or surface_elo_b is None:
            surface_elo_diff: float | None = None
        else:
            surface_elo_diff = surface_elo_a - surface_elo_b

        peak_ratio_a = state_a.elo_current / state_a.elo_peak if state_a.elo_peak > 0 else None
        peak_ratio_b = state_b.elo_current / state_b.elo_peak if state_b.elo_peak > 0 else None

        features: dict[str, float | None] = {
            "elo_a": elo_a,
            "elo_b": elo_b,
            "elo_diff": elo_a - elo_b,
            "surface_elo_a": surface_elo_a,
            "surface_elo_b": surface_elo_b,
            "surface_elo_diff": surface_elo_diff,
            "peak_elo_a": state_a.elo_peak,
            "peak_elo_b": state_b.elo_peak,
            "peak_ratio_a": peak_ratio_a,
            "peak_ratio_b": peak_ratio_b,
        }

        # Surface gap features
        for suffix, state in (("a", state_a), ("b", state_b)):
            if ctx.surface and ctx.surface in state.surface_elo:
                features[f"surface_gap_{suffix}"] = (
                    state.surface_elo[ctx.surface] - state.elo_current
                )
                other_elos = [
                    v for k, v in state.surface_elo.items() if k != ctx.surface
                ]
                features[f"off_surface_elo_{suffix}"] = (
                    sum(other_elos) / len(other_elos) if other_elos else None
                )
            else:
                features[f"surface_gap_{suffix}"] = None
                features[f"off_surface_elo_{suffix}"] = None

        return features


class EloHistoryFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "elo_history"

    def feature_names(self) -> list[str]:
        return [
            "elo_momentum_a",
            "elo_momentum_b",
        ]

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        _ = ctx
        return {
            "elo_momentum_a": _elo_momentum(state_a, window=8),
            "elo_momentum_b": _elo_momentum(state_b, window=8),
        }


class EloVarianceFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "elo_variance"

    def feature_names(self) -> list[str]:
        names: list[str] = []
        for window in (8, 16, 32, 64):
            names.append(f"elo_var_{window}_a")
            names.append(f"elo_var_{window}_b")
        return names

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        _ = ctx
        features: dict[str, float | None] = {}
        for window in (8, 16, 32, 64):
            features[f"elo_var_{window}_a"] = _elo_delta_variance(state_a, window)
            features[f"elo_var_{window}_b"] = _elo_delta_variance(state_b, window)
        return features
