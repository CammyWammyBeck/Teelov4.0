"""Feature group implementations."""

from __future__ import annotations

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, PlayerState


class ContextFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "context"

    @property
    def neutral_display(self) -> bool:
        return True

    def feature_names(self) -> list[str]:
        return [
            "surface_hard",
            "surface_clay",
            "surface_grass",
            "surface_indoor",
            "level_G",
            "level_M",
            "level_A",
            "level_C",
            "level_F",
            "round_F",
            "round_SF",
            "round_QF",
            "round_R16",
            "round_R32",
            "round_R64",
            "round_R128",
            "round_Q1",
            "round_Q2",
            "round_Q3",
            "round_RR",
            "tour_wta",
            "year",
        ]

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        _ = state_a, state_b

        features: dict[str, float | None] = {}

        if ctx.surface is None:
            features["surface_hard"] = None
            features["surface_clay"] = None
            features["surface_grass"] = None
            features["surface_indoor"] = None
        else:
            surface = ctx.surface.casefold()
            features["surface_hard"] = 1.0 if "hard" in surface else 0.0
            features["surface_clay"] = 1.0 if "clay" in surface else 0.0
            features["surface_grass"] = 1.0 if "grass" in surface else 0.0
            features["surface_indoor"] = 1.0 if "indoor" in surface else 0.0

        level = ctx.level_code.upper().strip()
        if level.startswith("W"):
            level = level[1:]
        for code in ("G", "M", "A", "C", "F"):
            features[f"level_{code}"] = 1.0 if level == code else 0.0

        if ctx.round is None:
            for round_code in (
                "F",
                "SF",
                "QF",
                "R16",
                "R32",
                "R64",
                "R128",
                "Q1",
                "Q2",
                "Q3",
                "RR",
            ):
                features[f"round_{round_code}"] = None
        else:
            round_code = ctx.round.upper().strip()
            for code in ("F", "SF", "QF", "R16", "R32", "R64", "R128", "Q1", "Q2", "Q3", "RR"):
                features[f"round_{code}"] = 1.0 if round_code == code else 0.0

        if ctx.tour is None:
            features["tour_wta"] = None
        else:
            features["tour_wta"] = 1.0 if ctx.tour.upper() == "WTA" else 0.0

        features["year"] = float(ctx.year) if ctx.year else None

        return features
