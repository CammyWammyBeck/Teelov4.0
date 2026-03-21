"""Country and region performance features."""

from __future__ import annotations

from teelo.features.registry import FeatureGroup
from teelo.features.state import MatchContext, PlayerState
from teelo.utils.geo import ioc_to_region


def _career_win_rate(state: PlayerState) -> float:
    total = state.wins_total + state.losses_total
    if total == 0:
        return 0.5
    return state.wins_total / total


class CountryPerformanceFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "country_performance"

    def feature_names(self) -> list[str]:
        names: list[str] = []
        for suffix in ("a", "b"):
            for feat in (
                "country_win_rate",
                "country_matches",
                "country_delta",
                "region_win_rate",
                "region_matches",
                "region_delta",
                "is_home",
            ):
                names.append(f"{feat}_{suffix}")
        return names

    def compute(
        self,
        state_a: PlayerState,
        state_b: PlayerState,
        ctx: MatchContext,
    ) -> dict[str, float | None]:
        features: dict[str, float | None] = {}
        country = ctx.tournament_country_ioc
        region = ioc_to_region(country) if country else None

        for suffix, state, nationality in (
            ("a", state_a, ctx.player_a_nationality),
            ("b", state_b, ctx.player_b_nationality),
        ):
            career_wr = _career_win_rate(state)

            # Country features
            if country and country in state.country_record:
                wins, losses = state.country_record[country]
                total = wins + losses
                features[f"country_matches_{suffix}"] = float(total)
                wr = wins / total if total > 0 else 0.5
                features[f"country_win_rate_{suffix}"] = wr
                features[f"country_delta_{suffix}"] = wr - career_wr
            else:
                features[f"country_win_rate_{suffix}"] = 0.5
                features[f"country_matches_{suffix}"] = 0.0
                features[f"country_delta_{suffix}"] = 0.0

            # Region features
            if region and region in state.region_record:
                wins, losses = state.region_record[region]
                total = wins + losses
                features[f"region_matches_{suffix}"] = float(total)
                wr = wins / total if total > 0 else 0.5
                features[f"region_win_rate_{suffix}"] = wr
                features[f"region_delta_{suffix}"] = wr - career_wr
            else:
                features[f"region_win_rate_{suffix}"] = 0.5
                features[f"region_matches_{suffix}"] = 0.0
                features[f"region_delta_{suffix}"] = 0.0

            # Is home
            if nationality and country:
                features[f"is_home_{suffix}"] = 1.0 if nationality == country else 0.0
            else:
                features[f"is_home_{suffix}"] = 0.0

        return features
