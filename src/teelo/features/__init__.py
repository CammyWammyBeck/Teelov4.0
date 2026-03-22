"""
Feature store module for ML experimentation.

Provides a versioned feature store where features are defined as
modular classes that can be easily added, removed, or modified.
"""

from __future__ import annotations

from teelo.features.groups.activity import ActivityFeatures
from teelo.features.groups.confidence import ConfidenceFeatures
from teelo.features.groups.context import ContextFeatures
from teelo.features.groups.score_profile import ScoreProfileFeatures
from teelo.features.groups.country_performance import CountryPerformanceFeatures
from teelo.features.groups.elo import EloCoreFeatures, EloHistoryFeatures, EloVarianceFeatures
from teelo.features.groups.fatigue import FatigueFeatures
from teelo.features.groups.form import FormFeatures
from teelo.features.groups.h2h import H2HFeatures
from teelo.features.groups.opponent_quality import OpponentQualityFeatures
from teelo.features.groups.tournament_history import TournamentHistoryFeatures
from teelo.features.registry import FeatureRegistry

# Features to drop based on selection analysis:
# - match_count_*: removing them *improved* the model
# - seed_*, both_seeded: zero impact
EXCLUDED_TRIMMED = {
    "match_count_4w_a",
    "match_count_4w_b",
    "match_count_8w_a",
    "match_count_8w_b",
    "match_count_16w_a",
    "match_count_16w_b",
    "match_count_32w_a",
    "match_count_32w_b",
    "match_count_64w_a",
    "match_count_64w_b",
    "match_count_128w_a",
    "match_count_128w_b",
    "match_count_256w_a",
    "match_count_256w_b",
    "match_count_512w_a",
    "match_count_512w_b",
    "seed_a",
    "seed_b",
    "seed_diff",
    "both_seeded",
}

EXCLUDED_TRIMMED_V2 = {
    "both_seeded",
    "days_since_debut_a",
    "days_since_debut_b",
    "days_since_last_a",
    "days_since_last_b",
    "deciding_set_rate_8_b",
    "elo_var_16_a",
    "elo_var_16_b",
    "elo_var_32_a",
    "elo_var_8_b",
    "games_7d_diff_ab",
    "games_last_match_a",
    "games_last_match_b",
    "games_tournament_a",
    "games_tournament_b",
    "h2h_a_wins_6m",
    "h2h_a_wins_level",
    "h2h_a_wins_surface",
    "h2h_b_wins_6m",
    "h2h_b_wins_level",
    "h2h_b_wins_surface",
    "h2h_sample_count",
    "h2h_surface_sample_count",
    "losses_vs_lower_elo_8_a",
    "losses_vs_lower_elo_8_b",
    "match_count_16w_a",
    "match_count_16w_b",
    "match_count_4w_a",
    "match_count_4w_b",
    "match_count_8w_a",
    "match_count_8w_b",
    "peak_elo_a",
    "peak_elo_b",
    "peak_ratio_a",
    "peak_ratio_b",
    "round_Q2",
    "round_QF",
    "round_R128",
    "round_R16",
    "round_R64",
    "round_RR",
    "seed_a",
    "seed_b",
    "seed_diff",
    "surface_clay",
    "surface_elo_a",
    "surface_elo_b",
    "surface_elo_default_a",
    "surface_elo_default_b",
    "surface_elo_observed_a",
    "surface_elo_observed_b",
    "surface_grass",
    "surface_indoor",
    "tiebreak_win_rate_8_a",
    "tiebreak_win_rate_8_b",
    "tournament_history_sample_count_a",
    "tournament_history_sample_count_b",
    "win_rate_128w_a",
    "win_rate_128w_b",
    "win_rate_32w_a",
    "win_rate_32w_b",
    "win_rate_64w_a",
    "win_rate_64w_b",
}

EXCLUDED_TRIMMED_V2B = EXCLUDED_TRIMMED_V2 | {
    "close_match_rate_8_a",
    "close_match_rate_8_b",
    "deciding_set_rate_8_a",
    "elo_overperf_8_a",
    "elo_overperf_8_b",
    "elo_var_64_a",
    "elo_var_8_a",
    "opp_surface_elo_avg_8_b",
    "rest_days_diff_ab",
    "round_F",
}

# Selection-report top-65 + a/b pairs + level_G + country_win_rate (76 features kept)
EXCLUDED_TRIMMED_V3 = {
    "both_seeded",
    "career_win_rate_a",
    "career_win_rate_b",
    "close_match_rate_64_a",
    "close_match_rate_64_b",
    "close_match_rate_8_a",
    "close_match_rate_8_b",
    "comeback_rate_64_a",
    "comeback_rate_64_b",
    "country_delta_a",
    "country_delta_b",
    "country_matches_a",
    "country_matches_b",
    "deciding_set_rate_64_a",
    "deciding_set_rate_64_b",
    "deciding_set_rate_8_a",
    "deciding_set_rate_8_b",
    "deciding_set_win_rate_64_a",
    "deciding_set_win_rate_64_b",
    "elo_momentum_a",
    "elo_momentum_b",
    "elo_overperf_8_a",
    "elo_overperf_8_b",
    "elo_var_16_a",
    "elo_var_16_b",
    "elo_var_32_a",
    "elo_var_32_b",
    "elo_var_64_a",
    "elo_var_64_b",
    "elo_var_8_a",
    "elo_var_8_b",
    "game_diff_avg_8_a",
    "game_diff_avg_8_b",
    "games_last_match_a",
    "games_last_match_b",
    "games_this_tournament_diff_ab",
    "games_7d_diff_ab",
    "games_tournament_a",
    "games_tournament_b",
    "h2h_a_wins_2y",
    "h2h_a_wins_6m",
    "h2h_a_wins_level",
    "h2h_a_wins_surface",
    "h2h_b_wins_2y",
    "h2h_b_wins_6m",
    "h2h_b_wins_level",
    "h2h_b_wins_surface",
    "h2h_sample_count",
    "h2h_surface_sample_count",
    "h2h_total",
    "is_home_a",
    "is_home_b",
    "level_A",
    "level_win_rate_a",
    "level_win_rate_b",
    "losses_vs_lower_elo_8_a",
    "losses_vs_lower_elo_8_b",
    "match_count_256w_a",
    "match_count_256w_b",
    "matches_16w_a",
    "matches_16w_b",
    "matches_4w_a",
    "matches_4w_b",
    "matches_8w_a",
    "matches_8w_b",
    "month_cos",
    "month_sin",
    "off_surface_elo_a",
    "off_surface_elo_b",
    "opponent_clutch_score_a",
    "opponent_clutch_score_b",
    "opp_surface_elo_avg_8_a",
    "opp_surface_elo_avg_8_b",
    "peak_elo_a",
    "peak_elo_b",
    "peak_ratio_a",
    "peak_ratio_b",
    "region_delta_a",
    "region_delta_b",
    "region_matches_a",
    "region_matches_b",
    "region_win_rate_a",
    "region_win_rate_b",
    "rest_days_a",
    "rest_days_b",
    "rest_days_diff_ab",
    "round_F",
    "round_Q2",
    "round_Q3",
    "round_QF",
    "round_R128",
    "round_R16",
    "round_R32",
    "round_R64",
    "round_RR",
    "round_SF",
    "seed_a",
    "seed_b",
    "seed_diff",
    "straight_sets_win_rate_64_a",
    "straight_sets_win_rate_64_b",
    "surface_clay",
    "surface_elo_default_a",
    "surface_elo_default_b",
    "surface_elo_observed_a",
    "surface_elo_observed_b",
    "surface_grass",
    "surface_hard",
    "surface_indoor",
    "surface_win_rate_a",
    "surface_win_rate_b",
    "tiebreak_win_rate_64_a",
    "tiebreak_win_rate_64_b",
    "tiebreak_win_rate_8_a",
    "tiebreak_win_rate_8_b",
    "tournament_history_sample_count_a",
    "tournament_history_sample_count_b",
    "tournament_match_count_a",
    "tournament_match_count_b",
    "tournament_win_rate_a",
    "tournament_win_rate_b",
    "vs_clutch_matches_a",
    "vs_clutch_matches_b",
    "vs_clutch_win_rate_a",
    "vs_clutch_win_rate_b",
    "vs_non_clutch_matches_a",
    "vs_non_clutch_matches_b",
    "vs_non_clutch_win_rate_a",
    "vs_non_clutch_win_rate_b",
    "vs_normal_clutch_win_rate_a",
    "vs_normal_clutch_win_rate_b",
    "win_rate_128w_a",
    "win_rate_128w_b",
    "win_rate_16w_a",
    "win_rate_16w_b",
    "win_rate_256w_a",
    "win_rate_256w_b",
    "win_rate_32w_a",
    "win_rate_32w_b",
    "win_rate_4w_a",
    "win_rate_4w_b",
    "win_rate_512w_a",
    "win_rate_512w_b",
    "year",
    "year_progress",
}

# Ordered from oldest to newest. Append new presets to the end.
PRESET_ORDER: list[str] = [
    "full",
    "trimmed",
    "baseline_v2",
    "trimmed_v2",
    "trimmed_v2b",
    "trimmed_v3",
]


def latest_preset() -> str:
    """Return the most recent preset name."""
    return PRESET_ORDER[-1]


# NOTE: PRESET_ORDER above is the canonical source of preset ordering.
def default_preset_for_feature_set(feature_set_name: str) -> str:
    if feature_set_name == "trimmed_v1":
        return "trimmed"
    if feature_set_name == "trimmed_v2":
        return "trimmed_v2"
    if feature_set_name == "trimmed_v2b":
        return "trimmed_v2b"
    if feature_set_name == "trimmed_v3":
        return "trimmed_v3"
    if feature_set_name == "baseline_v2":
        return "baseline_v2"
    return "full"


def build_registry(preset: str = "full") -> FeatureRegistry:
    """Build a FeatureRegistry with all groups registered.

    Presets:
        "full"    - all 110 features (baseline_v1)
        "trimmed" - drops match_count_* and seed features (trimmed_v1)
        "baseline_v2" - baseline_v1 plus additive v2 feature groups
        "trimmed_v2" - report-derived top-95 subset of baseline_v2
        "trimmed_v2b" - ablation-informed top-85 subset of baseline_v2
        "trimmed_v3" - selection-report top-76 subset of baseline_v2
    """
    if preset == "baseline_v1":
        preset = "full"
    elif preset == "trimmed_v1":
        preset = "trimmed"

    exclude = None
    if preset == "trimmed":
        exclude = EXCLUDED_TRIMMED
    elif preset == "trimmed_v2":
        exclude = EXCLUDED_TRIMMED_V2
    elif preset == "trimmed_v2b":
        exclude = EXCLUDED_TRIMMED_V2B
    elif preset == "trimmed_v3":
        exclude = EXCLUDED_TRIMMED_V3
    registry = FeatureRegistry(exclude=exclude)
    registry.register(ContextFeatures())
    registry.register(EloCoreFeatures())
    registry.register(EloHistoryFeatures())
    registry.register(EloVarianceFeatures())
    registry.register(FormFeatures())
    registry.register(H2HFeatures())
    registry.register(ActivityFeatures())
    if preset in {"baseline_v2", "trimmed_v2", "trimmed_v2b", "trimmed_v3"}:
        registry.register(OpponentQualityFeatures())
        registry.register(ScoreProfileFeatures())
        registry.register(FatigueFeatures())
        registry.register(TournamentHistoryFeatures())
        registry.register(ConfidenceFeatures())
        registry.register(CountryPerformanceFeatures())
    return registry
