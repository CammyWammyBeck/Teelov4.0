"""
Feature store module for ML experimentation.

Provides a versioned feature store where features are defined as
modular classes that can be easily added, removed, or modified.
"""

from __future__ import annotations

from teelo.features.groups.activity import ActivityFeatures
from teelo.features.groups.confidence import ConfidenceFeatures
from teelo.features.groups.context import ContextFeatures
from teelo.features.groups.dominance import DominanceFeatures
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


def default_preset_for_feature_set(feature_set_name: str) -> str:
    if feature_set_name == "trimmed_v1":
        return "trimmed"
    if feature_set_name == "trimmed_v2":
        return "trimmed_v2"
    if feature_set_name == "trimmed_v2b":
        return "trimmed_v2b"
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
    registry = FeatureRegistry(exclude=exclude)
    registry.register(ContextFeatures())
    registry.register(EloCoreFeatures())
    registry.register(EloHistoryFeatures())
    registry.register(EloVarianceFeatures())
    registry.register(FormFeatures())
    registry.register(H2HFeatures())
    registry.register(ActivityFeatures())
    if preset in {"baseline_v2", "trimmed_v2", "trimmed_v2b"}:
        registry.register(OpponentQualityFeatures())
        registry.register(DominanceFeatures())
        registry.register(FatigueFeatures())
        registry.register(TournamentHistoryFeatures())
        registry.register(ConfidenceFeatures())
    return registry
