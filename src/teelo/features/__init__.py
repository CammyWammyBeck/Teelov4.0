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


def default_preset_for_feature_set(feature_set_name: str) -> str:
    if feature_set_name == "trimmed_v1":
        return "trimmed"
    if feature_set_name == "baseline_v2":
        return "baseline_v2"
    return "full"


def build_registry(preset: str = "full") -> FeatureRegistry:
    """Build a FeatureRegistry with all groups registered.

    Presets:
        "full"    - all 110 features (baseline_v1)
        "trimmed" - drops match_count_* and seed features (trimmed_v1)
        "baseline_v2" - baseline_v1 plus additive v2 feature groups
    """
    if preset == "baseline_v1":
        preset = "full"
    elif preset == "trimmed_v1":
        preset = "trimmed"

    exclude = EXCLUDED_TRIMMED if preset == "trimmed" else None
    registry = FeatureRegistry(exclude=exclude)
    registry.register(ContextFeatures())
    registry.register(EloCoreFeatures())
    registry.register(EloHistoryFeatures())
    registry.register(EloVarianceFeatures())
    registry.register(FormFeatures())
    registry.register(H2HFeatures())
    registry.register(ActivityFeatures())
    if preset == "baseline_v2":
        registry.register(OpponentQualityFeatures())
        registry.register(DominanceFeatures())
        registry.register(FatigueFeatures())
        registry.register(TournamentHistoryFeatures())
        registry.register(ConfidenceFeatures())
    return registry
