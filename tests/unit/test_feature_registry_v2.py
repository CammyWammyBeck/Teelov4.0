from teelo.features import build_registry, default_preset_for_feature_set
from teelo.ml.selection import ABLATION_GROUP_PREFIXES
import subprocess
import sys


def test_build_registry_baseline_v2_includes_new_feature_groups() -> None:
    registry = build_registry("baseline_v2")
    feature_names = registry.all_feature_names()

    assert "opp_elo_avg_8_a" in feature_names
    assert "game_diff_avg_8_a" in feature_names
    assert "rest_days_diff_ab" in feature_names
    assert "tournament_match_count_a" in feature_names
    assert "surface_elo_default_a" in feature_names


def test_existing_registry_presets_remain_stable() -> None:
    full_names = build_registry("full").all_feature_names()
    trimmed_names = build_registry("trimmed").all_feature_names()
    trimmed_v2_names = build_registry("trimmed_v2").all_feature_names()
    trimmed_v2b_names = build_registry("trimmed_v2b").all_feature_names()

    assert "opp_elo_avg_8_a" not in full_names
    assert "match_count_4w_a" in full_names
    assert "match_count_4w_a" not in trimmed_names
    assert "opp_elo_avg_8_a" in trimmed_v2_names
    assert "surface_elo_default_a" not in trimmed_v2_names
    assert len(trimmed_v2_names) == 95
    assert "opp_elo_avg_8_a" in trimmed_v2b_names
    assert "close_match_rate_8_a" not in trimmed_v2b_names
    assert len(trimmed_v2b_names) == 85


def test_feature_set_name_maps_to_expected_preset() -> None:
    assert default_preset_for_feature_set("baseline_v1") == "full"
    assert default_preset_for_feature_set("trimmed_v1") == "trimmed"
    assert default_preset_for_feature_set("baseline_v2") == "baseline_v2"
    assert default_preset_for_feature_set("trimmed_v2") == "trimmed_v2"
    assert default_preset_for_feature_set("trimmed_v2b") == "trimmed_v2b"


def test_selection_ablation_prefixes_cover_baseline_v2_groups() -> None:
    for prefix in (
        "opp_",
        "game_diff_",
        "set_diff_",
        "straight_",
        "deciding_",
        "tiebreak_",
        "close_",
        "rest_",
        "tournament_",
        "surface_elo_",
        "match_date_estimated_",
    ):
        assert prefix in ABLATION_GROUP_PREFIXES


def test_feature_engine_help_lists_trimmed_v2_preset() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "teelo.features.engine", "--help"],
        check=False,
        capture_output=True,
        text=True,
        env={"PYTHONPATH": "src"},
    )

    assert result.returncode == 0
    assert "trimmed_v2" in result.stdout
    assert "trimmed_v2b" in result.stdout
