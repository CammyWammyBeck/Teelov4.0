"""Tests for the per-prediction explanation engine."""

from __future__ import annotations

import numpy as np
import pytest
import xgboost as xgb

from teelo.ml.explanations import (
    _attribute_pp,
    _classify_player_key,
    _pair_column_indices,
    _remap_swap_contribs,
    build_explanation,
    build_explanations_batch,
)

FEATURE_NAMES = [
    "elo_a",
    "elo_b",
    "h2h_a_wins",
    "h2h_b_wins",
    "elo_diff",
    "surface_hard",
]


@pytest.fixture(scope="module")
def tiny_booster() -> tuple[xgb.Booster, list[str]]:
    rng = np.random.default_rng(0)
    n = 400
    X = rng.normal(size=(n, len(FEATURE_NAMES))).astype(np.float32)  # noqa: N806
    # A wins iff elo_a > elo_b, with mild h2h nudge — enough signal to make
    # contributions non-trivial.
    logits = (X[:, 0] - X[:, 1]) + 0.3 * (X[:, 2] - X[:, 3])
    y = (logits > 0).astype(np.int32)
    model = xgb.XGBClassifier(
        n_estimators=8,
        max_depth=3,
        learning_rate=0.3,
        eval_metric="logloss",
    )
    model.fit(X, y)
    return model.get_booster(), list(FEATURE_NAMES)


def _sample_features() -> dict[str, float]:
    return {
        "elo_a": 2.0,
        "elo_b": -1.0,
        "h2h_a_wins": 3.0,
        "h2h_b_wins": 1.0,
        "elo_diff": 3.0,
        "surface_hard": 1.0,
    }


def test_pair_column_indices_covers_suffix_and_prefix_pairs() -> None:
    pairs = _pair_column_indices(FEATURE_NAMES)
    pair_sets = {frozenset(p) for p in pairs}
    idx = {n: i for i, n in enumerate(FEATURE_NAMES)}
    assert frozenset((idx["elo_a"], idx["elo_b"])) in pair_sets
    assert frozenset((idx["h2h_a_wins"], idx["h2h_b_wins"])) in pair_sets
    # diff, neutral, etc. must NOT appear as position-swap pairs
    assert len(pair_sets) == 2


def test_remap_swap_contribs_swaps_and_negates() -> None:
    # Column order: [elo_a, elo_b, h2h_a_wins, h2h_b_wins, elo_diff, surface_hard]
    contribs_swap = np.array([[0.4, -0.1, 0.2, -0.05, -0.3, 0.07]])
    pair_idx = _pair_column_indices(FEATURE_NAMES)
    remapped = _remap_swap_contribs(contribs_swap, pair_idx)
    # Positional swap then sign flip: elo_a <-> elo_b (then negate), etc.
    expected = np.array([[0.1, -0.4, 0.05, -0.2, 0.3, -0.07]])
    np.testing.assert_allclose(remapped, expected)


def test_attribute_pp_sum_invariant() -> None:
    # signed_sum = 1.0 → scale = target / 1.0 = 14.0 → pp sums to 14.0
    contribs = np.array([[1.0, -2.0, 0.5, 1.5]])
    predictions = np.array([0.64])
    pp = _attribute_pp(contribs, predictions, base_prob=0.5)
    np.testing.assert_allclose(pp.sum(axis=1), (predictions - 0.5) * 100.0)


def test_attribute_pp_handles_zero_signed_sum() -> None:
    # signed_sum = 0 → scaling falls back to zero to avoid div-by-zero
    contribs = np.array([[1.0, -1.0, 0.5, -0.5]])
    predictions = np.array([0.73])
    pp = _attribute_pp(contribs, predictions, base_prob=0.5)
    np.testing.assert_array_equal(pp, np.zeros_like(contribs))


def test_classify_player_key_patterns() -> None:
    assert _classify_player_key("elo_a") == ("elo", "a")
    assert _classify_player_key("elo_b") == ("elo", "b")
    assert _classify_player_key("h2h_a_wins") == ("h2h_wins", "a")
    assert _classify_player_key("elo_diff") == (None, None)
    assert _classify_player_key("surface_hard") == (None, None)


def test_build_explanation_sums_to_prediction_delta(tiny_booster) -> None:
    booster, feature_names = tiny_booster
    payload = build_explanation(
        booster=booster,
        feature_names=feature_names,
        feature_set_name="test_fs",
        model_artifact="test.json",
        features=_sample_features(),
        prediction_a=0.64,
    )
    total = sum(payload["contributions"].values())
    assert total == pytest.approx((0.64 - 0.5) * 100.0, abs=1e-6)


def test_build_explanation_payload_shape(tiny_booster) -> None:
    booster, feature_names = tiny_booster
    payload = build_explanation(
        booster=booster,
        feature_names=feature_names,
        feature_set_name="test_fs",
        model_artifact="test.json",
        features=_sample_features(),
        prediction_a=0.7,
    )
    assert payload["model_artifact"] == "test.json"
    assert payload["feature_set_name"] == "test_fs"
    assert payload["base_prob"] == 0.5
    assert payload["prediction_a"] == pytest.approx(0.7)
    assert set(payload["contributions"].keys()) == set(feature_names)
    # paired_rows should collapse elo_a/elo_b and h2h_a_wins/h2h_b_wins
    paired_keys = {r["key"] for r in payload["paired_rows"]}
    assert "elo" in paired_keys
    assert "h2h_wins" in paired_keys
    # Paired row pp == sum of its component contributions
    elo_row = next(r for r in payload["paired_rows"] if r["key"] == "elo")
    expected = payload["contributions"]["elo_a"] + payload["contributions"]["elo_b"]
    assert elo_row["pp"] == pytest.approx(expected)


def test_build_explanation_prediction_at_base(tiny_booster) -> None:
    booster, feature_names = tiny_booster
    payload = build_explanation(
        booster=booster,
        feature_names=feature_names,
        feature_set_name="test_fs",
        model_artifact="test.json",
        features=_sample_features(),
        prediction_a=0.5,
    )
    assert sum(payload["contributions"].values()) == pytest.approx(0.0)


def test_build_explanations_batch_preserves_order(tiny_booster) -> None:
    booster, feature_names = tiny_booster
    features_list = [_sample_features(), {**_sample_features(), "elo_a": -2.0}]
    predictions = [0.7, 0.3]
    payloads = build_explanations_batch(
        booster=booster,
        feature_names=feature_names,
        feature_set_name="test_fs",
        model_artifact="test.json",
        features_list=features_list,
        predictions_a=predictions,
    )
    assert len(payloads) == 2
    assert payloads[0]["prediction_a"] == pytest.approx(0.7)
    assert payloads[1]["prediction_a"] == pytest.approx(0.3)
    for payload, pred in zip(payloads, predictions):
        total = sum(payload["contributions"].values())
        assert total == pytest.approx((pred - 0.5) * 100.0, abs=1e-6)


def test_swap_symmetry_of_explanation(tiny_booster) -> None:
    """Swapping A/B inputs should reverse the sign of every contribution and
    negate paired-row pp values, leaving magnitudes invariant. This validates
    orientation handling end to end."""
    booster, feature_names = tiny_booster
    orig_features = _sample_features()
    # Build the mirrored view the way swap_ab_features does
    from teelo.ml.randomize import swap_ab_features

    swapped_features = swap_ab_features(orig_features)

    # The swap-averaged prediction for the swapped-input case is (1 - original_prob)
    # by construction. Pick matching predictions so attributions reflect the mirror.
    orig_pred = 0.72
    swap_pred = 1.0 - orig_pred

    orig_payload = build_explanation(
        booster=booster,
        feature_names=feature_names,
        feature_set_name="test_fs",
        model_artifact="test.json",
        features=orig_features,
        prediction_a=orig_pred,
    )
    swap_payload = build_explanation(
        booster=booster,
        feature_names=feature_names,
        feature_set_name="test_fs",
        model_artifact="test.json",
        features=swapped_features,
        prediction_a=swap_pred,
    )

    # Check that elo's paired-row pp flips sign under swap
    def paired(payload, key):
        return next(r["pp"] for r in payload["paired_rows"] if r["key"] == key)

    assert paired(orig_payload, "elo") == pytest.approx(-paired(swap_payload, "elo"))
    assert paired(orig_payload, "h2h_wins") == pytest.approx(-paired(swap_payload, "h2h_wins"))
