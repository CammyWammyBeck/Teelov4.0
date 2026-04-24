"""Per-prediction feature contribution explanations.

Produces a payload that decomposes a single prediction into percentage-point
attributions toward Player A for every feature column the model was trained on.
The attribution is proportional: for each match,

    pp_i = c_i / sum(c_j) * (prediction_a - base_prob) * 100

where c_i is the averaged margin contribution of feature i across the two
orientations (original and A/B-swapped) that the prediction itself uses. By
construction,

    sum(pp_i) == (prediction_a - base_prob) * 100

so the per-feature pp values add up to the exact probability-point swing the
model ended up predicting. They are attributions, not causal or counterfactual
effects.

The per-feature swap conventions here MUST stay in sync with
``teelo.ml.randomize.swap_ab_features`` and the pairing logic in
``teelo.web.services.feature_display``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import numpy as np
import pandas as pd
import xgboost as xgb

from teelo.features.registry import FeatureRegistry
from teelo.ml.randomize import swap_ab_features

BASE_PROB: float = 0.5


def _pair_column_indices(feature_names: list[str]) -> list[tuple[int, int]]:
    """Return (idx_a, idx_b) for every paired column where position-swapping
    is needed when remapping the swap orientation.

    Mirrors ``swap_ab_features`` rules #1 and #2: suffix ``_a``/``_b`` and
    prefix ``h2h_a_``/``h2h_b_`` pairs.
    """
    name_to_idx = {name: i for i, name in enumerate(feature_names)}
    pairs: list[tuple[int, int]] = []
    seen: set[frozenset[str]] = set()

    for name, idx in name_to_idx.items():
        if name.endswith("_a"):
            partner = name[:-2] + "_b"
        elif name.startswith("h2h_a_"):
            partner = "h2h_b_" + name[len("h2h_a_") :]
        else:
            continue
        if partner not in name_to_idx:
            continue
        key = frozenset((name, partner))
        if key in seen:
            continue
        seen.add(key)
        pairs.append((idx, name_to_idx[partner]))
    return pairs


def _remap_swap_contribs(contribs_swap: np.ndarray, pair_idx: list[tuple[int, int]]) -> np.ndarray:
    """Remap swap-orientation contributions back to original A-vs-B semantics.

    1. Column-swap paired features: in swap orientation, the ``elo_a`` column
       held B's original elo, so its contribution belongs under ``elo_b`` in
       original space (and vice versa).
    2. Sign-flip everything: raw swap contribs describe pushes toward the
       swap-orientation positive class (= original Player B). Negating gives
       pushes toward original Player A, matching the orientation used when
       averaging the two predictions.
    """
    remapped = contribs_swap.copy()
    for idx_a, idx_b in pair_idx:
        col_a = remapped[:, idx_a].copy()
        remapped[:, idx_a] = remapped[:, idx_b]
        remapped[:, idx_b] = col_a
    return -remapped


def _compute_contribs(
    booster: xgb.Booster, X: pd.DataFrame, feature_names: list[str]
) -> np.ndarray:
    """Return contribution matrix with the bias column dropped.

    Output shape: (n_rows, n_features).
    """
    dmat = xgb.DMatrix(X.to_numpy(dtype=np.float32), feature_names=feature_names)
    contribs = np.asarray(booster.predict(dmat, pred_contribs=True), dtype=np.float64)
    # XGBoost appends bias as the final column.
    return contribs[:, :-1]


def _prepare_matrix(
    features_list: Iterable[dict[str, Any]], feature_names: list[str]
) -> pd.DataFrame:
    df = pd.DataFrame(list(features_list)).reindex(columns=feature_names)
    return df.apply(pd.to_numeric, errors="coerce")


def _attribute_pp(
    contribs_avg: np.ndarray, predictions_a: np.ndarray, base_prob: float
) -> np.ndarray:
    """Scale margin contributions so each row sums to
    ``(prediction_a - base_prob) * 100``.

    Uses signed-sum rescaling: ``pp_i = c_i * (target / sum(c_j))``. When the
    signed sum of contributions is near zero the model is effectively at
    baseline margin, so the target is also near zero and we return zeros to
    avoid a numerically unstable scale factor.
    """
    signed_sum = contribs_avg.sum(axis=1)
    target = (predictions_a - base_prob) * 100.0
    scale = np.zeros_like(signed_sum, dtype=np.float64)
    mask = np.abs(signed_sum) > 1e-9
    scale[mask] = target[mask] / signed_sum[mask]
    return contribs_avg * scale[:, None]


def _classify_player_key(key: str) -> tuple[str | None, str | None]:
    """Return (base_name, player) for a feature key.

    Mirrors ``teelo.web.services.feature_display._classify_player_key`` — kept
    local to avoid layering ml on top of web.
    """
    if len(key) > 2 and key.endswith("_a"):
        return key[:-2], "a"
    if len(key) > 2 and key.endswith("_b"):
        return key[:-2], "b"
    for marker, player in (("_a_", "a"), ("_b_", "b")):
        idx = key.find(marker)
        if idx >= 0:
            base = key[:idx] + "_" + key[idx + len(marker) :]
            return base, player
    return None, None


_DISPLAY_OVERRIDES = {
    "elo": "ELO",
    "h2h": "H2H",
    "opp": "Opponent",
    "avg": "Avg",
    "var": "Variance",
    "perf": "Performance",
    "overperf": "Overperformance",
}


def _display_name(feature_key: str) -> str:
    base, _ = _classify_player_key(feature_key)
    base = base or feature_key
    parts = base.split("_")
    return " ".join(_DISPLAY_OVERRIDES.get(p, p.capitalize()) for p in parts)


_GROUP_DISPLAY_NAMES = {
    "elo_core": "ELO Ratings",
    "elo_history": "ELO History",
    "elo_variance": "ELO Variance",
    "form": "Recent Form",
    "h2h": "Head to Head",
    "activity": "Activity",
    "dominance": "Match Dominance",
    "opponent_quality": "Opponent Quality",
    "tournament_history": "Tournament History",
    "fatigue": "Fatigue & Recovery",
    "confidence": "Data Confidence",
    "context": "Match Context",
}


def _group_display_name(group_key: str) -> str:
    return _GROUP_DISPLAY_NAMES.get(group_key, group_key.replace("_", " ").title())


def _aggregate_paired_rows(
    contributions: dict[str, float], feature_names: list[str]
) -> list[dict[str, Any]]:
    pairs: dict[str, dict[str, Any]] = {}
    other: list[tuple[str, float]] = []

    for name in feature_names:
        pp = contributions.get(name)
        if pp is None:
            continue
        base, player = _classify_player_key(name)
        if player in ("a", "b") and base is not None:
            entry = pairs.setdefault(base, {})
            entry[player] = pp
            entry.setdefault("display_source", name)
        else:
            other.append((name, pp))

    rows: list[dict[str, Any]] = []
    for base, sides in pairs.items():
        display_source = sides.pop("display_source", base)
        if "a" in sides and "b" in sides:
            rows.append(
                {
                    "key": base,
                    "display_name": _display_name(display_source),
                    "pp": float(sides["a"] + sides["b"]),
                }
            )
            continue
        # Unpaired suffix — keep as single row.
        for player_key, pp in sides.items():
            rows.append(
                {
                    "key": f"{base}_{player_key}",
                    "display_name": _display_name(f"{base}_{player_key}"),
                    "pp": float(pp),
                }
            )

    for name, pp in other:
        rows.append(
            {
                "key": name,
                "display_name": _display_name(name),
                "pp": float(pp),
            }
        )

    rows.sort(key=lambda r: abs(r["pp"]), reverse=True)
    return rows


def _aggregate_groups(
    contributions: dict[str, float], registry: FeatureRegistry | None
) -> list[dict[str, Any]]:
    if registry is None:
        return []
    rows: list[dict[str, Any]] = []
    for group_key, names in registry.grouped_features().items():
        present = [n for n in names if n in contributions]
        if not present:
            continue
        total = sum(contributions[n] for n in present)
        rows.append(
            {
                "key": group_key,
                "display_name": _group_display_name(group_key),
                "pp": float(total),
            }
        )
    rows.sort(key=lambda g: abs(g["pp"]), reverse=True)
    return rows


def build_explanations_batch(
    *,
    booster: xgb.Booster,
    feature_names: list[str],
    feature_set_name: str,
    model_artifact: str,
    features_list: list[dict[str, Any]],
    predictions_a: list[float],
    registry: FeatureRegistry | None = None,
    base_prob: float = BASE_PROB,
) -> list[dict[str, Any]]:
    """Build explanation payloads for many matches in a single vectorized pass.

    Used by the batch predictor so the booster is called twice per chunk
    (original + swap orientation) regardless of batch size.
    """
    if not features_list:
        return []

    X_orig = _prepare_matrix(features_list, feature_names)
    X_swap = _prepare_matrix((swap_ab_features(f) for f in features_list), feature_names)

    contribs_orig = _compute_contribs(booster, X_orig, feature_names)
    contribs_swap = _compute_contribs(booster, X_swap, feature_names)

    pair_idx = _pair_column_indices(feature_names)
    contribs_swap_remapped = _remap_swap_contribs(contribs_swap, pair_idx)

    contribs_avg = (contribs_orig + contribs_swap_remapped) / 2.0
    predictions_arr = np.asarray(predictions_a, dtype=np.float64)
    pp_matrix = _attribute_pp(contribs_avg, predictions_arr, base_prob)

    payloads: list[dict[str, Any]] = []
    for row_idx in range(pp_matrix.shape[0]):
        contributions = {
            feature_names[c]: float(pp_matrix[row_idx, c]) for c in range(len(feature_names))
        }
        payloads.append(
            {
                "model_artifact": model_artifact,
                "feature_set_name": feature_set_name,
                "base_prob": float(base_prob),
                "prediction_a": float(predictions_arr[row_idx]),
                "contributions": contributions,
                "paired_rows": _aggregate_paired_rows(contributions, feature_names),
                "groups": _aggregate_groups(contributions, registry),
            }
        )
    return payloads


def build_explanation(
    *,
    booster: xgb.Booster,
    feature_names: list[str],
    feature_set_name: str,
    model_artifact: str,
    features: dict[str, Any],
    prediction_a: float,
    registry: FeatureRegistry | None = None,
    base_prob: float = BASE_PROB,
) -> dict[str, Any]:
    """Single-row convenience wrapper around :func:`build_explanations_batch`."""
    payloads = build_explanations_batch(
        booster=booster,
        feature_names=feature_names,
        feature_set_name=feature_set_name,
        model_artifact=model_artifact,
        features_list=[features],
        predictions_a=[prediction_a],
        registry=registry,
        base_prob=base_prob,
    )
    return payloads[0]
