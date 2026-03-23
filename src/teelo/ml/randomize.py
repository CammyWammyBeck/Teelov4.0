"""Randomize player A/B orientation to prevent positional bias."""
from __future__ import annotations

import logging

import numpy as np

AB_SWAP_SEED = 42


def randomize_ab(
    X: np.ndarray,
    y: np.ndarray,
    columns: list[str],
    seed: int = AB_SWAP_SEED,
) -> None:
    """Randomly swap player A/B features and flip labels for ~50% of rows in-place."""
    rng = np.random.default_rng(seed)
    mask = rng.random(len(X)) < 0.5

    col_idx = {name: i for i, name in enumerate(columns)}

    a_cols = [c for c in columns if c.endswith("_a")]
    pairs = [
        (col_idx[c], col_idx[c[:-2] + "_b"])
        for c in a_cols
        if c[:-2] + "_b" in col_idx
    ]

    seen = {(a, b) for a, b in pairs}
    for c in columns:
        if c.startswith("h2h_a_"):
            partner = "h2h_b_" + c[len("h2h_a_"):]
            if partner in col_idx:
                pair = (col_idx[c], col_idx[partner])
                if pair not in seen:
                    seen.add(pair)
                    pairs.append(pair)

    swap_pair_indices = {idx for pair in pairs for idx in pair}
    # Features computed as (a - b) that need sign-flipping
    diff_col_indices = [col_idx[c] for c in columns if "diff" in c.lower() and col_idx[c] not in swap_pair_indices]
    # Features computed as a / total that need complementing (1 - x)
    complement_col_indices = [col_idx[c] for c in columns if c == "h2h_a_dominance" and col_idx[c] not in swap_pair_indices]

    handled = swap_pair_indices | set(diff_col_indices) | set(complement_col_indices)
    for c in columns:
        if col_idx[c] in handled:
            continue
        if c.endswith("_a") or c.startswith("h2h_a_"):
            logging.warning("Unmatched A-side column in A/B randomization: %s", c)
        elif c.endswith("_b") or c.startswith("h2h_b_"):
            logging.warning("Unmatched B-side column in A/B randomization: %s", c)

    for idx_a, idx_b in pairs:
        tmp = X[mask, idx_a].copy()
        X[mask, idx_a] = X[mask, idx_b]
        X[mask, idx_b] = tmp

    for idx in diff_col_indices:
        X[mask, idx] *= -1.0

    for idx in complement_col_indices:
        X[mask, idx] = 1.0 - X[mask, idx]

    y[mask] = 1 - y[mask]


def swap_ab_features(features: dict) -> dict:
    """Return a copy of *features* with player-A and player-B roles swapped.

    Handles three patterns:
    1. ``_a`` / ``_b`` suffix pairs → swap values
    2. ``h2h_a_`` / ``h2h_b_`` prefix pairs → swap values
    3. ``*diff*`` features (not already swapped) → negate
    4. ``h2h_a_dominance`` (not already swapped) → complement (1 - x)
    5. Everything else → pass through unchanged
    """
    swapped: dict = {}
    processed: set = set()

    # 1. Swap _a / _b suffix pairs
    for key, val in features.items():
        if key in processed:
            continue
        if key.endswith("_a"):
            partner = key[:-2] + "_b"
            if partner in features:
                swapped[key] = features[partner]
                swapped[partner] = val
                processed.add(key)
                processed.add(partner)

    # 2. Swap h2h_a_ / h2h_b_ prefix pairs
    for key, val in features.items():
        if key in processed:
            continue
        if key.startswith("h2h_a_"):
            partner = "h2h_b_" + key[len("h2h_a_"):]
            if partner in features:
                swapped[key] = features[partner]
                swapped[partner] = val
                processed.add(key)
                processed.add(partner)

    # 3. Negate *diff* features not already handled
    for key, val in features.items():
        if key in processed:
            continue
        if "diff" in key:
            swapped[key] = -val if val is not None else val
            processed.add(key)

    # 4. Complement h2h_a_dominance if not already swapped
    for key, val in features.items():
        if key in processed:
            continue
        if key == "h2h_a_dominance":
            swapped[key] = (1.0 - val) if val is not None else val
            processed.add(key)

    # 5. Pass through anything not yet handled
    for key, val in features.items():
        if key not in processed:
            swapped[key] = val

    return swapped
