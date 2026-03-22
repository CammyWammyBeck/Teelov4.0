"""Randomize player A/B orientation to prevent positional bias."""
from __future__ import annotations

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

    # Features computed as (a - b) that need sign-flipping
    diff_col_indices = [col_idx[c] for c in columns if "diff" in c.lower()]
    # Features computed as a / total that need complementing (1 - x)
    complement_col_indices = [col_idx[c] for c in columns if c == "h2h_a_dominance"]

    for idx_a, idx_b in pairs:
        tmp = X[mask, idx_a].copy()
        X[mask, idx_a] = X[mask, idx_b]
        X[mask, idx_b] = tmp

    for idx in diff_col_indices:
        X[mask, idx] *= -1.0

    for idx in complement_col_indices:
        X[mask, idx] = 1.0 - X[mask, idx]

    y[mask] = 1 - y[mask]
