"""Randomize player A/B orientation to prevent positional bias."""
from __future__ import annotations

import numpy as np
import pandas as pd

AB_SWAP_SEED = 42


def randomize_ab(
    X: pd.DataFrame,
    y: pd.Series,
    seed: int = AB_SWAP_SEED,
) -> tuple[pd.DataFrame, pd.Series]:
    """Randomly swap player A/B features and flip labels for ~50% of rows."""
    rng = np.random.default_rng(seed)
    mask = rng.random(len(X)) < 0.5

    a_cols = [c for c in X.columns if c.endswith("_a")]
    pairs = [(c, c[:-2] + "_b") for c in a_cols if c[:-2] + "_b" in X.columns]

    X_out = X.copy()
    y_out = y.copy()

    for col_a, col_b in pairs:
        X_out.loc[mask, col_a] = X.loc[mask, col_b].values
        X_out.loc[mask, col_b] = X.loc[mask, col_a].values

    y_out[mask] = 1 - y_out[mask]
    return X_out, y_out
