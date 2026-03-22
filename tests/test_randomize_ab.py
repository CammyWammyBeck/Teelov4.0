"""Standalone diagnostic script for randomize_ab coverage across baseline_v2 features.

Run with:
    python tests/test_randomize_ab.py
"""

from __future__ import annotations

import sys
import numpy as np

from teelo.features import build_registry
from teelo.ml.randomize import randomize_ab


# ── helpers ──────────────────────────────────────────────────────────────────

def classify_features(columns: list[str]) -> dict:
    """Replicate the classification logic inside randomize_ab without mutating data."""
    col_idx = {name: i for i, name in enumerate(columns)}

    a_cols = [c for c in columns if c.endswith("_a")]
    pairs: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()

    for c in a_cols:
        partner = c[:-2] + "_b"
        if partner in col_idx:
            pair = (col_idx[c], col_idx[partner])
            seen.add(pair)
            pairs.append(pair)

    for c in columns:
        if c.startswith("h2h_a_"):
            partner = "h2h_b_" + c[len("h2h_a_"):]
            if partner in col_idx:
                pair = (col_idx[c], col_idx[partner])
                if pair not in seen:
                    seen.add(pair)
                    pairs.append(pair)

    swap_pair_indices = {idx for p in pairs for idx in p}
    diff_col_indices = [
        col_idx[c] for c in columns
        if "diff" in c.lower() and col_idx[c] not in swap_pair_indices
    ]
    complement_col_indices = [
        col_idx[c] for c in columns
        if c == "h2h_a_dominance" and col_idx[c] not in swap_pair_indices
    ]
    handled = swap_pair_indices | set(diff_col_indices) | set(complement_col_indices)

    # Build per-column classification
    pair_lookup: dict[int, int] = {}
    for a_idx, b_idx in pairs:
        pair_lookup[a_idx] = b_idx
        pair_lookup[b_idx] = a_idx

    result = {
        "pairs": pairs,
        "swap_pair_indices": swap_pair_indices,
        "diff_col_indices": diff_col_indices,
        "complement_col_indices": complement_col_indices,
        "handled": handled,
        "pair_lookup": pair_lookup,
        "col_idx": col_idx,
    }
    return result


def handling_label(col_idx_val: int, info: dict) -> str:
    if col_idx_val in info["swap_pair_indices"]:
        return "swap_pair"
    if col_idx_val in set(info["diff_col_indices"]):
        return "diff_sign_flip"
    if col_idx_val in set(info["complement_col_indices"]):
        return "complement"
    return "UNHANDLED"


def partner_name(col_idx_val: int, columns: list[str], info: dict) -> str:
    partner_idx = info["pair_lookup"].get(col_idx_val)
    if partner_idx is None:
        return ""
    return columns[partner_idx]


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Build registry and get feature names
    registry = build_registry(preset="baseline_v2")
    columns = registry.all_feature_names()
    n_features = len(columns)
    print(f"baseline_v2 feature count: {n_features}")
    print()

    # 2. Build a synthetic dataset.
    #    Use 100 rows so the seed-42 mask gives us a mix of swapped/unswapped rows.
    #    Each row i gets value i * 1000 + feature_index so values are distinct.
    n_rows = 100
    X_base = np.zeros((n_rows, n_features), dtype=float)
    for row in range(n_rows):
        for col in range(n_features):
            X_base[row, col] = float(row * 1000 + col)

    X = X_base.copy()
    y = np.ones(n_rows, dtype=float)  # all "A wins"

    # 3. Find which rows the seed-42 mask will swap (before calling randomize_ab)
    rng_probe = np.random.default_rng(42)
    swap_mask = rng_probe.random(n_rows) < 0.5

    # Pick one swapped row and one unswapped row for value display
    swapped_rows = np.where(swap_mask)[0]
    unswapped_rows = np.where(~swap_mask)[0]
    if len(swapped_rows) == 0 or len(unswapped_rows) == 0:
        print("ERROR: seed=42 produced a degenerate mask. Aborting.")
        sys.exit(1)
    swapped_row = int(swapped_rows[0])
    unswapped_row = int(unswapped_rows[0])

    # Save original values for comparison
    orig_swapped = X_base[swapped_row].copy()
    orig_unswapped = X_base[unswapped_row].copy()

    # 4. Run randomize_ab
    randomize_ab(X, y, columns, seed=42)

    post_swapped = X[swapped_row]
    post_unswapped = X[unswapped_row]

    # 5. Classify features
    info = classify_features(columns)

    # ── per-feature table ──────────────────────────────────────────────────────
    groups: dict[str, list[dict]] = {
        "swap_pair": [],
        "diff_sign_flip": [],
        "complement": [],
        "UNHANDLED": [],
    }

    col_idx_map = info["col_idx"]

    for col_name in columns:
        idx = col_idx_map[col_name]
        label = handling_label(idx, info)
        p_name = partner_name(idx, columns, info)

        orig_val = orig_swapped[idx]
        post_val = post_swapped[idx]

        if label == "swap_pair":
            p_idx = info["pair_lookup"][idx]
            expected = orig_swapped[p_idx]
            correct = np.isclose(post_val, expected)
        elif label == "diff_sign_flip":
            expected = -orig_val
            correct = np.isclose(post_val, expected)
        elif label == "complement":
            expected = 1.0 - orig_val
            correct = np.isclose(post_val, expected)
        else:
            expected = orig_val  # should be unchanged
            correct = True  # no expectation

        # Verify unswapped row is untouched
        unswapped_ok = np.isclose(post_unswapped[idx], orig_unswapped[idx])

        groups[label].append({
            "name": col_name,
            "partner": p_name,
            "orig": orig_val,
            "post": post_val,
            "expected": expected,
            "correct": correct,
            "unswapped_ok": unswapped_ok,
        })

    # ── print grouped tables ───────────────────────────────────────────────────
    for label in ["swap_pair", "diff_sign_flip", "complement", "UNHANDLED"]:
        rows = groups[label]
        if not rows:
            print(f"[{label}] — none\n")
            continue

        print(f"{'='*100}")
        print(f"  {label}  ({len(rows)} features)")
        print(f"{'='*100}")

        if label == "swap_pair":
            hdr = f"{'Feature':<45} {'Partner':<45} {'Orig':>10} {'Post':>10} {'OK?':>5} {'Unswap?':>8}"
            print(hdr)
            print("-" * len(hdr))
            for r in rows:
                ok = "YES" if r["correct"] else "NO !"
                uok = "YES" if r["unswapped_ok"] else "NO !"
                print(f"{r['name']:<45} {r['partner']:<45} {r['orig']:>10.1f} {r['post']:>10.1f} {ok:>5} {uok:>8}")
        elif label in ("diff_sign_flip", "complement"):
            hdr = f"{'Feature':<55} {'Orig':>10} {'Expected':>10} {'Post':>10} {'OK?':>5} {'Unswap?':>8}"
            print(hdr)
            print("-" * len(hdr))
            for r in rows:
                ok = "YES" if r["correct"] else "NO !"
                uok = "YES" if r["unswapped_ok"] else "NO !"
                print(f"{r['name']:<55} {r['orig']:>10.1f} {r['expected']:>10.1f} {r['post']:>10.1f} {ok:>5} {uok:>8}")
        else:  # UNHANDLED
            hdr = f"{'Feature':<55} {'Orig':>10} {'Post':>10} {'Changed?':>9} {'Unswap?':>8}"
            print(hdr)
            print("-" * len(hdr))
            for r in rows:
                changed = "YES" if not np.isclose(r["orig"], r["post"]) else "no"
                uok = "YES" if r["unswapped_ok"] else "NO !"
                print(f"{r['name']:<55} {r['orig']:>10.1f} {r['post']:>10.1f} {changed:>9} {uok:>8}")
        print()

    # ── flag problems ──────────────────────────────────────────────────────────
    print(f"{'='*100}")
    print("  PROBLEM FLAGS")
    print(f"{'='*100}")

    problems: list[str] = []

    # A/B-named features that are UNHANDLED
    for r in groups["UNHANDLED"]:
        name = r["name"]
        if name.endswith("_a") or name.endswith("_b") or name.startswith("h2h_a_") or name.startswith("h2h_b_"):
            problems.append(f"  UNHANDLED A/B feature: {name}")

    # swap_pair entries whose partner does not exist
    for r in groups["swap_pair"]:
        if not r["partner"]:
            problems.append(f"  swap_pair but no partner: {r['name']}")

    # Diff features that also appear in swap_pair_indices (double-handled)
    diff_idx_set = set(info["diff_col_indices"])
    for col_name in columns:
        idx = col_idx_map[col_name]
        if idx in diff_idx_set and idx in info["swap_pair_indices"]:
            problems.append(f"  double-handled (diff + swap_pair): {col_name}")

    # Incorrect transformations
    for label in ("swap_pair", "diff_sign_flip", "complement"):
        for r in groups[label]:
            if not r["correct"]:
                problems.append(f"  INCORRECT {label}: {r['name']} orig={r['orig']:.1f} post={r['post']:.1f} expected={r['expected']:.1f}")

    # Unswapped rows modified when they shouldn't be
    for label in groups:
        for r in groups[label]:
            if not r["unswapped_ok"]:
                problems.append(f"  UNSWAPPED ROW MODIFIED: {r['name']}")

    if problems:
        for p in problems:
            print(p)
    else:
        print("  None — all features handled correctly.")

    print()

    # ── summary counts ─────────────────────────────────────────────────────────
    print(f"{'='*100}")
    print("  SUMMARY")
    print(f"{'='*100}")
    print(f"  Total features : {n_features}")
    print(f"  swap_pair      : {len(groups['swap_pair'])}")
    print(f"  diff_sign_flip : {len(groups['diff_sign_flip'])}")
    print(f"  complement     : {len(groups['complement'])}")
    print(f"  UNHANDLED      : {len(groups['UNHANDLED'])}")
    total_accounted = len(groups["swap_pair"]) + len(groups["diff_sign_flip"]) + len(groups["complement"]) + len(groups["UNHANDLED"])
    print(f"  Accounted      : {total_accounted}")
    swapped_count = int(swap_mask.sum())
    print(f"  Rows swapped   : {swapped_count}/{n_rows} (seed=42)")
    print()


if __name__ == "__main__":
    main()
