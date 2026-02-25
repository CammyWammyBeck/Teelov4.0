"""
Optuna-based ELO parameter optimization.

This optimizer supports:
- Explicit chronological train/validation/test evaluation
- Optional coarse-history optimization for faster trial loops
- Optional staged search to handle correlated parameter spaces

Usage examples:
    # Baseline single-pass tuning
    python scripts/optimise_elo.py --n-trials 200

    # Staged tuning on coarse slice, then final selection on full history
    python scripts/optimise_elo.py \
      --staged \
      --stage1-trials 220 \
      --stage2-trials 260 \
      --stage3-trials 180 \
      --coarse-start-date 2018-01-01 \
      --split-mode recent_years_ratio \
      --eval-years 5 \
      --val-ratio 0.2 \
      --test-ratio 0.2

    # Tune + guarded activation + full inline state rebuild
    python scripts/optimise_elo.py \
      --staged \
      --stage1-trials 220 \
      --stage2-trials 260 \
      --stage3-trials 180 \
      --coarse-start-date 2018-01-01 \
      --activate-best \
      --min-improvement 0.002 \
      --rebuild-live-state
"""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta

import optuna

from teelo.db.session import get_session
from teelo.elo.pipeline import EloParams, EloPipeline, load_matches_for_elo
from teelo.elo.params_store import get_active_elo_params, persist_elo_params

MEN_LEVELS = ["F", "C", "A", "M", "G"]
WOMEN_LEVELS = ["WF", "WC", "WA", "WM", "WG"]
ALL_LEVELS = MEN_LEVELS + WOMEN_LEVELS

SHARED_PARAM_NAMES = [
    "margin_base",
    "margin_scale",
    "decay_rate",
    "decay_start_days",
    "new_threshold",
    "new_boost",
    "returning_days",
    "returning_boost",
    "start_elo_men",
    "start_elo_women",
]

FINE_TUNE_PARAM_NAMES = [
    "K_G",
    "S_G",
    "K_WG",
    "S_WG",
    "K_A",
    "S_A",
    "K_WA",
    "S_WA",
    "margin_scale",
    "decay_rate",
    "new_boost",
    "returning_boost",
]


def compute_log_loss(probs: list[float]) -> float:
    """Compute binary log-loss from winner probabilities."""
    if not probs:
        return float("inf")

    total = 0.0
    for p in probs:
        p = max(1e-7, min(1.0 - 1e-7, p))
        total += math.log(p)
    return -total / len(probs)


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def filter_matches_by_start_date(matches: list[dict], start: date | None) -> list[dict]:
    if start is None:
        return matches
    filtered: list[dict] = []
    for m in matches:
        match_date = m.get("match_date")
        if match_date is not None and match_date >= start:
            filtered.append(m)
    return filtered


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def param_bounds(name: str) -> tuple[float, float]:
    if name.startswith("K_"):
        if name in {"K_F", "K_WF"}:
            return (80.0, 300.0)
        if name in {"K_C", "K_WC"}:
            return (60.0, 250.0)
        return (50.0, 200.0)
    if name.startswith("S_"):
        if name in {"S_F", "S_WF"}:
            return (500.0, 2500.0)
        if name in {"S_C", "S_WC", "S_G", "S_WG"}:
            return (600.0, 2800.0)
        return (800.0, 3000.0)
    if name == "margin_base":
        return (0.5, 1.2)
    if name == "margin_scale":
        return (0.05, 0.8)
    if name == "decay_rate":
        return (0.001, 0.2)
    if name == "decay_start_days":
        return (20.0, 180.0)
    if name == "new_threshold":
        return (5.0, 80.0)
    if name == "new_boost":
        return (1.0, 2.5)
    if name == "returning_days":
        return (60.0, 365.0)
    if name == "returning_boost":
        return (1.0, 2.0)
    if name in {"start_elo_men", "start_elo_women"}:
        return (1200.0, 1800.0)
    raise KeyError(f"Unknown parameter bounds for: {name}")


def params_to_dict(params: EloParams) -> dict[str, float | int]:
    return asdict(params)


def enforce_mens_grandslam_s_constraint(values: dict[str, float | int]) -> dict[str, float | int]:
    """
    Enforce men's Grand Slam spread constraint:
    S_G must be <= S_F, S_C, S_A, S_M.
    """
    out = dict(values)
    max_allowed = min(float(out["S_F"]), float(out["S_C"]), float(out["S_A"]), float(out["S_M"]))
    out["S_G"] = min(float(out["S_G"]), max_allowed)
    return out


def round_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return round(value / step) * step


def simplify_params(values: dict[str, float | int]) -> dict[str, float | int]:
    """Quantize parameters to simpler, more stable values."""
    out = dict(values)
    for key, value in list(out.items()):
        if key.startswith("K_"):
            out[key] = float(round_to_step(float(value), 5.0))
        elif key.startswith("S_"):
            out[key] = float(round_to_step(float(value), 25.0))
        elif key in {"margin_base", "margin_scale"}:
            out[key] = float(round_to_step(float(value), 0.01))
        elif key == "decay_rate":
            out[key] = float(round_to_step(float(value), 0.005))
        elif key in {"decay_start_days", "returning_days"}:
            out[key] = float(round_to_step(float(value), 5.0))
        elif key == "new_threshold":
            out[key] = int(round_to_step(float(value), 5.0))
        elif key in {"new_boost", "returning_boost"}:
            out[key] = float(round_to_step(float(value), 0.05))
        elif key in {"start_elo_men", "start_elo_women"}:
            out[key] = float(round_to_step(float(value), 100.0))
    return out


def dict_to_params(values: dict[str, float | int]) -> EloParams:
    materialized = simplify_params(values)
    materialized = enforce_mens_grandslam_s_constraint(materialized)
    materialized["new_threshold"] = int(materialized["new_threshold"])
    return EloParams(**materialized)


def make_sampler(args: argparse.Namespace) -> optuna.samplers.TPESampler:
    multivariate_tpe = not args.independent_tpe
    group_tpe = multivariate_tpe and (not args.no_group_tpe)
    return optuna.samplers.TPESampler(
        seed=args.seed,
        n_startup_trials=args.n_startup_trials,
        n_ei_candidates=args.n_ei_candidates,
        multivariate=multivariate_tpe,
        group=group_tpe,
    )


def build_split(
    matches: list[dict],
    split_mode: str,
    test_months: int,
    eval_years: int,
    val_ratio: float,
    test_ratio: float,
) -> dict[str, int | date]:
    if len(matches) < 10:
        raise ValueError("Need at least 10 matches after filtering for a stable split.")

    if val_ratio <= 0 or test_ratio <= 0 or (val_ratio + test_ratio) >= 0.9:
        raise ValueError("val_ratio and test_ratio must both be > 0, and val_ratio + test_ratio must be < 0.9")

    if split_mode == "match_date":
        dated_matches = [m for m in matches if m["match_date"] is not None]
        if not dated_matches:
            raise ValueError("No matches with dates found for split-mode=match_date")

        max_date = max(m["match_date"] for m in dated_matches)
        eval_start_date = max_date - timedelta(days=test_months * 30)

        eval_start_idx = len(matches)
        for i, m in enumerate(matches):
            if m["match_date"] is not None and m["match_date"] >= eval_start_date:
                eval_start_idx = i
                break

        if eval_start_idx >= len(matches):
            raise ValueError("No matches found in match_date evaluation window")

    elif split_mode == "recent_years_ratio":
        dated_matches = [m for m in matches if m["match_date"] is not None]
        if not dated_matches:
            raise ValueError("No matches with dates found for split-mode=recent_years_ratio")

        max_date = max(m["match_date"] for m in dated_matches)
        eval_start_date = max_date - timedelta(days=eval_years * 365)

        eval_start_idx = len(matches)
        for i, m in enumerate(matches):
            if m["match_date"] is not None and m["match_date"] >= eval_start_date:
                eval_start_idx = i
                break

        if eval_start_idx >= len(matches):
            raise ValueError("No matches found in recent evaluation window")

    elif split_mode == "temporal_order":
        eval_start_idx = 0
        eval_start_date = None
    else:
        raise ValueError(f"Unknown split mode: {split_mode}")

    n_eval = len(matches) - eval_start_idx
    if n_eval < 100:
        raise ValueError(f"Evaluation segment too small ({n_eval} matches)")

    train_end = eval_start_idx + int(n_eval * (1.0 - (val_ratio + test_ratio)))
    val_end = eval_start_idx + int(n_eval * (1.0 - test_ratio))

    min_train = eval_start_idx + 20
    min_val = min_train + 20
    train_end = max(min_train, min(train_end, len(matches) - 40))
    val_end = max(min_val, min(val_end, len(matches) - 20))

    if not (eval_start_idx < train_end < val_end < len(matches)):
        raise ValueError("Could not build valid train/val/test split with current ratios")

    return {
        "warmup_end": eval_start_idx,
        "train_end": train_end,
        "val_end": val_end,
        "n_total": len(matches),
        "n_eval": n_eval,
        "eval_start_date": eval_start_date,
    }


def evaluate_params(params: EloParams, matches: list[dict], split: dict[str, int | date]) -> dict[str, float]:
    constrained_params = dict_to_params(params_to_dict(params))
    pipeline = EloPipeline(constrained_params)
    probs = pipeline.run_fast(matches)

    train_end = int(split["train_end"])
    val_end = int(split["val_end"])

    train_probs = probs[int(split["warmup_end"]):train_end]
    val_probs = probs[train_end:val_end]
    test_probs = probs[val_end:]

    return {
        "train": compute_log_loss(train_probs),
        "val": compute_log_loss(val_probs),
        "test": compute_log_loss(test_probs),
    }


def create_full_params_from_trial(trial: optuna.Trial) -> EloParams:
    return EloParams(
        K_F=trial.suggest_float("K_F", 80, 300),
        K_C=trial.suggest_float("K_C", 60, 250),
        K_A=trial.suggest_float("K_A", 50, 200),
        K_M=trial.suggest_float("K_M", 50, 200),
        K_G=trial.suggest_float("K_G", 50, 200),
        S_F=trial.suggest_float("S_F", 500, 2500),
        S_C=trial.suggest_float("S_C", 600, 2800),
        S_A=trial.suggest_float("S_A", 800, 3000),
        S_M=trial.suggest_float("S_M", 800, 3000),
        S_G=trial.suggest_float("S_G", 600, 2800),
        K_WF=trial.suggest_float("K_WF", 80, 300),
        K_WC=trial.suggest_float("K_WC", 60, 250),
        K_WA=trial.suggest_float("K_WA", 50, 200),
        K_WM=trial.suggest_float("K_WM", 50, 200),
        K_WG=trial.suggest_float("K_WG", 50, 200),
        S_WF=trial.suggest_float("S_WF", 500, 2500),
        S_WC=trial.suggest_float("S_WC", 600, 2800),
        S_WA=trial.suggest_float("S_WA", 800, 3000),
        S_WM=trial.suggest_float("S_WM", 800, 3000),
        S_WG=trial.suggest_float("S_WG", 600, 2800),
        margin_base=trial.suggest_float("margin_base", 0.5, 1.2),
        margin_scale=trial.suggest_float("margin_scale", 0.05, 0.8),
        decay_rate=trial.suggest_float("decay_rate", 0.001, 0.2),
        decay_start_days=trial.suggest_float("decay_start_days", 20, 180),
        new_threshold=trial.suggest_int("new_threshold", 5, 80),
        new_boost=trial.suggest_float("new_boost", 1.0, 2.5),
        returning_days=trial.suggest_float("returning_days", 60, 365),
        returning_boost=trial.suggest_float("returning_boost", 1.0, 2.0),
        start_elo_men=trial.suggest_int("start_elo_men", 1200, 1800, step=100),
        start_elo_women=trial.suggest_int("start_elo_women", 1200, 1800, step=100),
    )


def stage1_params_from_values(values: dict[str, float | int], base: EloParams) -> EloParams:
    base_vals = params_to_dict(base)
    k_scale_men_non_g = float(values["k_scale_men_non_g"])
    s_scale_men_non_g = float(values["s_scale_men_non_g"])
    k_scale_men_g = float(values["k_scale_men_g"])
    s_scale_men_g = float(values["s_scale_men_g"])
    k_scale_women_non_g = float(values["k_scale_women_non_g"])
    s_scale_women_non_g = float(values["s_scale_women_non_g"])
    k_scale_women_g = float(values["k_scale_women_g"])
    s_scale_women_g = float(values["s_scale_women_g"])

    out = dict(base_vals)

    for code in ["F", "C", "A", "M"]:
        out[f"K_{code}"] = clamp(float(base_vals[f"K_{code}"]) * k_scale_men_non_g, *param_bounds(f"K_{code}"))
        out[f"S_{code}"] = clamp(float(base_vals[f"S_{code}"]) * s_scale_men_non_g, *param_bounds(f"S_{code}"))

    out["K_G"] = clamp(float(base_vals["K_G"]) * k_scale_men_g, *param_bounds("K_G"))
    out["S_G"] = clamp(float(base_vals["S_G"]) * s_scale_men_g, *param_bounds("S_G"))

    for code in ["WF", "WC", "WA", "WM"]:
        out[f"K_{code}"] = clamp(float(base_vals[f"K_{code}"]) * k_scale_women_non_g, *param_bounds(f"K_{code}"))
        out[f"S_{code}"] = clamp(float(base_vals[f"S_{code}"]) * s_scale_women_non_g, *param_bounds(f"S_{code}"))

    out["K_WG"] = clamp(float(base_vals["K_WG"]) * k_scale_women_g, *param_bounds("K_WG"))
    out["S_WG"] = clamp(float(base_vals["S_WG"]) * s_scale_women_g, *param_bounds("S_WG"))

    out["margin_base"] = float(values["margin_base"])
    out["margin_scale"] = float(values["margin_scale"])
    out["decay_rate"] = float(values["decay_rate"])
    out["decay_start_days"] = float(values["decay_start_days"])
    out["new_threshold"] = int(values["new_threshold"])
    out["new_boost"] = float(values["new_boost"])
    out["returning_days"] = float(values["returning_days"])
    out["returning_boost"] = float(values["returning_boost"])
    out["start_elo_men"] = float(values["start_elo_men"])
    out["start_elo_women"] = float(values["start_elo_women"])

    return dict_to_params(out)


def create_stage1_params(trial: optuna.Trial, base: EloParams) -> EloParams:
    values: dict[str, float | int] = {
        "k_scale_men_non_g": trial.suggest_float("k_scale_men_non_g", 0.75, 1.25),
        "s_scale_men_non_g": trial.suggest_float("s_scale_men_non_g", 0.75, 1.25),
        "k_scale_men_g": trial.suggest_float("k_scale_men_g", 0.75, 1.25),
        "s_scale_men_g": trial.suggest_float("s_scale_men_g", 0.75, 1.25),
        "k_scale_women_non_g": trial.suggest_float("k_scale_women_non_g", 0.75, 1.25),
        "s_scale_women_non_g": trial.suggest_float("s_scale_women_non_g", 0.75, 1.25),
        "k_scale_women_g": trial.suggest_float("k_scale_women_g", 0.75, 1.25),
        "s_scale_women_g": trial.suggest_float("s_scale_women_g", 0.75, 1.25),
        "margin_base": trial.suggest_float("margin_base", 0.5, 1.2),
        "margin_scale": trial.suggest_float("margin_scale", 0.05, 0.8),
        "decay_rate": trial.suggest_float("decay_rate", 0.001, 0.2),
        "decay_start_days": trial.suggest_float("decay_start_days", 20, 180),
        "new_threshold": trial.suggest_int("new_threshold", 5, 80),
        "new_boost": trial.suggest_float("new_boost", 1.0, 2.5),
        "returning_days": trial.suggest_float("returning_days", 60, 365),
        "returning_boost": trial.suggest_float("returning_boost", 1.0, 2.0),
        "start_elo_men": trial.suggest_int("start_elo_men", 1200, 1800, step=100),
        "start_elo_women": trial.suggest_int("start_elo_women", 1200, 1800, step=100),
    }
    return stage1_params_from_values(values, base)


def stage2_params_from_values(values: dict[str, float | int], anchor: EloParams) -> EloParams:
    anchor_vals = params_to_dict(anchor)
    out = dict(anchor_vals)

    for code in ALL_LEVELS:
        k_name = f"K_{code}"
        s_name = f"S_{code}"
        out[k_name] = clamp(
            float(anchor_vals[k_name]) * float(values[f"{k_name}_delta"]),
            *param_bounds(k_name),
        )
        out[s_name] = clamp(
            float(anchor_vals[s_name]) * float(values[f"{s_name}_delta"]),
            *param_bounds(s_name),
        )

    for name in SHARED_PARAM_NAMES:
        low, high = param_bounds(name)
        value = float(values[name])
        if name == "new_threshold":
            out[name] = int(clamp(round(value), low, high))
        else:
            out[name] = clamp(value, low, high)

    return dict_to_params(out)


def create_stage2_params(trial: optuna.Trial, anchor: EloParams) -> EloParams:
    anchor_vals = params_to_dict(anchor)
    values: dict[str, float | int] = {}
    for code in ALL_LEVELS:
        k_name = f"K_{code}"
        s_name = f"S_{code}"
        values[f"{k_name}_delta"] = trial.suggest_float(f"{k_name}_delta", 0.82, 1.18)
        values[f"{s_name}_delta"] = trial.suggest_float(f"{s_name}_delta", 0.82, 1.18)

    for name in SHARED_PARAM_NAMES:
        low, high = param_bounds(name)
        center = float(anchor_vals[name])
        if name == "new_threshold":
            lo = int(max(low, round(center - 12)))
            hi = int(min(high, round(center + 12)))
            if lo == hi:
                lo = max(int(low), lo - 1)
                hi = min(int(high), hi + 1)
            values[name] = trial.suggest_int(name, lo, hi)
        elif name in {"start_elo_men", "start_elo_women"}:
            lo = int(max(low, round_to_step(center - 300, 100)))
            hi = int(min(high, round_to_step(center + 300, 100)))
            if lo >= hi:
                lo, hi = int(low), int(high)
            values[name] = trial.suggest_int(name, lo, hi, step=100)
        else:
            span = high - low
            lo = max(low, center - (0.18 * span))
            hi = min(high, center + (0.18 * span))
            if lo >= hi:
                lo, hi = low, high
            values[name] = trial.suggest_float(name, lo, hi)
    return stage2_params_from_values(values, anchor)


def stage3_params_from_values(values: dict[str, float | int], anchor: EloParams) -> EloParams:
    anchor_vals = params_to_dict(anchor)
    out = dict(anchor_vals)

    for name in FINE_TUNE_PARAM_NAMES:
        low, high = param_bounds(name)
        value = float(values[name])
        if name == "new_threshold":
            out[name] = int(clamp(round(value), low, high))
        else:
            out[name] = clamp(value, low, high)

    return dict_to_params(out)


def create_stage3_params(trial: optuna.Trial, anchor: EloParams) -> EloParams:
    anchor_vals = params_to_dict(anchor)
    values: dict[str, float | int] = {}
    for name in FINE_TUNE_PARAM_NAMES:
        low, high = param_bounds(name)
        center = float(anchor_vals[name])
        if name == "new_threshold":
            values[name] = trial.suggest_int(
                name,
                int(max(low, round(center - 6))),
                int(min(high, round(center + 6))),
            )
            continue
        span = high - low
        lo = max(low, center - (0.10 * span))
        hi = min(high, center + (0.10 * span))
        if lo >= hi:
            lo, hi = low, high
        values[name] = trial.suggest_float(name, lo, hi)
    return stage3_params_from_values(values, anchor)


def run_optimization(
    *,
    stage_name: str,
    trials: int,
    args: argparse.Namespace,
    objective_fn,
) -> optuna.study.Study:
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    sampler = make_sampler(args)

    print(f"[{stage_name}] sampler seed={args.seed}, startup={args.n_startup_trials}, ei={args.n_ei_candidates}")
    print(f"[{stage_name}] running {trials} trials...")

    study = optuna.create_study(direction="minimize", sampler=sampler)
    study.optimize(objective_fn, n_trials=trials, show_progress_bar=True)

    print(f"[{stage_name}] best val log-loss: {study.best_trial.value:.6f}")
    return study


def rebuild_live_elo_state(params: EloParams, params_version: str) -> tuple[int, int, int]:
    """
    Rebuild all ELO artifacts from completed matches in temporal order.

    Delegates to EloUpdater.rebuild() which processes all terminal matches in a
    single in-memory pass and writes results in two bulk DB operations.

    Returns (processed_count, 0, n_player_states) for backward-compatible callers.
    The middle value (formerly "flagged_recompute") is always 0 in the new system —
    backfills are handled automatically rather than being flagged for later.
    """
    from teelo.db.models import PlayerEloState
    from teelo.elo.updater import EloUpdater

    with get_session() as session:
        updater = EloUpdater(params=params, params_version=params_version)
        result = updater.rebuild(session)
        session.commit()
        n_states = session.query(PlayerEloState).count()
        return result.processed, 0, n_states


def print_split_info(label: str, split: dict[str, int | date], matches: list[dict]) -> None:
    warmup_end = int(split["warmup_end"])
    train_end = int(split["train_end"])
    val_end = int(split["val_end"])

    print(f"{label} matches: {len(matches)}")
    if split.get("eval_start_date"):
        print(f"{label} eval window start date: {split['eval_start_date']}")
    print(f"{label} warmup: {warmup_end}")
    print(f"{label} train: {train_end - warmup_end}")
    print(f"{label} val: {val_end - train_end}")
    print(f"{label} test: {len(matches) - val_end}")


def select_best_candidate_on_full(
    candidates: list[EloParams],
    full_matches: list[dict],
    full_split: dict[str, int | date],
) -> tuple[EloParams, dict[str, float]]:
    best_metrics: dict[str, float] | None = None
    best_params: EloParams | None = None

    for idx, params in enumerate(candidates, start=1):
        metrics = evaluate_params(params, full_matches, full_split)
        print(
            f"[full-eval candidate {idx}] "
            f"train={metrics['train']:.6f} val={metrics['val']:.6f} test={metrics['test']:.6f}"
        )
        if best_metrics is None or metrics["val"] < best_metrics["val"]:
            best_metrics = metrics
            best_params = params

    if best_params is None or best_metrics is None:
        raise RuntimeError("No candidates available for full-history selection")

    return best_params, best_metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Optimize ELO parameters with Optuna")
    parser.add_argument("--n-trials", type=int, default=200, help="Number of Optuna trials for single-pass mode")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for Optuna sampler")
    parser.add_argument("--n-startup-trials", type=int, default=120, help="Random startup trials before TPE")
    parser.add_argument("--n-ei-candidates", type=int, default=96, help="TPE candidate points per trial")
    parser.add_argument("--independent-tpe", action="store_true", help="Use independent (non-multivariate) TPE")
    parser.add_argument("--no-group-tpe", action="store_true", help="Disable grouped dimensions for multivariate TPE")

    parser.add_argument("--staged", action="store_true", help="Use staged optimization (recommended for large datasets)")
    parser.add_argument("--stage1-trials", type=int, default=220, help="Trials for stage 1 (coarse structural search)")
    parser.add_argument("--stage2-trials", type=int, default=260, help="Trials for stage 2 (level deltas)")
    parser.add_argument("--stage3-trials", type=int, default=180, help="Trials for stage 3 (fine tune sensitive params)")
    parser.add_argument("--full-eval-top-k", type=int, default=5, help="Top staged candidates evaluated on full history")

    parser.add_argument("--coarse-start-date", type=parse_date, default=None, help="Optional YYYY-MM-DD start for faster staged trials")

    parser.add_argument(
        "--split-mode",
        choices=["match_date", "temporal_order", "recent_years_ratio"],
        default="recent_years_ratio",
        help="How to build chronological splits",
    )
    parser.add_argument("--test-months", type=int, default=3, help="Months for eval window when split-mode=match_date")
    parser.add_argument("--eval-years", type=int, default=5, help="Years for eval window when split-mode=recent_years_ratio")
    parser.add_argument("--val-ratio", type=float, default=0.2, help="Validation tail ratio within eval window")
    parser.add_argument("--test-ratio", type=float, default=0.2, help="Test tail ratio within eval window")

    parser.add_argument("--rebuild-live-state", action="store_true", help="Rebuild inline ELO snapshots/player state")
    parser.add_argument("--activate-best", action="store_true", help="Persist and activate best params")
    parser.add_argument(
        "--min-improvement",
        type=float,
        default=0.0,
        help="Minimum TEST log-loss improvement vs active params required for activation",
    )

    args = parser.parse_args()

    if args.n_startup_trials < 0:
        print("ERROR: --n-startup-trials must be >= 0")
        sys.exit(2)
    if args.n_ei_candidates < 2:
        print("ERROR: --n-ei-candidates must be >= 2")
        sys.exit(2)
    if args.eval_years < 1:
        print("ERROR: --eval-years must be >= 1")
        sys.exit(2)
    if args.test_months < 1:
        print("ERROR: --test-months must be >= 1")
        sys.exit(2)
    if args.full_eval_top_k < 1:
        print("ERROR: --full-eval-top-k must be >= 1")
        sys.exit(2)

    print("Loading matches from database...")
    with get_session() as session:
        full_matches = load_matches_for_elo(session)
        active_params, active_params_version = get_active_elo_params(session)

    if not full_matches:
        print("ERROR: No completed matches found in database")
        sys.exit(1)

    print(f"Loaded {len(full_matches)} completed matches")

    coarse_matches = filter_matches_by_start_date(full_matches, args.coarse_start_date)
    if args.coarse_start_date:
        print(f"Coarse start date: {args.coarse_start_date} -> {len(coarse_matches)} matches")
        if len(coarse_matches) < 5000:
            print("WARNING: Coarse slice is very small; optimization may be noisy")

    try:
        full_split = build_split(
            full_matches,
            split_mode=args.split_mode,
            test_months=args.test_months,
            eval_years=args.eval_years,
            val_ratio=args.val_ratio,
            test_ratio=args.test_ratio,
        )
        coarse_split = build_split(
            coarse_matches,
            split_mode=args.split_mode,
            test_months=args.test_months,
            eval_years=args.eval_years,
            val_ratio=args.val_ratio,
            test_ratio=args.test_ratio,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}")
        sys.exit(2)

    print_split_info("[full]", full_split, full_matches)
    print_split_info("[coarse]", coarse_split, coarse_matches)

    default_metrics_full = evaluate_params(EloParams(), full_matches, full_split)
    active_metrics_full = evaluate_params(active_params, full_matches, full_split)

    print()
    print("Baselines on full history:")
    print(
        f"  default ({active_params_version=} not used): "
        f"train={default_metrics_full['train']:.6f} "
        f"val={default_metrics_full['val']:.6f} "
        f"test={default_metrics_full['test']:.6f}"
    )
    print(
        f"  active ({active_params_version}): "
        f"train={active_metrics_full['train']:.6f} "
        f"val={active_metrics_full['val']:.6f} "
        f"test={active_metrics_full['test']:.6f}"
    )
    print(f"  random baseline: {-math.log(0.5):.6f}")

    best_params: EloParams
    best_metrics_full: dict[str, float]

    if not args.staged:
        print()
        print("Running single-pass optimization on full parameter space...")

        def objective_single(trial: optuna.Trial) -> float:
            params = create_full_params_from_trial(trial)
            metrics = evaluate_params(params, coarse_matches, coarse_split)
            trial.set_user_attr("train_log_loss", metrics["train"])
            trial.set_user_attr("test_log_loss", metrics["test"])
            return metrics["val"]

        study = run_optimization(stage_name="single", trials=args.n_trials, args=args, objective_fn=objective_single)
        best_params = dict_to_params(study.best_trial.params)
        best_metrics_full = evaluate_params(best_params, full_matches, full_split)

    else:
        print()
        print("Running staged optimization...")

        def stage1_objective(trial: optuna.Trial) -> float:
            params = create_stage1_params(trial, active_params)
            metrics = evaluate_params(params, coarse_matches, coarse_split)
            trial.set_user_attr("train_log_loss", metrics["train"])
            trial.set_user_attr("test_log_loss", metrics["test"])
            return metrics["val"]

        stage1 = run_optimization(
            stage_name="stage1",
            trials=args.stage1_trials,
            args=args,
            objective_fn=stage1_objective,
        )
        stage1_best = stage1_params_from_values(stage1.best_trial.params, active_params)

        def stage2_objective(trial: optuna.Trial) -> float:
            params = create_stage2_params(trial, stage1_best)
            metrics = evaluate_params(params, coarse_matches, coarse_split)
            trial.set_user_attr("train_log_loss", metrics["train"])
            trial.set_user_attr("test_log_loss", metrics["test"])
            return metrics["val"]

        stage2 = run_optimization(
            stage_name="stage2",
            trials=args.stage2_trials,
            args=args,
            objective_fn=stage2_objective,
        )
        stage2_best = stage2_params_from_values(stage2.best_trial.params, stage1_best)

        def stage3_objective(trial: optuna.Trial) -> float:
            params = create_stage3_params(trial, stage2_best)
            metrics = evaluate_params(params, coarse_matches, coarse_split)
            trial.set_user_attr("train_log_loss", metrics["train"])
            trial.set_user_attr("test_log_loss", metrics["test"])
            return metrics["val"]

        stage3 = run_optimization(
            stage_name="stage3",
            trials=args.stage3_trials,
            args=args,
            objective_fn=stage3_objective,
        )
        stage3_best = stage3_params_from_values(stage3.best_trial.params, stage2_best)

        candidates: list[EloParams] = [stage3_best, stage2_best, stage1_best]

        top_stage3 = sorted(stage3.trials, key=lambda t: t.value)[: args.full_eval_top_k]
        for t in top_stage3:
            candidates.append(stage3_params_from_values(t.params, stage2_best))

        # Deduplicate by serialized params
        seen: set[tuple] = set()
        unique_candidates: list[EloParams] = []
        for p in candidates:
            key = tuple(sorted(params_to_dict(p).items()))
            if key in seen:
                continue
            seen.add(key)
            unique_candidates.append(p)

        best_params, best_metrics_full = select_best_candidate_on_full(unique_candidates, full_matches, full_split)

    print()
    print("=" * 64)
    print("Best selected params on full-history validation:")
    print(
        f"  train={best_metrics_full['train']:.6f} "
        f"val={best_metrics_full['val']:.6f} "
        f"test={best_metrics_full['test']:.6f}"
    )
    print(
        f"  improvement vs active (test): {active_metrics_full['test'] - best_metrics_full['test']:.6f}"
    )
    print(
        f"  improvement vs default (test): {default_metrics_full['test'] - best_metrics_full['test']:.6f}"
    )
    print()
    print("Best parameter values:")
    for key, value in sorted(params_to_dict(best_params).items()):
        if isinstance(value, float):
            print(f"  {key}: {value:.4f}")
        else:
            print(f"  {key}: {value}")

    improvement_vs_active_test = active_metrics_full["test"] - best_metrics_full["test"]
    activated_name = None

    if args.activate_best:
        if improvement_vs_active_test < args.min_improvement:
            print(
                f"Skipping activation: test improvement vs active ({improvement_vs_active_test:.6f}) "
                f"is below --min-improvement ({args.min_improvement:.6f})"
            )
        else:
            run_name = f"optuna-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"
            with get_session() as session:
                record = persist_elo_params(
                    session=session,
                    name=run_name,
                    params=best_params,
                    source="optuna",
                    activate=True,
                )
                activated_name = record.name
                print(f"Activated ELO params set: {record.name}")

    if args.rebuild_live_state:
        params_version = activated_name
        if not params_version:
            run_name = f"optuna-candidate-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"
            with get_session() as session:
                record = persist_elo_params(
                    session=session,
                    name=run_name,
                    params=best_params,
                    source="optuna",
                    activate=False,
                )
                params_version = record.name
                print(f"Persisted non-active ELO params set for rebuild: {record.name}")

        print()
        print("Rebuilding ELO state...")
        processed, _, n_states = rebuild_live_elo_state(best_params, params_version=params_version)
        print(f"Processed matches: {processed}")
        print(f"PlayerEloState rows: {n_states}")

    print("Done.")


if __name__ == "__main__":
    main()
