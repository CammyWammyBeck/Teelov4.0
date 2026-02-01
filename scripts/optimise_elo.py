"""
Optuna-based ELO parameter optimization.

Finds the best K, S, margin, decay, and boost parameters by minimizing
log-loss on a chronological test set (last 3 months of data).

Usage:
    # Quick test (5 trials)
    python scripts/optimise_elo.py --n-trials 5

    # Full optimization (200 trials)
    python scripts/optimise_elo.py --n-trials 200

    # Optimize and write results to DB
    python scripts/optimise_elo.py --n-trials 200 --write-db
"""

import argparse
import math
import sys
from datetime import timedelta

import optuna

from teelo.db.session import get_session
from teelo.elo.pipeline import EloPipeline, EloParams, load_matches_for_elo


def compute_log_loss(probs: list[float]) -> float:
    """
    Compute log-loss (binary cross-entropy) from winner probabilities.

    Each probability is the model's predicted chance that the actual winner
    would win. Perfect predictions give log-loss of 0; random (0.5) gives ~0.693.

    Args:
        probs: List of predicted probabilities for the actual winner

    Returns:
        Mean log-loss (lower is better)
    """
    if not probs:
        return float("inf")

    total = 0.0
    for p in probs:
        # Clamp for numerical stability
        p = max(1e-7, min(1.0 - 1e-7, p))
        total += math.log(p)

    return -total / len(probs)


def create_params_from_trial(trial: optuna.Trial) -> EloParams:
    """
    Create EloParams from an Optuna trial's suggested hyperparameters.

    Defines the search space for all ~18 optimizable parameters.
    """
    return EloParams(
        # Men's K-factors per level (range: 50-300)
        K_F=trial.suggest_float("K_F", 80, 300),
        K_C=trial.suggest_float("K_C", 60, 250),
        K_A=trial.suggest_float("K_A", 50, 200),
        K_M=trial.suggest_float("K_M", 50, 200),
        K_G=trial.suggest_float("K_G", 50, 200),

        # Men's S-factors per level (range: 500-3000)
        S_F=trial.suggest_float("S_F", 500, 2500),
        S_C=trial.suggest_float("S_C", 600, 2800),
        S_A=trial.suggest_float("S_A", 800, 3000),
        S_M=trial.suggest_float("S_M", 800, 3000),
        S_G=trial.suggest_float("S_G", 600, 2800),

        # Women's K-factors per level
        K_WF=trial.suggest_float("K_WF", 80, 300),
        K_WC=trial.suggest_float("K_WC", 60, 250),
        K_WA=trial.suggest_float("K_WA", 50, 200),
        K_WM=trial.suggest_float("K_WM", 50, 200),
        K_WG=trial.suggest_float("K_WG", 50, 200),

        # Women's S-factors per level
        S_WF=trial.suggest_float("S_WF", 500, 2500),
        S_WC=trial.suggest_float("S_WC", 600, 2800),
        S_WA=trial.suggest_float("S_WA", 800, 3000),
        S_WM=trial.suggest_float("S_WM", 800, 3000),
        S_WG=trial.suggest_float("S_WG", 600, 2800),

        # Margin-of-victory parameters (shared across genders)
        margin_base=trial.suggest_float("margin_base", 0.5, 1.2),
        margin_scale=trial.suggest_float("margin_scale", 0.05, 0.8),

        # Decay parameters (shared across genders)
        decay_rate=trial.suggest_float("decay_rate", 0.001, 0.2),
        decay_start_days=trial.suggest_float("decay_start_days", 20, 180),

        # New player boost (shared across genders)
        new_threshold=trial.suggest_int("new_threshold", 5, 80),
        new_boost=trial.suggest_float("new_boost", 1.0, 2.5),

        # Returning player boost (shared across genders)
        returning_days=trial.suggest_float("returning_days", 60, 365),
        returning_boost=trial.suggest_float("returning_boost", 1.0, 2.0),
    )


def main():
    parser = argparse.ArgumentParser(description="Optimize ELO parameters with Optuna")
    parser.add_argument("--n-trials", type=int, default=200, help="Number of Optuna trials")
    parser.add_argument("--write-db", action="store_true", help="Write final ratings to DB with best params")
    parser.add_argument("--test-months", type=int, default=3, help="Months of data for test set")
    args = parser.parse_args()

    # Load all matches once
    print("Loading matches from database...")
    with get_session() as session:
        all_matches = load_matches_for_elo(session)

    if not all_matches:
        print("ERROR: No completed matches found in database.")
        sys.exit(1)

    print(f"Loaded {len(all_matches)} completed matches")

    # Determine train/test split based on match dates
    # Use the last N months as test set
    dated_matches = [m for m in all_matches if m["match_date"] is not None]
    if not dated_matches:
        print("ERROR: No matches with dates found.")
        sys.exit(1)

    max_date = max(m["match_date"] for m in dated_matches)
    split_date = max_date - timedelta(days=args.test_months * 30)
    print(f"Date range: {min(m['match_date'] for m in dated_matches)} to {max_date}")
    print(f"Test split date: {split_date}")

    # Find the index where test set starts (matches are sorted by temporal_order)
    # We need to process ALL matches through the pipeline (to build up ratings),
    # but only evaluate log-loss on the test portion.
    test_start_idx = len(all_matches)  # Default: all training
    for i, m in enumerate(all_matches):
        if m["match_date"] is not None and m["match_date"] >= split_date:
            test_start_idx = i
            break

    n_train = test_start_idx
    n_test = len(all_matches) - test_start_idx
    print(f"Train: {n_train} matches, Test: {n_test} matches")

    if n_test < 50:
        print(f"WARNING: Only {n_test} test matches. Results may be unreliable.")

    # Compute baseline log-loss with default parameters
    baseline_params = EloParams()
    baseline_pipeline = EloPipeline(baseline_params)
    baseline_probs = baseline_pipeline.run_fast(all_matches)
    baseline_test_probs = baseline_probs[test_start_idx:]
    baseline_loss = compute_log_loss(baseline_test_probs)
    print(f"Baseline log-loss (default params): {baseline_loss:.6f}")
    print(f"Random baseline (log-loss 0.5): {-math.log(0.5):.6f}")
    print()

    # Run Optuna optimization
    def objective(trial: optuna.Trial) -> float:
        params = create_params_from_trial(trial)
        pipeline = EloPipeline(params)
        probs = pipeline.run_fast(all_matches)
        # Only evaluate on test portion
        test_probs = probs[test_start_idx:]
        return compute_log_loss(test_probs)

    # Suppress Optuna's per-trial logging (very verbose with all parameter values)
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    print(f"Starting Optuna optimization with {args.n_trials} trials...")
    study = optuna.create_study(
        direction="minimize",
        sampler=optuna.samplers.TPESampler(seed=42),
    )
    study.optimize(objective, n_trials=args.n_trials, show_progress_bar=True)

    # Print results
    best = study.best_trial
    print()
    print("=" * 60)
    print(f"Best log-loss: {best.value:.6f} (baseline: {baseline_loss:.6f})")
    print(f"Improvement: {baseline_loss - best.value:.6f}")
    print()
    print("Best parameters:")
    for key, value in sorted(best.params.items()):
        if isinstance(value, float):
            print(f"  {key}: {value:.4f}")
        else:
            print(f"  {key}: {value}")

    # Optionally write final ratings to DB
    if args.write_db:
        print()
        print("Writing final ratings to database with best parameters...")
        best_params = create_params_from_trial_values(best.params)
        pipeline = EloPipeline(best_params)

        with get_session() as session:
            # Clear existing ELO ratings before rewriting
            from teelo.db.models import EloRating
            session.query(EloRating).delete()
            session.flush()

            n_records = pipeline.run_full(session)
            print(f"Created {n_records} EloRating records")

    print("Done.")


def create_params_from_trial_values(params_dict: dict) -> EloParams:
    """Create EloParams from a dict of parameter values (e.g., from best trial)."""
    return EloParams(**params_dict)


if __name__ == "__main__":
    main()
