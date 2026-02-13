"""
Optuna-based ELO parameter optimization.

Finds the best K, S, margin, decay, and boost parameters by minimizing
log-loss on a chronological test set (last 3 months of data).

Usage:
    # Quick test (5 trials)
    python scripts/optimise_elo.py --n-trials 5

    # Full optimization (200 trials)
    python scripts/optimise_elo.py --n-trials 200

    # More aggressive exploration profile
    python scripts/optimise_elo.py --n-trials 1000 --n-startup-trials 200 --n-ei-candidates 128

    # Use all history for ELO warmup, score on last 5 years (80/20 split)
    python scripts/optimise_elo.py --split-mode recent_years_ratio --eval-years 5 --test-ratio 0.2

    # Optimize and rebuild live inline ELO state/snapshots
    python scripts/optimise_elo.py --n-trials 200 --rebuild-live-state --activate-best
"""

import argparse
import math
import sys
from datetime import datetime, timedelta

import optuna

from teelo.db.session import get_session
from teelo.elo.pipeline import EloPipeline, EloParams, load_matches_for_elo
from teelo.elo.params_store import get_active_elo_params, persist_elo_params


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


def rebuild_live_elo_state(params: EloParams, params_version: str) -> tuple[int, int, int]:
    """
    Rebuild live inline ELO artifacts from completed matches in temporal order.

    This rebuilds:
    - match-level pre/post ELO snapshots + metadata columns
    - player_elo_states
    """
    from teelo.db.models import Match, PlayerEloState, TournamentEdition, Tournament
    from teelo.elo.constants import get_level_code
    from teelo.elo.live import LiveEloUpdater, TERMINAL_STATUSES

    with get_session() as session:
        session.query(PlayerEloState).delete()
        session.query(Match).update(
            {
                Match.elo_pre_player_a: None,
                Match.elo_pre_player_b: None,
                Match.elo_post_player_a: None,
                Match.elo_post_player_b: None,
                Match.elo_params_version: None,
                Match.elo_processed_at: None,
                Match.elo_needs_recompute: False,
            },
            synchronize_session=False,
        )
        session.flush()

        matches = (
            session.query(Match)
            .join(TournamentEdition, Match.tournament_edition_id == TournamentEdition.id)
            .join(Tournament, TournamentEdition.tournament_id == Tournament.id)
            .filter(
                Match.status.in_(tuple(TERMINAL_STATUSES)),
                Match.winner_id.isnot(None),
                Match.temporal_order.isnot(None),
            )
            .order_by(Match.temporal_order.asc(), Match.id.asc())
            .all()
        )

        updater = LiveEloUpdater(params=params, params_version=params_version)
        processed = 0
        flagged_recompute = 0
        for match in matches:
            level_code = get_level_code(
                match.tournament_edition.tournament.level,
                match.tournament_edition.tournament.tour,
            )
            updater.ensure_pre_match_snapshot(session, match, force=True)
            if updater.apply_completed_match(session, match, level_code=level_code):
                processed += 1
            elif match.elo_needs_recompute:
                flagged_recompute += 1

        session.commit()
        n_states = session.query(PlayerEloState).count()
        return processed, flagged_recompute, n_states


def main():
    parser = argparse.ArgumentParser(description="Optimize ELO parameters with Optuna")
    parser.add_argument("--n-trials", type=int, default=200, help="Number of Optuna trials")
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for Optuna sampler",
    )
    parser.add_argument(
        "--n-startup-trials",
        type=int,
        default=120,
        help="Number of random startup trials before TPE exploitation",
    )
    parser.add_argument(
        "--n-ei-candidates",
        type=int,
        default=96,
        help="Number of TPE candidate points per trial (higher explores more, slower per trial)",
    )
    parser.add_argument(
        "--independent-tpe",
        action="store_true",
        help="Use independent (non-multivariate) TPE sampling",
    )
    parser.add_argument(
        "--no-group-tpe",
        action="store_true",
        help="Disable grouped TPE dimensions (only applies with multivariate TPE)",
    )
    parser.add_argument(
        "--rebuild-live-state",
        action="store_true",
        help="Rebuild inline ELO snapshots/player state from completed matches in temporal order",
    )
    parser.add_argument(
        "--activate-best",
        action="store_true",
        help="Persist and activate best params for inline ELO updates",
    )
    parser.add_argument(
        "--min-improvement",
        type=float,
        default=0.0,
        help="Minimum log-loss improvement vs active params required to activate best params",
    )
    parser.add_argument(
        "--split-mode",
        choices=["match_date", "temporal_order", "recent_years_ratio"],
        default="recent_years_ratio",
        help="How to split train/test set",
    )
    parser.add_argument("--test-months", type=int, default=3, help="Months of data for test set when split-mode=match_date")
    parser.add_argument(
        "--eval-years",
        type=int,
        default=5,
        help="Years in evaluation window when split-mode=recent_years_ratio",
    )
    parser.add_argument(
        "--test-ratio",
        type=float,
        default=0.2,
        help="Fraction of matches for test set tail when split-mode=temporal_order/recent_years_ratio",
    )
    args = parser.parse_args()

    if not (0 < args.test_ratio < 1):
        print("ERROR: --test-ratio must be between 0 and 1.")
        sys.exit(2)
    if args.eval_years < 1:
        print("ERROR: --eval-years must be >= 1.")
        sys.exit(2)
    if args.n_startup_trials < 0:
        print("ERROR: --n-startup-trials must be >= 0.")
        sys.exit(2)
    if args.n_ei_candidates < 2:
        print("ERROR: --n-ei-candidates must be >= 2.")
        sys.exit(2)

    # Load all matches once
    print("Loading matches from database...")
    with get_session() as session:
        all_matches = load_matches_for_elo(session)
        active_params, active_params_version = get_active_elo_params(session)

    if not all_matches:
        print("ERROR: No completed matches found in database.")
        sys.exit(1)

    print(f"Loaded {len(all_matches)} completed matches")

    # Determine train/test split
    if args.split_mode == "match_date":
        dated_matches = [m for m in all_matches if m["match_date"] is not None]
        if not dated_matches:
            print("ERROR: No matches with dates found.")
            sys.exit(1)

        max_date = max(m["match_date"] for m in dated_matches)
        split_date = max_date - timedelta(days=args.test_months * 30)
        print(f"Date range: {min(m['match_date'] for m in dated_matches)} to {max_date}")
        print(f"Test split date: {split_date}")

        # Matches are sorted by temporal_order
        test_start_idx = len(all_matches)
        for i, m in enumerate(all_matches):
            if m["match_date"] is not None and m["match_date"] >= split_date:
                test_start_idx = i
                break
        warmup_count = test_start_idx
        eval_start_idx = test_start_idx
    elif args.split_mode == "recent_years_ratio":
        dated_matches = [m for m in all_matches if m["match_date"] is not None]
        if not dated_matches:
            print("ERROR: No matches with dates found.")
            sys.exit(1)

        max_date = max(m["match_date"] for m in dated_matches)
        eval_start_date = max_date - timedelta(days=args.eval_years * 365)
        eval_start_idx = len(all_matches)
        for i, m in enumerate(all_matches):
            if m["match_date"] is not None and m["match_date"] >= eval_start_date:
                eval_start_idx = i
                break

        if eval_start_idx >= len(all_matches):
            print("ERROR: No matches found in recent evaluation window.")
            sys.exit(1)

        n_eval = len(all_matches) - eval_start_idx
        if n_eval < 2:
            print("ERROR: Not enough matches in recent evaluation window.")
            sys.exit(1)

        test_start_idx = eval_start_idx + int(n_eval * (1 - args.test_ratio))
        test_start_idx = max(eval_start_idx + 1, min(test_start_idx, len(all_matches) - 1))
        warmup_count = eval_start_idx
        print(
            f"Split mode: recent_years_ratio "
            f"(window start: {eval_start_date}, eval matches: {n_eval}, test tail: {args.test_ratio:.0%})"
        )
    else:
        test_start_idx = int(len(all_matches) * (1 - args.test_ratio))
        test_start_idx = max(1, min(test_start_idx, len(all_matches) - 1))
        warmup_count = test_start_idx
        eval_start_idx = test_start_idx
        print(f"Split mode: temporal_order ({args.test_ratio:.0%} test tail)")

    n_train = test_start_idx - eval_start_idx
    n_test = len(all_matches) - test_start_idx
    print(f"Warmup (not scored): {warmup_count} matches")
    print(f"Train (scored window): {n_train} matches, Test: {n_test} matches")

    if n_test < 50:
        print(f"WARNING: Only {n_test} test matches. Results may be unreliable.")

    # Compute baseline log-loss with default parameters
    baseline_params = EloParams()
    baseline_pipeline = EloPipeline(baseline_params)
    baseline_probs = baseline_pipeline.run_fast(all_matches)
    baseline_test_probs = baseline_probs[test_start_idx:]
    baseline_loss = compute_log_loss(baseline_test_probs)

    active_pipeline = EloPipeline(active_params)
    active_probs = active_pipeline.run_fast(all_matches)
    active_test_probs = active_probs[test_start_idx:]
    active_loss = compute_log_loss(active_test_probs)

    print(f"Baseline log-loss (default params): {baseline_loss:.6f}")
    print(f"Baseline log-loss (active params: {active_params_version}): {active_loss:.6f}")
    print(f"Random baseline: {-math.log(0.5):.6f}")
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

    multivariate_tpe = not args.independent_tpe
    group_tpe = multivariate_tpe and (not args.no_group_tpe)
    sampler = optuna.samplers.TPESampler(
        seed=args.seed,
        n_startup_trials=args.n_startup_trials,
        n_ei_candidates=args.n_ei_candidates,
        multivariate=multivariate_tpe,
        group=group_tpe,
    )
    print("Sampler configuration:")
    print(f"  seed={args.seed}")
    print(f"  n_startup_trials={args.n_startup_trials}")
    print(f"  n_ei_candidates={args.n_ei_candidates}")
    print(f"  multivariate_tpe={multivariate_tpe}")
    print(f"  group_tpe={group_tpe}")
    print()

    print(f"Starting Optuna optimization with {args.n_trials} trials...")
    study = optuna.create_study(
        direction="minimize",
        sampler=sampler,
    )
    study.optimize(objective, n_trials=args.n_trials, show_progress_bar=True)

    # Print results
    best = study.best_trial
    print()
    print("=" * 60)
    print(f"Best log-loss: {best.value:.6f}")
    print(f"Improvement vs default: {baseline_loss - best.value:.6f}")
    print(f"Improvement vs active ({active_params_version}): {active_loss - best.value:.6f}")
    print()
    print("Best parameters:")
    for key, value in sorted(best.params.items()):
        if isinstance(value, float):
            print(f"  {key}: {value:.4f}")
        else:
            print(f"  {key}: {value}")


    best_params = create_params_from_trial_values(best.params)
    improvement_vs_active = active_loss - best.value
    activated_name = None

    if args.activate_best:
        if improvement_vs_active < args.min_improvement:
            print(
                f"Skipping activation: improvement vs active ({improvement_vs_active:.6f}) "
                f"is below --min-improvement ({args.min_improvement:.6f})"
            )
        else:
            run_name = f"optuna-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
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
            run_name = f"optuna-candidate-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
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
        print("Rebuilding live inline ELO state...")
        processed, flagged, n_states = rebuild_live_elo_state(best_params, params_version=params_version)
        print(f"Processed matches: {processed}")
        print(f"Out-of-order flagged: {flagged}")
        print(f"PlayerEloState rows: {n_states}")

    print("Done.")


def create_params_from_trial_values(params_dict: dict) -> EloParams:
    """Create EloParams from a dict of parameter values (e.g., from best trial)."""
    return EloParams(**params_dict)


if __name__ == "__main__":
    main()
