# Baseline V2 Feature Set Design

**Date:** 2026-03-14
**Status:** Approved in conversation, pending written-spec review

## Goal

Add a new ML feature-set version, `baseline_v2`, that extends the current tennis match prediction pipeline with five new feature families:

- opponent-strength-adjusted form
- score-derived dominance and clutch signals
- fatigue differential signals
- tournament-history signals
- confidence and missingness companion signals

The new version must preserve direct comparability with `baseline_v1` and `trimmed_v1`, avoid rewriting the current pipeline, and keep semantically meaningful missing values where `None` means "unknown" or "insufficient evidence."

## Context

The current feature pipeline computes chronological match features via a Python replay over historical matches and stores them in `match_features`. Existing feature groups cover:

- context
- Elo
- form
- head-to-head
- activity

The live database strongly supports match-history-driven features:

- match outcomes, dates, Elo snapshots, and structured scores have near-complete coverage
- tournament surface, level, year, and dates have near-complete coverage

The database does **not** currently support reliable enrichment-heavy features:

- player bios such as height, handedness, birth date, and turned-pro year are sparse
- `matches.stats`, `matches.odds_*`, `tournaments.indoor_outdoor`, `tournament_editions.draw_size`, and `tournament_editions.prize_money_usd` have effectively no useful coverage today

This design therefore focuses on features that can be derived from already-populated historical match and tournament data.

## Non-Goals

- Replacing `baseline_v1` or `trimmed_v1`
- Rewriting the trainer, selector, evaluator, or predictor architecture
- Converting all missing features to numeric defaults
- Adding new scraping for odds, serve stats, or player-biographical fields
- Adding tournament winner simulation in this change

## Design Principles

### 1. Additive versioning

`baseline_v2` is a new feature-set version. Existing feature sets remain intact for model comparison, regression checks, and rollback.

### 2. Preserve semantic missingness

`None` should remain whenever a feature genuinely means:

- unknown
- not enough sample size
- not applicable

This is especially important for rates and ratios such as surface win rate, H2H dominance, and date-driven recency metrics.

### 3. Pair sparse signals with explicit evidence

Whenever a feature can be `None`, the new version should expose numeric companion features where useful:

- sample counts
- eligibility flags
- default-rating flags
- estimated-date flags

This lets XGBoost use native missing-value handling while also learning why a feature is missing.

### 4. Minimal churn to the pipeline

The feature engine should continue to:

1. replay matches in chronological order
2. maintain in-memory per-player state
3. compute a feature dict for each match
4. persist features into `match_features`

The main change is extending the tracked state and adding new feature-group modules.

## Proposed Architecture

### Existing components to preserve

- `teelo.features.engine.FeatureEngine`
- `teelo.features.registry.FeatureRegistry`
- existing feature groups and presets
- `ModelTrainer`, `FeatureSelector`, `ModelEvaluator`, and `BatchPredictor`

### New or modified components

- Extend `PlayerState` in `src/teelo/features/state.py`
- Add new feature-group modules under `src/teelo/features/groups/`
- Register new groups in `src/teelo/features/__init__.py`
- Add focused tests for state update behavior and feature outputs

### Preset/version strategy

Create a new registry preset path for `baseline_v2`. If trimmed behavior is still desired after selection, add a later trimmed v2 preset based on fresh selection output rather than guessing upfront.

## Feature Families

### 1. Opponent-Strength-Adjusted Form

### Purpose

The current form features measure results frequency and win rate, but they do not directly encode how strong recent opponents were. Two players with the same 8-week win rate can arrive from very different schedules.

### Candidate features

- average opponent Elo over recent windows
- average opponent surface Elo over recent windows when available
- wins over higher-Elo opponents in recent windows
- losses to lower-Elo opponents in recent windows
- rolling performance versus Elo expectation:
  - actual wins minus expected wins
  - mean residual `(actual - expected)`

### Data requirements

Derived from current state plus pre-match Elo information already available during chronological replay.

### State additions needed

Each historical match record should retain enough opponent-strength context to compute these windows efficiently, for example:

- opponent pre-match Elo
- opponent pre-match surface Elo
- expected win probability for the player

### 2. Score-Derived Dominance and Clutch

### Purpose

Current activity features only use total games from the last match and tournament. Structured score data can provide richer information about how convincingly or narrowly a player tends to win and how often they survive close sets.

### Candidate features

- rolling game differential
- rolling set differential
- straight-sets win rate
- deciding-set rate
- tiebreak participation rate
- tiebreak win rate
- close-match rate, defined from narrow game or set margins

### Data requirements

Derived from `score_structured`, which is already populated for nearly all completed matches.

### State additions needed

Historical match records should retain derived score summaries such as:

- games won / games lost
- sets won / sets lost
- whether match went deciding set
- tiebreak count
- tiebreaks won / lost

### 3. Fatigue Differential

### Purpose

Current activity features are mostly per-player totals. Match prediction often benefits from explicit asymmetry: one player arrives fresher, or one player has logged far more court time in the last week or in the current tournament.

### Candidate features

- rest-day differential
- short-window match-count differential
- short-window game-load differential
- consecutive-day play flags
- in-tournament cumulative games differential
- in-tournament cumulative matches differential

### Data requirements

Derived from match dates, existing match histories, and tournament edition IDs.

### State additions needed

Mostly derivable from current match history, but helper methods should avoid repeated full scans where practical.

### 4. Tournament History

### Purpose

Generic surface and level form miss venue-specific familiarity. Some players repeatedly outperform their generic baseline at the same tournament or same tournament family across years.

### Candidate features

- prior matches at same tournament
- prior wins at same tournament
- same-tournament win rate
- prior matches at same tournament in last N years
- same-tournament surface-context matches and win rate

### Data requirements

Derived from `tournament_edition_id`, tournament identity, year, and replayed match history.

### State additions needed

Running counts keyed by tournament identity and optionally by tournament plus recent-year cutoff.

### 5. Confidence and Missingness Companions

### Purpose

Many existing and new features have legitimate missing values. Rather than forcing fake numeric defaults, expose the amount and quality of evidence behind those features.

### Candidate features

- sample counts behind surface form windows
- sample counts behind H2H and H2H-on-surface
- sample counts behind tournament-history rates
- sample counts behind opponent-strength windows
- flag that current surface Elo is default / unproven
- flag that match date was estimated
- has-enough-data flags for selected rate features

### Data requirements

Derived from existing state and match context.

## Missing-Value Policy

### Keep `None`

Keep `None` where zero would be semantically false, including:

- rates with insufficient sample size
- ratios with zero denominator
- date-delta features when no dated prior match exists
- context values that are genuinely unknown

### Return numbers

Return numeric values where zero is literal and unambiguous, including:

- counts of wins, losses, matches, tiebreaks
- explicit indicator flags
- differentials between two observed numeric quantities

### Why this is beneficial

For this codebase, `None` is beneficial because:

- it preserves meaning
- it avoids cold-start bias against new or sparse-data players
- XGBoost natively handles missing values
- it distinguishes "unknown" from "observed zero"

The downside is ambiguity around *why* a feature is missing, which is exactly why `baseline_v2` should add confidence and evidence companions.

## Data Flow

1. Load matches chronologically as today.
2. Build `MatchContext` as today.
3. For each match:
   - compute `baseline_v2` features from current state
   - persist the feature row
4. After feature computation for completed matches:
   - update state with outcome, score-derived summaries, opponent-strength summaries, and tournament-history counters

No trainer or prediction storage redesign is needed.

## Testing Strategy

Use TDD for all implementation work.

### Unit tests

- state update tests for new tracked values
- per-group feature tests with controlled chronological histories
- edge-case tests for:
  - no prior data
  - insufficient sample size
  - estimated match dates
  - missing surface Elo
  - tournament-history cold starts

### Integration verification

- run feature engine for a test subset or fixture-backed scenario
- confirm `FeatureSet` row contains the new feature definitions
- confirm `match_features` rows populate for the new set
- run feature selection and training against the new set without schema or runtime errors

## Operational Instructions to Deliver

Implementation must conclude with exact commands for:

- rebuilding or refreshing prerequisite Elo artifacts if needed
- computing `baseline_v2` features from scratch
- running feature selection on `baseline_v2`
- training a model on `baseline_v2`
- evaluating the trained model
- optionally backfilling predictions

These instructions should assume the user wants to start from the beginning of the feature/training path.

## Risks

- State growth can become inefficient if new history is stored naively.
- Tournament-history features may need careful key selection to avoid edition-vs-tournament confusion.
- Opponent-strength windows can become expensive if implemented with repeated scans instead of compact running records.
- Added features may improve training loss but not holdout quality; feature selection must be rerun from scratch.

## Mitigations

- Store only compact per-match summaries in state
- prefer rolling aggregates or bounded histories where possible
- preserve `baseline_v1` for direct comparison
- validate with temporal holdout and fresh feature selection

## Success Criteria

- `baseline_v2` can be computed end-to-end without breaking the existing pipeline
- existing feature sets remain usable unchanged
- new feature groups have tests proving semantics and missing-value behavior
- the user receives exact reproducible commands to rebuild features and retrain from scratch
- missing-value semantics remain intentional, not accidental
