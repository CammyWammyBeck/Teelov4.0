# Baseline V2 Feature Set Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `baseline_v2` as a new ML feature-set version with five new feature families, preserve semantic missing values, and deliver exact commands to rebuild features, run selection, train, and evaluate from scratch.

**Architecture:** Extend the existing chronological feature engine by enriching `PlayerState` with compact historical summaries and adding new feature-group modules. Keep `baseline_v1` / `trimmed_v1` unchanged, wire `baseline_v2` into registry construction and feature-engine CLI entrypoints, and validate behavior with focused unit tests before any full backfill or model training.

**Tech Stack:** Python 3.12, SQLAlchemy, PostgreSQL, pytest, XGBoost, Optuna

---

## File Map

**Modify**
- `src/teelo/features/state.py`
- `src/teelo/features/__init__.py`
- `src/teelo/features/engine.py`

**Create**
- `src/teelo/features/groups/opponent_quality.py`
- `src/teelo/features/groups/dominance.py`
- `src/teelo/features/groups/fatigue.py`
- `src/teelo/features/groups/tournament_history.py`
- `src/teelo/features/groups/confidence.py`
- `tests/unit/test_feature_state.py`
- `tests/unit/test_feature_groups_v2.py`
- `tests/unit/test_feature_registry_v2.py`

**Optional modify if needed during execution**
- `src/teelo/ml/versioning.py`
- `scripts/run_hourly_update.py`

## Chunk 1: State Foundation

### Task 1: Add failing state tests for new historical summaries

**Files:**
- Create: `tests/unit/test_feature_state.py`
- Modify: `src/teelo/features/state.py`

- [ ] **Step 1: Write the failing test**

Add tests covering:
- `PlayerState.update()` stores opponent-strength context for later windows
- `PlayerState.update()` stores score-derived summaries needed for dominance/clutch
- tournament-history counters are updated per tournament identity
- missingness metadata can distinguish default surface Elo from observed surface Elo

Use small synthetic `MatchRecord` fixtures and assert concrete stored state values after one or more updates.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_feature_state.py -v`

Expected: FAIL because the new state fields/helpers do not exist yet.

- [ ] **Step 3: Write minimal implementation**

In `src/teelo/features/state.py`:
- extend `MatchRecord` with the minimal extra fields required by `baseline_v2`
- add compact running structures to `PlayerState` for:
  - opponent pre-match Elo context
  - expected-win residual context
  - score summary context
  - tournament-history counters
  - explicit missingness / evidence tracking where it belongs in state
- keep the state compact and chronological; do not add expensive cross-player scans

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_feature_state.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/unit/test_feature_state.py src/teelo/features/state.py
git commit -m "feat: extend feature state for baseline v2"
```

## Chunk 2: New Feature Groups

### Task 2: Add failing tests for opponent-quality and dominance groups

**Files:**
- Create: `tests/unit/test_feature_groups_v2.py`
- Create: `src/teelo/features/groups/opponent_quality.py`
- Create: `src/teelo/features/groups/dominance.py`

- [ ] **Step 1: Write the failing test**

Add tests proving:
- opponent-quality windows compute average opponent Elo and over/under-performance values from prior matches only
- dominance features compute game differential, straight-sets rates, deciding-set behavior, and tiebreak rates
- semantically missing rate features remain `None` when sample size is insufficient

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_feature_groups_v2.py -k "opponent or dominance" -v`

Expected: FAIL because the modules and features are not implemented yet.

- [ ] **Step 3: Write minimal implementation**

Implement:
- `OpponentQualityFeatures` in `src/teelo/features/groups/opponent_quality.py`
- `DominanceFeatures` in `src/teelo/features/groups/dominance.py`

Keep feature names stable and prefix-based so selection ablation can group them cleanly.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_feature_groups_v2.py -k "opponent or dominance" -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/unit/test_feature_groups_v2.py src/teelo/features/groups/opponent_quality.py src/teelo/features/groups/dominance.py
git commit -m "feat: add baseline v2 opponent quality and dominance features"
```

### Task 3: Add failing tests for fatigue, tournament-history, and confidence groups

**Files:**
- Modify: `tests/unit/test_feature_groups_v2.py`
- Create: `src/teelo/features/groups/fatigue.py`
- Create: `src/teelo/features/groups/tournament_history.py`
- Create: `src/teelo/features/groups/confidence.py`

- [ ] **Step 1: Write the failing test**

Add tests proving:
- fatigue features expose differentials between player A and player B rather than only per-player counts
- tournament-history features use prior tournament history only and avoid leaking current match result
- confidence/missingness features emit numeric companion fields for sparse existing and new signals
- rate features that should remain semantically missing still return `None`

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_feature_groups_v2.py -k "fatigue or tournament or confidence" -v`

Expected: FAIL because the feature groups do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Implement:
- `FatigueFeatures`
- `TournamentHistoryFeatures`
- `ConfidenceFeatures`

Ensure the confidence group covers:
- default/unproven surface Elo flags
- estimated-date flags
- evidence counts / enough-data flags for selected rate features

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_feature_groups_v2.py -k "fatigue or tournament or confidence" -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/unit/test_feature_groups_v2.py src/teelo/features/groups/fatigue.py src/teelo/features/groups/tournament_history.py src/teelo/features/groups/confidence.py
git commit -m "feat: add baseline v2 fatigue tournament and confidence features"
```

## Chunk 3: Registry and Engine Wiring

### Task 4: Add failing registry tests for `baseline_v2`

**Files:**
- Create: `tests/unit/test_feature_registry_v2.py`
- Modify: `src/teelo/features/__init__.py`
- Modify: `src/teelo/features/engine.py`

- [ ] **Step 1: Write the failing test**

Add tests covering:
- a new preset or versioned builder path creates a registry for `baseline_v2`
- `baseline_v1` and `trimmed_v1` remain unchanged
- `baseline_v2` includes the new groups and their feature names
- feature-engine CLI accepts the new preset/version cleanly

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_feature_registry_v2.py -v`

Expected: FAIL because `baseline_v2` is not wired yet.

- [ ] **Step 3: Write minimal implementation**

In `src/teelo/features/__init__.py`:
- introduce explicit version-aware registry construction
- keep old behavior working for existing callers
- register the new groups only for `baseline_v2`

In `src/teelo/features/engine.py`:
- update CLI args so a user can backfill `baseline_v2` intentionally
- keep existing defaults stable unless the implementation intentionally changes them

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_feature_registry_v2.py -v`

Expected: PASS

- [ ] **Step 5: Run targeted regression tests**

Run: `pytest tests/unit/test_run_hourly_update_registry.py -v`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add tests/unit/test_feature_registry_v2.py src/teelo/features/__init__.py src/teelo/features/engine.py
git commit -m "feat: wire baseline v2 registry and engine options"
```

## Chunk 4: Integration Verification and Operator Instructions

### Task 5: Verify end-to-end feature computation on tests and document exact rebuild commands

**Files:**
- Modify: `docs/superpowers/specs/2026-03-14-baseline-v2-feature-set-design.md`
- Optionally modify: `src/teelo/ml/versioning.py`
- Optionally modify: `scripts/run_hourly_update.py`

- [ ] **Step 1: Run the full unit test slice for feature work**

Run:
- `pytest tests/unit/test_feature_state.py tests/unit/test_feature_groups_v2.py tests/unit/test_feature_registry_v2.py tests/unit/test_run_hourly_update_registry.py -v`

Expected: all PASS

- [ ] **Step 2: Run a broader regression slice**

Run:
- `pytest tests/unit/test_elo.py tests/unit/test_elo_updater_surface.py tests/unit/test_run_hourly_update_registry.py -v`

Expected: PASS and no feature-engine regressions from state changes

- [ ] **Step 3: Verify feature-engine CLI help and registry wiring**

Run:
- `python -m teelo.features.engine --help`

Expected: help text includes the new preset/version path for `baseline_v2`

- [ ] **Step 4: Decide whether automatic latest-feature-set selection should change**

Review whether `latest_feature_set()` in `src/teelo/ml/versioning.py` should continue returning the most recently created DB feature set, or whether the operator instructions should always pass `--feature-set baseline_v2` explicitly to avoid accidental coupling.

Default recommendation:
- keep code unchanged unless tests or current workflows prove it is unsafe
- use explicit `--feature-set baseline_v2` in all operator commands

- [ ] **Step 5: Add exact operator instructions to the spec or a follow-up doc**

Document the exact commands for a fresh run, in this order:

1. Rebuild inline Elo state if needed:
```bash
python3 scripts/optimise_elo.py --n-trials 200 --split-mode temporal_order --test-ratio 0.2 --activate-best --min-improvement 0.002 --rebuild-live-state
```

2. Backfill `baseline_v2` features from scratch:
```bash
PYTHONPATH=src python -m teelo.features.engine --backfill --feature-set baseline_v2 --preset baseline_v2
```

3. Run feature selection:
```bash
PYTHONPATH=src python -m teelo.ml.selection --feature-set baseline_v2
```

4. Train the model:
```bash
PYTHONPATH=src python -m teelo.ml.trainer --feature-set baseline_v2
```

5. Evaluate on a holdout year:
```bash
PYTHONPATH=src python -m teelo.ml.evaluator --feature-set baseline_v2 --holdout-year 2025
```

6. Optionally backfill predictions:
```bash
PYTHONPATH=src python -m teelo.ml.predictor --feature-set baseline_v2 --backfill
```

If any command syntax must change during implementation, update the plan-adjacent documentation to match the real code before closing the task.

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-03-14-baseline-v2-feature-set-design.md
git commit -m "docs: add baseline v2 rebuild and training instructions"
```

## Chunk 5: Final Verification

### Task 6: Run completion verification before handoff

**Files:**
- No new files

- [ ] **Step 1: Run the fresh verification command set**

Run:
- `pytest tests/unit/test_feature_state.py tests/unit/test_feature_groups_v2.py tests/unit/test_feature_registry_v2.py tests/unit/test_run_hourly_update_registry.py tests/unit/test_elo.py tests/unit/test_elo_updater_surface.py -v`

Expected: PASS

- [ ] **Step 2: Run git status**

Run: `git status --short`

Expected: only intended files are modified

- [ ] **Step 3: Prepare handoff summary**

Summarize:
- new feature families added
- how missing values were handled
- exact commands to rebuild features, run selection, train, evaluate, and backfill predictions
- any residual risk, especially performance of full historical backfill

