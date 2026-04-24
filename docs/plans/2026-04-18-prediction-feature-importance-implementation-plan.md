# Prediction Feature Importance Implementation Plan

> **Status (2026-04-24 revision):** Rewritten after review. v1 now pre-computes per-feature contributions during the prediction pipeline and stores them on `Match.prediction_explanation`, rather than computing on demand at page-load time.

**Goal:** For every stored match prediction, record how much each feature contributed to the difference between the baseline 50% probability and the final predicted probability. Surface top drivers on the public match detail page. Make the stored payload directly consumable by the content-generation agent.

**Architecture:**

1. Make each prediction traceable to its exact model artifact (change `prediction_model_version` from a timestamp to the artifact filename, add `model_artifact` + `feature_set_name` to trainer metadata).
2. Compute local XGBoost contribution scores in the prediction pipeline using the same swap-averaged orientation policy as the prediction itself.
3. Convert raw margin contributions into **percentage-point attributions** relative to the 50% baseline so they sum to `(final_prob − 0.5) × 100`.
4. Persist the full per-feature attribution plus pre-aggregated group rollups on `Match.prediction_explanation` (new JSONB column).
5. Fix the existing `MatchFeatures` lookup bug on the match detail page and render an explanation summary above the existing feature table.

**Tech Stack:** Python 3.11, FastAPI, SQLAlchemy, Alembic, XGBoost, Jinja, existing `MatchFeatures` JSONB feature store.

---

## Background

The repo already has:

- per-match stored feature snapshots in `src/teelo/db/models.py` via `MatchFeatures.features`
- batch prediction writes in `src/teelo/ml/predictor.py` (called from `scripts/run_hourly_update.py::_run_predictions_stage`)
- a public match detail page that loads feature data in `src/teelo/web/routers/matches.py`
- a grouped raw feature display in `src/teelo/web/services/feature_display.py` and `src/teelo/web/templates/match_detail.html`
- offline global feature selection and gain ranking in `src/teelo/ml/selection.py`
- swap-averaged inference helpers in `src/teelo/ml/randomize.py::swap_ab_features`, used by both `BatchPredictor._predict_chunk` and `services/forecast_prediction.py::predict_probability_a`

The missing piece is per-prediction local explainability in a form that both the match-detail UI and a downstream content-generation agent can consume.

## Decisions Captured During Planning

- v1 answers "which features pushed this specific prediction toward A or B, and by how many percentage points" — not global gain rankings.
- Contributions are persisted at **pipeline time**, not computed on demand. This keeps page loads free, gives the content agent a stable read target, and freezes the explanation alongside the prediction it describes.
- Storage shape: **all** per-column attributions, not a top-N cut. Sorting/filtering is a trivial render-time operation, and storing everything keeps v1 future-proof for the content agent.
- Unit is **percentage points relative to the 50% baseline**. A feature with `pp = +3.2` contributed +3.2 pp toward the `P(A wins)` the model eventually predicted. All per-feature `pp` values sum exactly to `(prediction_a − 0.5) × 100` by construction.
- `v1` does **not** add a new `shap` dependency. XGBoost native `pred_contribs` is sufficient for a tree-based model.
- `v1` does **not** backfill historical predictions. Forward-only; existing matches without an explanation degrade gracefully in the UI.
- `v1` does **not** cover tournament forecast nodes. Match-level only.

## Recommended Approach

Pre-compute local XGBoost contribution scores inside `BatchPredictor._predict_chunk`, reusing the already-loaded model. For each match:

1. Call `booster.predict(DMatrix(X_orig), pred_contribs=True)` and the same for `X_swap`.
2. Remap the swap orientation contributions back to original A/B semantics (column rename for paired features, and a global sign flip because swap contribs describe "push toward swapped-A" i.e. "push toward orig-B").
3. Average the two remapped margin-contribution vectors.
4. Drop the XGBoost bias column.
5. Convert to percentage points via proportional attribution:

   ```
   pp_i = (c_i / Σ|c_j|) × (prediction_a − 0.5) × 100
   ```

   This choice — rather than sigmoid-space counterfactual deltas — gives an additive, sign-preserving, human-readable decomposition that sums cleanly to the displayed difference.
6. Aggregate into paired rows (suffix/infix pairs collapse to one entry) and group rollups using the existing feature registry.
7. Persist on `Match.prediction_explanation`.

## Alternatives Considered

### 1. On-demand explanation at page load (previous plan)

Rejected because:

- every match detail page view would re-load the XGBoost model (no caching layer exists today)
- the content agent would need model-artifact access and XGBoost to read explanations
- legacy artifact resolution is fragile and silently ambiguous when feature sets overlap

### 2. Persist only a top-N cut

Rejected because the match detail page already renders the full feature table, the content agent may want to highlight long-tail drivers, and sorting at render time is trivial.

### 3. SHAP as a new dependency

Rejected — native `pred_contribs=True` on a tree-based XGBoost booster is equivalent for our purposes and avoids an extra dependency.

## Constraints And Risks

- `Match.prediction_model_version` today stores `metadata["created_at"]` rather than the artifact filename (`src/teelo/ml/predictor.py:50`). Changing this affects `ModelEvaluationSnapshot.model_version` semantics too — the metrics stage reads the same metadata field (`scripts/run_hourly_update.py:295`). This plan updates both sites in one pass.
- Trainer metadata does not currently persist `feature_set_name` or `model_artifact` (`src/teelo/ml/trainer.py::_save`). Fixed here.
- The match detail router selects `MatchFeatures` by `computed_at desc` alone (`src/teelo/web/routers/matches.py:136-142`). If multiple feature sets exist for a match, this can return a snapshot that does not match the prediction. Fixed here by keying on the explanation's `feature_set_name`.
- Contributions are additive on **model margin** (logit), but the final prediction is the average of two probabilities — `(prob_orig + (1 − prob_swap)) / 2`. There is no exact margin decomposition of that quantity. The proportional-attribution approach in step 5 above is an approximation; the UI must frame these as attributions, not causal or counterfactual effects.
- Legacy predictions (before this change) will have `prediction_model_version` set to a timestamp and no stored explanation. They will show the existing feature table without an explanation summary.

## Scope

### In Scope

- `Match.prediction_explanation` column (Alembic migration)
- exact model-artifact traceability going forward
- explanation engine (`src/teelo/ml/explanations.py`)
- integration into `BatchPredictor` pipeline path
- forecast-side alignment of model-version naming
- `MatchFeatures` snapshot selection fix
- explanation UI on `/matches/{match_id}` above the existing feature table
- unit tests for remap/attribution correctness and route integration
- `docs/prediction-explainability.md` including the payload contract for the content agent

### Out Of Scope

- admin dashboard for global importance
- backfill of historical predictions (forward-only; a standalone backfill script is a follow-up)
- tournament forecast node explanations
- any UI beyond the match detail page
- new public API endpoints

## File Map

### Create

- `alembic/versions/<new>_add_match_prediction_explanation.py`
- `src/teelo/ml/explanations.py`
- `tests/unit/test_prediction_explanations.py`
- `tests/unit/test_model_version_resolution.py`
- `tests/unit/test_match_detail_feature_importance.py`
- `docs/prediction-explainability.md`

### Modify

- `src/teelo/db/models.py` — add `prediction_explanation` column
- `src/teelo/ml/trainer.py` — extend saved metadata
- `src/teelo/ml/predictor.py` — artifact-name versioning, compute + write explanations
- `src/teelo/ml/versioning.py` — add artifact resolver helpers
- `src/teelo/services/forecast_prediction.py` — artifact-name versioning
- `scripts/run_hourly_update.py` — metrics stage reads artifact name
- `src/teelo/web/routers/matches.py` — correct MatchFeatures lookup, pass explanation
- `src/teelo/web/services/feature_display.py` — small helpers for pp annotation
- `src/teelo/web/templates/match_detail.html` — new explanation section
- `README.md`

## Target Behavior

### Pipeline run (forward)

For each new prediction the hourly pipeline makes, `Match.prediction_explanation` is populated atomically with the prediction itself. `prediction_model_version` stores the artifact filename (e.g. `prediction_v17.json`).

### Match detail page

When a user opens `/matches/{match_id}`:

1. Load the match and its stored prediction.
2. If `prediction_explanation` is present, use its `feature_set_name` to pick the matching `MatchFeatures` row. Render:
   - a new hero section: "Why Teelo favors {player}"
   - top drivers toward Player A
   - top drivers toward Player B
   - an optional group-rollup summary
   - the existing raw feature table below, with a per-row pp annotation where available
3. If `prediction_explanation` is missing (legacy prediction), fall back to the existing raw feature table and show a small note.

## Data Contracts

### Trainer metadata (going forward)

```json
{
  "model_artifact": "prediction_v17.json",
  "feature_set_name": "baseline_v2",
  "feature_names": ["elo_a", "elo_b", "elo_diff", ...],
  "params": {},
  "cv_scores": {},
  "train_size": 123456,
  "date_range": "2015-2025",
  "created_at": "2026-04-18T12:34:56+00:00"
}
```

### `Match.prediction_explanation` payload

```json
{
  "model_artifact": "prediction_v17.json",
  "feature_set_name": "baseline_v2",
  "base_prob": 0.5,
  "prediction_a": 0.6412,
  "contributions": {
    "elo_a": 4.21,
    "elo_b": -3.87,
    "elo_diff": 2.10,
    "h2h_a_wins": 1.05,
    "...": "..."
  },
  "paired_rows": [
    {"key": "elo", "display_name": "ELO", "pp": 5.3},
    {"key": "h2h_wins", "display_name": "H2H Wins", "pp": 1.8}
  ],
  "groups": [
    {"key": "elo_core", "display_name": "ELO Ratings", "pp": 7.1},
    {"key": "form", "display_name": "Recent Form", "pp": 2.3}
  ]
}
```

Contract notes:

- All `pp` values are **percentage points toward Player A**. Positive = pushes toward A, negative = pushes toward B.
- `sum(contributions.values()) == (prediction_a − base_prob) × 100` by construction (up to float epsilon).
- `paired_rows` and `groups` are convenience pre-aggregations. They are derived from `contributions` — consumers can recompute them if they prefer.
- `contributions` is keyed by the exact feature column names the model was trained on (same as `MatchFeatures.features` for that feature set).
- The shape is additive-only in v1. No feature-interaction terms.

## Explanation Method

For each match in the prediction chunk:

1. Build `X_orig` from the stored feature dict, reindexed to `feature_names`.
2. Build `X_swap` from `swap_ab_features(features)`, reindexed to `feature_names`.
3. Compute `contribs_orig = booster.predict(DMatrix(X_orig), pred_contribs=True)` — shape `(n, n_features + 1)`, last column is bias.
4. Compute `contribs_swap` similarly.
5. Remap `contribs_swap` back to original orientation:
   - For each `_a`/`_b` suffix pair, swap the column positions.
   - For each `h2h_a_*`/`h2h_b_*` infix pair, swap the column positions.
   - Negate every column (because swap contribs describe "toward swapped-A" = "toward orig-B").
6. `contribs_avg = (contribs_orig + contribs_swap_remapped) / 2`. Drop the bias column.
7. Attribute percentage points:

   ```
   total_abs = sum(|c| for c in contribs_avg[match])
   pp[f] = (contribs_avg[match][f] / total_abs) * (prediction_a - 0.5) * 100
   ```

   Edge case: `total_abs == 0` → all pp zero (the model is at pure baseline for this match). Skip pp computation entirely.
8. Aggregate paired rows (suffix/infix pairs collapse) and groups using the existing `FeatureRegistry.grouped_features()`.

Note on neutral features: `swap_ab_features` passes truly neutral features through unchanged. If such a feature genuinely does not discriminate A vs B, `contribs_orig` and `contribs_swap` will be approximately equal and opposite after the global sign flip, so the average is ≈0. This is correct: a feature that cannot tell the two players apart should not be a driver in an A-vs-B attribution.

## Pipeline Integration

`BatchPredictor._predict_chunk` currently does two `predict_proba` calls on the loaded model. After this change it will additionally:

- call `booster.predict(..., pred_contribs=True)` on both orientations (one extra traversal per chunk per orientation — effectively free compared to the boosted-tree prediction already happening)
- build `prediction_explanation` per match using the helper in `explanations.py`
- include `prediction_explanation` in the bulk update payload

`prediction_model_version` changes from `metadata["created_at"]` to `Path(self.model_path).name` (e.g. `prediction_v17.json`). The metrics stage in `run_hourly_update.py` is updated to read the artifact name consistently.

## Legacy Behavior

- Historical matches keep their existing `prediction_model_version` (timestamp string). They have no `prediction_explanation`.
- The match detail page degrades gracefully: it shows the existing feature table without an explanation summary, with a small footer note.
- A `resolve_model_artifact_name(model_ref)` helper is added to `versioning.py` for any future use case that needs to map a legacy timestamp back to an artifact. It is **not** used in the page load path; the page uses the stored `prediction_explanation.model_artifact` directly.
- An optional `teelo predictions-backfill-explanations` script is listed as follow-up work, not part of v1.

## Chunks

### Chunk 1: Schema and traceability

**Files:**
- Create: `alembic/versions/<new>_add_match_prediction_explanation.py`
- Modify: `src/teelo/db/models.py`
- Modify: `src/teelo/ml/trainer.py`
- Modify: `src/teelo/ml/versioning.py`
- Test: `tests/unit/test_model_version_resolution.py`

Steps:

1. Add a failing test for trainer metadata fields (`model_artifact`, `feature_set_name`).
2. Add a failing test for artifact-name resolution from a legacy timestamp.
3. Write the Alembic migration adding `matches.prediction_explanation` JSONB nullable (down-rev = `2b8c0c3d2c1a`).
4. Add the mapped column to `Match` in `src/teelo/db/models.py`.
5. Extend `ModelTrainer._save()` metadata.
6. Add `resolve_model_artifact_name`, `load_model_metadata_by_artifact`, `infer_feature_set_name` helpers in `versioning.py`.  `infer_feature_set_name` must raise on ambiguity rather than guess.
7. Run `pytest tests/unit/test_model_version_resolution.py -v`.

### Chunk 2: Explanation engine

**Files:**
- Create: `src/teelo/ml/explanations.py`
- Test: `tests/unit/test_prediction_explanations.py`

Steps:

1. Failing tests:
   - orientation remap + sign-flip preserves additivity
   - bias column is excluded from final payload
   - `sum(contributions.values())` matches `(prediction_a − 0.5) × 100` within float epsilon
   - `diff` and `dominance` features handle correctly (mirrors `swap_ab_features` sign/complement logic)
   - `total_abs == 0` edge case returns all-zero pp without NaN
2. Public API:
   ```python
   def build_explanation(
       *,
       booster: xgb.Booster,
       feature_names: list[str],
       feature_set_name: str,
       model_artifact: str,
       features: dict[str, Any],
       prediction_a: float,
       registry: FeatureRegistry | None = None,
   ) -> dict[str, Any]
   ```
3. Internal helpers for `_build_matrices`, `_compute_contribs`, `_remap_swap_contribs`, `_attribute_pp`, `_aggregate_paired`, `_aggregate_groups`.
4. Run `pytest tests/unit/test_prediction_explanations.py -v`.

### Chunk 3: Pipeline wiring

**Files:**
- Modify: `src/teelo/ml/predictor.py`
- Modify: `src/teelo/services/forecast_prediction.py`
- Modify: `scripts/run_hourly_update.py`
- Test: additions to existing unit tests if needed

Steps:

1. Change `BatchPredictor` model-version source to artifact filename.
2. Compute explanations inside `_predict_chunk` and include them in the bulk update payload (add a new bindparam).
3. Update the forecast-side model version naming.
4. Update the metrics stage to read the artifact filename.
5. Validate: run existing predictor-touching tests; run metrics-stage path if a test exists.

### Chunk 4: Match detail integration

**Files:**
- Modify: `src/teelo/web/routers/matches.py`
- Modify: `src/teelo/web/services/feature_display.py`
- Modify: `src/teelo/web/templates/match_detail.html`
- Test: `tests/unit/test_match_detail_feature_importance.py`

Steps:

1. Failing test: route attaches explanation and correct MatchFeatures row to template context.
2. In the router, when `match.prediction_explanation` is present, read `feature_set_name` from it and use that (via `feature_set_id`) to select the `MatchFeatures` row. Fall back to current behavior when absent.
3. Add `feature_display` helpers to merge `contributions` into the pair/group rendering (annotate rows with pp where available).
4. Add the "Why Teelo favors {player}" section to the template. Render top drivers toward A and B (sort-by-`|pp|`), plus an optional group rollup.
5. Graceful degradation note when explanation is missing.
6. Run `pytest tests/unit/test_match_detail_feature_importance.py -v`.

### Chunk 5: Docs and verification

**Files:**
- Create: `docs/prediction-explainability.md`
- Modify: `README.md`

Steps:

1. Document the payload contract (aimed at both UI and content-agent consumers).
2. Document the swap-average nuance and the "attribution, not causal" framing.
3. Document the `prediction_model_version` semantic change.
4. Add the new doc to the README docs map.
5. Run full verification:
   ```bash
   pytest
   ruff check .
   black --check .
   mypy src
   ```
6. Run `alembic upgrade head` against the local dev database to confirm the migration applies cleanly.

## Suggested Helper Shapes

### `src/teelo/ml/versioning.py`

```python
def resolve_model_artifact_name(model_ref: str | None) -> str | None: ...
def load_model_metadata_by_artifact(model_artifact: str) -> dict[str, Any]: ...
def infer_feature_set_name(feature_names: list[str]) -> str: ...  # raises on ambiguity
```

### `src/teelo/ml/explanations.py`

```python
def build_explanation(
    *,
    booster: xgb.Booster,
    feature_names: list[str],
    feature_set_name: str,
    model_artifact: str,
    features: dict[str, Any],
    prediction_a: float,
    registry: FeatureRegistry | None = None,
) -> dict[str, Any]: ...
```

## Testing Notes

- Prefer unit tests with a small synthetic booster (train a 2-3 tree model on fabricated data) rather than loading a real artifact.
- Directly exercise the orientation remap — it is the most failure-prone piece.
- Include a regression test that `sum(contributions.values()) ≈ (prediction_a − 0.5) × 100`.
- Add a route-level test that confirms graceful degradation when `prediction_explanation` is NULL.

## Operational Notes

- Alembic migration is additive and safe to apply while predictions are running.
- No automatic backfill. Historical predictions remain without explanations.
- `prediction_model_version` semantics change from timestamp to artifact filename. Dashboards or queries that depend on this field's format will need updating — this is called out in the final handoff.
- `ModelEvaluationSnapshot.model_version` inherits the same change.

## Follow-Up Work After V1

1. `teelo predictions-backfill-explanations` script for operator-triggered historical coverage.
2. Global feature-importance admin view, reusing `src/teelo/ml/selection.py` output plus the new metadata fields.
3. Extend the same explanation flow to `TournamentForecastNode.features_json`.
4. Explanation-based content templates for the openclaw content agent, once the payload contract is exercised end-to-end.

## Execution Handoff

Plan revised 2026-04-24. Ready to execute chunk by chunk with verification between chunks.
