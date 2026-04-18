# Prediction Feature Importance Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Explain each individual match prediction on the public match detail page by surfacing the features that most pushed the model toward Player A or Player B.

**Architecture:** Add exact model-artifact traceability to predictions and metadata, compute local XGBoost contribution scores from the exact artifact using the same swap-averaged logic as live prediction, aggregate raw contributions into the existing paired and diff feature display rows, and render a new explanation summary above the current raw feature breakdown. Keep v1 on-demand rather than persisting explanation blobs.

**Tech Stack:** Python 3.11, FastAPI, SQLAlchemy, XGBoost, Jinja, existing `MatchFeatures` JSONB feature store.

---

## Background

The repo already has:

- per-match stored feature snapshots in `src/teelo/db/models.py` via `MatchFeatures.features`
- batch prediction writes in `src/teelo/ml/predictor.py`
- a public match detail page that loads feature data in `src/teelo/web/routers/matches.py`
- a grouped raw feature display in `src/teelo/web/services/feature_display.py` and `src/teelo/web/templates/match_detail.html`
- offline global feature selection and gain ranking in `src/teelo/ml/selection.py`

The missing piece is per-prediction explainability. The requested first version should answer: for this exact prediction, which features mattered most?

## Decisions Captured During Planning

- First version should prioritize per-match local explanation, not global-only importance.
- First surface should be the public match detail page.
- Explanations must be tied to the exact model artifact that produced the stored prediction.
- V1 should avoid a new `shap` dependency and instead use XGBoost native contribution output.
- V1 should not persist explanation blobs unless on-demand performance later proves unacceptable.

## Recommended Approach

Use native XGBoost local contribution scores computed on demand from the same model artifact that produced the prediction.

Why this approach:

- smallest correct change for the current architecture
- no schema migration required for v1
- reuses existing `MatchFeatures` storage and the existing match detail UI
- avoids a new explainability dependency
- can be made exact if prediction rows reference the actual model artifact name

## Alternatives Considered

### 1. Persist explanations when predictions are generated

Pros:

- exact read-time output
- fast page loads

Cons:

- requires schema/storage work
- likely requires backfill if we want historical coverage
- more operational complexity than needed for v1

### 2. Expose only global feature importance in admin

Pros:

- easiest implementation because `src/teelo/ml/selection.py` already has ranking output

Cons:

- does not answer why a specific match prediction favors one player
- does not satisfy the chosen first-version scope

### 3. Add SHAP as a new dependency

Pros:

- well-known explainability library

Cons:

- new dependency and extra complexity
- unnecessary for a tree-based XGBoost model when native contribution output is available

## Constraints And Risks

- `Match.prediction_model_version` currently stores `metadata["created_at"]` rather than the model artifact filename in `src/teelo/ml/predictor.py`.
- trainer metadata currently does not persist `feature_set_name` or `model_artifact` in `src/teelo/ml/trainer.py`.
- local contribution values are naturally additive on model margin/log-odds, not directly on final averaged probability.
- current prediction output is a swap-average of `P(A wins)` and `1 - P(swapped wins)`, so explanation logic must mirror that orientation handling.
- legacy predictions may not always be perfectly reconstructable if older metadata is incomplete.

## Scope

### In Scope

- exact model-artifact traceability for new predictions
- best-effort legacy artifact resolution for old predictions
- per-match explanation service for existing match predictions
- public match detail page summary of top positive and negative drivers
- tests for explanation alignment, legacy handling, and UI data plumbing
- documentation for semantics and limitations

### Out Of Scope

- admin dashboard for global importance in v1
- persisted explanation JSON in the database
- new prediction APIs for list pages
- tournament forecast explanation support in v1

## File Map

### Create

- `src/teelo/ml/explanations.py`
- `tests/unit/test_prediction_explanations.py`
- `tests/unit/test_model_version_resolution.py`
- `tests/unit/test_match_detail_feature_importance.py`
- `docs/prediction-explainability.md`

### Modify

- `src/teelo/ml/trainer.py`
- `src/teelo/ml/predictor.py`
- `src/teelo/services/forecast_prediction.py`
- `src/teelo/ml/versioning.py`
- `src/teelo/web/routers/matches.py`
- `src/teelo/web/services/feature_display.py`
- `src/teelo/web/templates/match_detail.html`
- `README.md`

## Target Behavior

When a user opens `/matches/{match_id}` for a match with a stored prediction:

1. Load the exact model artifact that produced the prediction.
2. Load the matching feature snapshot for that model's feature set.
3. Compute local feature contributions using the same orientation policy as live prediction.
4. Aggregate contributions into human-readable rows and groups.
5. Show:
   - why the model favors Player A
   - what pushes back toward Player B
   - optional group totals
6. Keep the current raw feature comparison table below the new explanation summary.

If exact reconstruction is unavailable, the page should degrade gracefully and still show the raw feature table.

## Data Contracts

### Model metadata to save going forward

```json
{
  "model_artifact": "prediction_v17.json",
  "feature_set_name": "baseline_v2",
  "feature_names": ["elo_a", "elo_b", "elo_diff"],
  "params": {},
  "cv_scores": {},
  "train_size": 123456,
  "date_range": "2015-2025",
  "created_at": "2026-04-18T12:34:56+00:00"
}
```

### Explanation payload returned by the explanation layer

```json
{
  "model_artifact": "prediction_v17.json",
  "feature_set_name": "baseline_v2",
  "prediction_a": 0.6412,
  "favored_side": "a",
  "top_for_a": [
    {"key": "elo", "display_name": "ELO", "impact": 0.83}
  ],
  "top_for_b": [
    {"key": "surface_win_rate", "display_name": "Surface Win Rate", "impact": -0.41}
  ],
  "groups": [
    {"key": "elo_core", "display_name": "ELO Ratings", "impact": 1.12}
  ]
}
```

The `impact` value should be documented and labeled as model contribution, not causal effect and not literal probability-point swing.

## Explanation Method

Use the trained XGBoost model's native contribution output.

Implementation notes:

- use the exact artifact identified for the stored prediction
- load the exact ordered `feature_names` from model metadata
- build `X_orig` from the stored feature dict
- build `X_swap` using the same `swap_ab_features()` helper already used by prediction
- compute contribution scores for both orientations
- map swapped-orientation contributions back to original A/B semantics
- flip swapped contribution signs so they represent contribution toward original Player A rather than toward swapped Player A
- average original and remapped-swapped contributions to match the swap-averaged prediction policy

Important limitation:

- contributions will be additive on model margin, not directly on final probability
- the UI copy must say these features pushed the model toward one side or the other

## Legacy Compatibility Strategy

New predictions should become exact and straightforward once artifact names and feature set names are saved consistently.

Older predictions need a best-effort path:

1. If `prediction_model_version` already looks like `prediction_vN.json`, use it directly.
2. If it looks like an ISO timestamp, scan available metadata files for matching `created_at`.
3. If resolved metadata lacks `feature_set_name`, infer it by comparing `metadata.feature_names` against `FeatureSet.feature_definitions` keys.
4. If artifact or feature set still cannot be resolved, surface `explanation unavailable for this legacy prediction` and keep the page otherwise functional.

## Chunks

## Chunk 1: Model Traceability

### Task 1: Make prediction rows reproducible to the exact model artifact

**Files:**
- Modify: `src/teelo/ml/trainer.py`
- Modify: `src/teelo/ml/predictor.py`
- Modify: `src/teelo/services/forecast_prediction.py`
- Modify: `src/teelo/ml/versioning.py`
- Test: `tests/unit/test_model_version_resolution.py`

- [ ] **Step 1: Write the failing test for saved metadata fields**

Add a test that verifies trainer metadata now includes both `model_artifact` and `feature_set_name`.

- [ ] **Step 2: Write the failing test for legacy model reference resolution**

Add a test that verifies an old timestamp-style `prediction_model_version` can be mapped back to an artifact name using model metadata.

- [ ] **Step 3: Update trainer metadata output**

Modify `ModelTrainer._save()` in `src/teelo/ml/trainer.py` so the saved metadata includes the artifact filename and feature set name.

Suggested shape:

```python
metadata = {
    "model_artifact": output.name,
    "feature_set_name": self.feature_set_name,
    "feature_names": self.feature_names,
    "params": params,
    "cv_scores": self.cv_scores,
    "train_size": int(len(y)),
    "date_range": f"{min_year}-{max_year}",
    "created_at": datetime.now(timezone.utc).isoformat(),
}
```

- [ ] **Step 4: Standardize future prediction writes**

Modify `BatchPredictor.predict()` so `prediction_model_version` stores `prediction_vN.json` rather than `created_at`.

- [ ] **Step 5: Standardize forecast-side model references**

Modify `src/teelo/services/forecast_prediction.py` to use the same artifact naming convention so forecast-side code stays aligned.

- [ ] **Step 6: Add a model reference resolver**

Add helpers in `src/teelo/ml/versioning.py` to:

- accept direct artifact names
- resolve legacy timestamp values to artifact names
- cache metadata scans to avoid repeated file and S3 work

- [ ] **Step 7: Add feature-set inference for legacy artifacts**

If metadata lacks `feature_set_name`, infer it by matching metadata feature names to a `FeatureSet` definition in the database.

- [ ] **Step 8: Run the targeted tests**

Run:

```bash
pytest tests/unit/test_model_version_resolution.py -v
```

Expected: PASS

## Chunk 2: Explanation Engine

### Task 2: Add exact per-prediction local explanation logic

**Files:**
- Create: `src/teelo/ml/explanations.py`
- Test: `tests/unit/test_prediction_explanations.py`

- [ ] **Step 1: Write the failing tests for contribution alignment**

Cover:

- explanation uses the same swap-average policy as prediction
- swapped contributions are remapped back into original A/B semantics
- bias or base-value terms are excluded from top-feature ranking
- paired, diff, and neutral features are aggregated consistently

- [ ] **Step 2: Add an explanation model loader**

Create a small loader that returns:

- the exact loaded model or booster
- `feature_names`
- `feature_set_name`
- `model_artifact`

- [ ] **Step 3: Build the original and swapped feature matrices**

Use the stored feature dict plus the existing `swap_ab_features()` helper so explanation input matches prediction input.

- [ ] **Step 4: Compute local XGBoost contributions**

Use XGBoost native contribution output from the exact artifact for both orientations.

Implementation target:

```python
booster = model.get_booster()
contribs = booster.predict(dmatrix, pred_contribs=True)
```

- [ ] **Step 5: Remap swapped contributions back to original semantics**

Mirror the logic already present in feature-side swapping helpers:

- suffix `_a` and `_b` pairs swap
- infix `_a_` and `_b_` pairs swap
- `diff` features invert sign
- dominance-style features use the same perspective conversion policy as feature swapping

- [ ] **Step 6: Convert swapped orientation from B-perspective to A-perspective**

Negate remapped swapped contributions before averaging so the result explains the displayed `P(A wins)`.

- [ ] **Step 7: Aggregate column impacts into display concepts**

Aggregate to the same concepts the match detail page already understands:

- paired rows such as `elo_a` and `elo_b` collapse into one display row
- single and diff rows remain single concepts
- group impact equals the sum of the group's row impacts

- [ ] **Step 8: Return a display-ready explanation payload**

Include:

- favored side
- top features for Player A
- top features for Player B
- optional group totals
- model artifact and feature set identifiers for debugging

- [ ] **Step 9: Run the targeted tests**

Run:

```bash
pytest tests/unit/test_prediction_explanations.py -v
```

Expected: PASS

## Chunk 3: Match Detail Integration

### Task 3: Show explanations on the match detail page

**Files:**
- Modify: `src/teelo/web/routers/matches.py`
- Modify: `src/teelo/web/services/feature_display.py`
- Modify: `src/teelo/web/templates/match_detail.html`
- Test: `tests/unit/test_match_detail_feature_importance.py`

- [ ] **Step 1: Write the failing route and template data test**

Verify that the route can attach explanation data to the template context for a match with a stored prediction.

- [ ] **Step 2: Load the correct feature snapshot for explanation**

Stop assuming the newest `MatchFeatures` row is always correct. Use the explanation model metadata's `feature_set_name` to select the correct `MatchFeatures` row for that prediction.

- [ ] **Step 3: Keep the current raw breakdown intact**

Do not replace the existing feature comparison table. Add the explanation summary above it.

- [ ] **Step 4: Extend feature display helpers only where needed**

Add small helper support in `src/teelo/web/services/feature_display.py` so the template can render contribution rows or group impacts without rewriting the existing feature table model.

- [ ] **Step 5: Add the explanation summary UI**

Render a new section in `src/teelo/web/templates/match_detail.html` showing:

- `Why Teelo favors <player>`
- top 3 to 5 drivers toward Player A
- top 3 to 5 drivers toward Player B
- optional group-level summary if it improves readability
- a short note that these are model contributions, not guarantees

- [ ] **Step 6: Add graceful degradation for missing explanations**

If artifact resolution, metadata inference, or feature lookup fails, show a small non-fatal note such as `Explanation unavailable for this legacy prediction`.

- [ ] **Step 7: Run the targeted tests**

Run:

```bash
pytest tests/unit/test_match_detail_feature_importance.py -v
```

Expected: PASS

## Chunk 4: Docs And Verification

### Task 4: Document the semantics and verify the work

**Files:**
- Create: `docs/prediction-explainability.md`
- Modify: `README.md`

- [ ] **Step 1: Document what the explanation means**

Explain that the new output represents local model contribution for a single prediction, not causal truth and not a guaranteed match outcome.

- [ ] **Step 2: Document the swap-average nuance**

Explain that predictions and explanations are both built from the same A/B orientation-robust inference policy.

- [ ] **Step 3: Document legacy behavior**

Explain:

- new predictions are fully traceable by artifact name
- old predictions are best-effort explainable
- operators can run prediction backfill later if full historical explainability is required

- [ ] **Step 4: Update the top-level docs map**

Add the new explainability doc to `README.md`.

- [ ] **Step 5: Run the targeted verification set**

Run:

```bash
pytest tests/unit/test_model_version_resolution.py tests/unit/test_prediction_explanations.py tests/unit/test_match_detail_feature_importance.py -v
```

Expected: PASS

- [ ] **Step 6: Run full verification**

Run:

```bash
pytest
ruff check .
black --check .
mypy src
```

Expected: PASS

## Suggested Helper Shapes

These are not mandatory names, but the implementation should stay similarly small and focused.

### `src/teelo/ml/versioning.py`

Suggested additions:

```python
def resolve_model_artifact_name(model_ref: str | None) -> str | None:
    ...

def load_model_metadata_by_artifact(model_artifact: str) -> dict[str, Any]:
    ...

def infer_feature_set_name(feature_names: list[str]) -> str | None:
    ...
```

### `src/teelo/ml/explanations.py`

Suggested additions:

```python
def explain_match_prediction(
    *,
    prediction_model_ref: str | None,
    prediction_a: float,
    features: dict[str, Any],
) -> dict[str, Any] | None:
    ...
```

Keep the first version in one module unless reuse pressure clearly justifies a split.

## Testing Notes

- prefer unit tests with small synthetic feature dicts and stub metadata
- do not rely on a real trained model file in tests if a small generated model or controlled stub can verify the mapping behavior
- directly test the orientation remapping logic because it is the most failure-prone part
- add at least one route-level test that confirms graceful degradation when explanation data is unavailable

## Operational Notes

- no Alembic migration is required for the recommended v1
- no automatic prediction backfill should run as part of the implementation
- if exact historical coverage is desired, operators can later run `teelo predictions-backfill` after deployment and validation
- changing `prediction_model_version` semantics from timestamp-like values to artifact names should be called out in the final implementation summary and docs

## Follow-Up Work After V1

1. Add a global model-importance view in admin using `src/teelo/ml/selection.py` output plus saved model metadata.
2. Persist compact explanation summaries if public traffic makes on-demand explanation too expensive.
3. Extend the same explanation flow to tournament forecast nodes stored in `TournamentForecastNode.features_json`.

## Execution Handoff

Plan complete and saved to `docs/plans/2026-04-18-prediction-feature-importance-implementation-plan.md`. Ready to execute once reviewed.
