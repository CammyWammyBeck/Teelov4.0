# Prediction Explainability

Per-match prediction explanations are pre-computed by the hourly prediction
pipeline and stored on `Match.prediction_explanation` (JSONB). Both the public
match detail page and downstream consumers (e.g. content-generation agents)
read this column directly — no model load is required at read time.

## Payload shape

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
    "surface_hard": 0.0
  },
  "paired_rows": [
    {"key": "elo", "display_name": "ELO", "pp": 0.34},
    {"key": "h2h_wins", "display_name": "H2H Wins", "pp": 1.05}
  ],
  "groups": [
    {"key": "elo_core", "display_name": "ELO Ratings", "pp": 0.34},
    {"key": "h2h", "display_name": "Head to Head", "pp": 1.05}
  ]
}
```

Field contract:

| Field | Meaning |
| --- | --- |
| `model_artifact` | Filename of the XGBoost model that produced the prediction (e.g. `prediction_v17.json`). Same value as `Match.prediction_model_version` going forward. |
| `feature_set_name` | Name of the `FeatureSet` the model consumes. Use this to pick the correct `MatchFeatures` snapshot when rendering. |
| `base_prob` | Baseline probability the attribution is measured against. Always `0.5` in v1. |
| `prediction_a` | The probability of Player A winning that was actually stored on the match. Repeated here so consumers don't need to join. |
| `contributions` | Map of raw feature column name → percentage-point contribution toward `P(A wins)`. Keys match the model's `feature_names`. |
| `paired_rows` | Convenience pre-aggregation. `_a`/`_b` and `h2h_a_*`/`h2h_b_*` pairs are collapsed into one row each. `pp` is the sum of the pair's component contributions. Sorted by `|pp|` descending. |
| `groups` | Group-level rollups using the feature registry's group definitions. Same `pp` units. Sorted by `|pp|` descending. |

## What `pp` means

`pp` stands for **percentage points toward Player A**. A `pp` of `+3.2` means
the feature contributed +3.2 percentage points to the `P(A wins)` the model
ended up predicting. A negative value means the feature pushed toward Player B.

By construction,

```
sum(contributions.values()) == (prediction_a - base_prob) * 100
```

up to float epsilon. So if `prediction_a = 0.64`, the per-feature `pp` values
sum to `+14.0`. If `prediction_a = 0.40`, they sum to `-10.0`.

`paired_rows` sums to the same total; `groups` sums to the same total (every
contribution is accounted for in exactly one group as long as the feature
registry covers every column, which it does for the shipped feature sets).

## What `pp` does NOT mean

- **Not a causal effect.** Dropping this feature from the model will not move
  the probability by the shown `pp`. The attribution is a local proportional
  decomposition of the model's current output, not a counterfactual.
- **Not a probability of anything.** A `pp` of `+3.2` is a slice of the
  probability difference between the prediction and 50%, not a standalone
  probability.

## Computation

Contributions are computed at pipeline time from the same swap-averaged
orientation policy as the prediction itself. Full detail:

1. For each match, run the booster twice — once on the original feature vector,
   once on the A/B-swapped vector produced by
   `teelo.ml.randomize.swap_ab_features`.
2. Call `booster.predict(dmat, pred_contribs=True)` to get per-column margin
   contributions for each orientation. Drop the XGBoost bias column.
3. Remap the swap-orientation contributions back to original A/B semantics:
   swap paired columns (`elo_a` <-> `elo_b`, `h2h_a_*` <-> `h2h_b_*`), then
   sign-flip every column (because swap contribs point toward original Player
   B).
4. Average the two remapped margin-contribution vectors.
5. Scale each feature's averaged contribution so that the per-row sum equals
   `(prediction_a - base_prob) * 100`:

   ```
   pp_i = c_i * (target / sum(c_j))
   ```

   where `target = (prediction_a - 0.5) * 100`. When the signed sum of
   contributions is near zero (model is at baseline margin), `target` is also
   near zero and `pp_i` is set to 0 to avoid a numerically unstable scale
   factor.

See `src/teelo/ml/explanations.py` for the implementation.

## Consumer guide — match detail page

The router reads `Match.prediction_explanation`, picks the matching
`MatchFeatures` snapshot via `feature_set_name`, and passes the payload to the
template. The template renders a "Key Factors" section showing top drivers
toward each player, plus an optional group-rollup summary. Each row in the
existing feature comparison table is annotated with its `pp` value via
`build_row_pp_lookup` from `src/teelo/web/services/feature_display.py`.

## Consumer guide — content agents

The content agent should read the JSONB column directly. Typical uses:

- **Narrative lead:** filter `paired_rows` by `pp > 0` vs `pp < 0`, sort by
  magnitude, and synthesise one sentence per top driver.
- **Grouped summary:** read `groups` and describe clusters of evidence
  (e.g. "ELO advantage, cushioned by similar form").
- **Custom filtering:** walk `contributions` directly when the agent wants to
  reason about long-tail features the pre-aggregations drop.

No model load or feature registry access is needed.

## Legacy predictions

Predictions written before this change have `prediction_explanation IS NULL`
and a `prediction_model_version` in ISO timestamp format. The match detail
page degrades gracefully — it still renders the raw feature table without a
Key Factors section. No automatic backfill is provided in v1; operators can
write a script to rerun predictions for historical matches if full coverage
is required.

For lookups that need to map a legacy timestamp-style
`prediction_model_version` back to its artifact filename, use
`teelo.ml.versioning.resolve_model_artifact_name()`.

## Operational notes

- `Match.prediction_model_version` changed meaning in this release. Forward-
  written rows store the artifact filename (e.g. `prediction_v17.json`);
  historical rows keep their timestamp value. `ModelEvaluationSnapshot.model_version`
  follows the same convention going forward, driven by the metrics stage in
  `scripts/run_hourly_update.py`.
- The Alembic migration adding `prediction_explanation` is additive and safe
  to apply while the prediction pipeline is running.
- Storing explanations adds a negligible cost per chunk — `pred_contribs=True`
  is a single extra tree walk on a DMatrix that already exists.
