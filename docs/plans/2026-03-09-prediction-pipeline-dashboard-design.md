# Prediction Pipeline & Dashboard Design

Date: 2026-03-09

## Goal

Wire ML predictions into the hourly pipeline and build a comprehensive prediction tracking dashboard behind admin auth, with plans to open it to all users in future.

## Pipeline Integration

Add two new stages to `run_hourly_update.py`, running after ELO updates:

1. **`predictions`** — computes features for upcoming/scheduled matches missing them, then runs `BatchPredictor` with the latest model (auto-downloaded from S3).
2. **`metrics_snapshot`** — recomputes aggregate prediction metrics and upserts today's snapshot row in `model_evaluation_snapshots`.

Pipeline order:
1. `current_events_ingest` (existing)
2. `elo_incremental` (existing)
3. `player_enrichment_incremental` (existing)
4. `predictions` (new)
5. `metrics_snapshot` (new)

Guard: results ingestion must not overwrite `prediction_a`, `prediction_model_version`, or `prediction_updated_at` on completed matches.

## Data Model Changes

### New column on `matches`

- `prediction_source` (`String(20)`, nullable) — `"live"` (predicted before match played) or `"backfill"` (retroactive prediction on historical matches).

### New table: `model_evaluation_snapshots`

| Column | Type | Notes |
|--------|------|-------|
| id | Integer PK | |
| model_version | String(50) | e.g. "prediction_v3" |
| snapshot_date | Date | one row per day per model per source |
| source_filter | String(20) | "live", "backfill", "all" |
| metrics | JSONB | accuracy, log_loss, brier_score, ece, n_matches, per-tour/surface/level/round/ranking-tier/confidence-bucket breakdowns |
| computed_at | DateTime | |

Unique constraint on `(model_version, snapshot_date, source_filter)`.

## Dashboard Design

### Page: `/admin/predictions`

Protected by `_require_admin`. Linked from admin home panel.

### Header Cards

- Overall accuracy, log_loss, ECE (live predictions)
- Total predictions made / total resulted
- Change vs last week (delta with up/down arrows)

### Tabs

1. **Live Predictions** — predictions made before matches happened (gold standard, guaranteed no leakage)
2. **Historical (Backfill)** — model evaluated retroactively on completed matches
3. **Training CV** — model metadata: CV fold scores, hyperparameters, training date range

### Metrics Table (tabs 1 & 2)

Breakdown rows by: tour, surface, tournament level, round, player ranking tier, confidence bucket.

Columns: n_matches, accuracy, log_loss, brier_score.

### Charts (Chart.js, tabs 1 & 2)

- **Accuracy over time** — daily line chart with rolling 7-day and 30-day smoothed lines
- **Calibration curve** — predicted probability (x-axis) vs actual win rate (y-axis), ideal diagonal shown
- **Confidence distribution** — histogram of prediction probabilities
- **Per-tour accuracy over time** — multi-line chart

### Filters

- Date range picker
- Tour multi-select
- Surface multi-select
- Tournament level multi-select
- Round multi-select
- Confidence range slider
- Rolling window selector (7d, 30d, 90d, all)

### API Endpoints

All under `/admin/api/predictions/`:

- `GET /summary` — header card data
- `GET /breakdown` — filtered metrics table data
- `GET /charts/accuracy` — time series data
- `GET /charts/calibration` — calibration curve data
- `GET /charts/distribution` — confidence histogram data

## Backfill Strategy

One-time script `scripts/backfill_predictions.py`:
- Runs `BatchPredictor` against completed matches that have features but no prediction
- Sets `prediction_source = "backfill"`
- Run once after deployment; re-run if model is retrained

Predictor changes:
- Add `--backfill` flag to target completed matches instead of upcoming/scheduled
- Set `prediction_source` appropriately ("live" for normal runs, "backfill" for backfill)

## Files to Create

- `src/teelo/ml/metrics.py` — metrics computation logic for snapshots
- `src/teelo/web/routers/admin.py` — prediction dashboard routes + API endpoints (extend existing)
- `src/teelo/web/templates/admin_predictions.html` — dashboard template
- `src/teelo/web/static/js/admin_predictions.js` — Chart.js charts + filter logic
- `scripts/backfill_predictions.py` — one-time backfill script
- Alembic migration for new column + table

## Files to Modify

- `src/teelo/db/models.py` — add `prediction_source` to Match, new `ModelEvaluationSnapshot` model
- `src/teelo/ml/predictor.py` — add backfill mode, set `prediction_source`
- `scripts/run_hourly_update.py` — add `predictions` and `metrics_snapshot` stages
- `src/teelo/web/templates/admin_home.html` — add prediction dashboard card
- `src/teelo/services/results_ingestion.py` — guard prediction fields

## Dependencies

- Chart.js via CDN

## Implementation Order

1. DB migration (new column + table)
2. Pipeline integration (predictions + metrics stages)
3. Backfill script + predictor changes
4. Metrics computation module
5. API endpoints
6. Dashboard template + JS
