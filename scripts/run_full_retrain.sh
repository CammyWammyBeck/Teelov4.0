#!/usr/bin/env bash
#
# Full retrain pipeline: backfill features → train → evaluate → feature selection
# Run from the project root with venv activated:
#   bash scripts/run_full_retrain.sh 2>&1 | tee retrain.log
#
set -euo pipefail

PRESET="baseline_v2"
HOLDOUT_YEAR=2025

echo "============================================"
echo "Step 1/4: Feature backfill (preset=$PRESET)"
echo "============================================"
python -m teelo.features.engine --backfill --preset "$PRESET"

echo ""
echo "============================================"
echo "Step 2/4: Train model (feature_set=$PRESET)"
echo "============================================"
python -m teelo.ml.trainer --feature-set "$PRESET"

echo ""
echo "============================================"
echo "Step 3/4: Evaluate (holdout_year=$HOLDOUT_YEAR)"
echo "============================================"
python -m teelo.ml.evaluator --feature-set "$PRESET" --holdout-year "$HOLDOUT_YEAR"

echo ""
echo "============================================"
echo "Step 4/4: Feature selection"
echo "============================================"
python -m teelo.ml.selection --feature-set "$PRESET"

echo ""
echo "============================================"
echo "Done. Check:"
echo "  - Evaluation output above for accuracy/calibration"
echo "  - models/feature_selection_report.json for feature rankings"
echo "============================================"
