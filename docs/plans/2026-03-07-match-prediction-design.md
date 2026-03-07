# Match Prediction System Design

**Date**: 2026-03-07
**Status**: Approved for implementation

## Overview

XGBoost/LightGBM model predicting tennis match outcomes using ~112 candidate features derived from ELO ratings, form, head-to-head records, and match context. Features computed via chronological pass over ~1M matches and stored per-match in `MatchFeatures` JSONB for historical visibility.

## Key Decisions

- **Framework**: XGBoost/LightGBM (gradient-boosted trees)
- **Feature approach**: Systematic search — broad candidate set, pruned via importance/ablation
- **Training scope**: Single model, tour/gender/level as categorical features
- **Feature computation**: Python chronological pass maintaining per-player running state
- **Feature storage**: Every match stores its feature snapshot (historical + upcoming)
- **Pipeline trigger**: Batch after ELO update
- **Excluded**: Serve/return stats (sparse in historical data), betting odds comparison

## Architecture

```
ELO Update → Feature Engine (chronological pass) → MatchFeatures table
                                                         ↓
                                              ML Model (XGBoost)
                                                         ↓
                                              Match.prediction_a
```

## Feature Groups (~112 candidates)

1. **Context** (~20): surface, level, round, tour, year one-hots
2. **ELO** (~20): overall/surface ELO, peak, momentum, variance
3. **Form** (~40): win rates over 8 time windows, surface/level-specific
4. **H2H** (~12): all-time, recent, surface/level-specific head-to-head
5. **Activity** (~16): days since last match, match counts, fatigue, seeding

## Training

- Time-series expanding window CV (no future leakage)
- Optuna hyperparameter optimization
- Feature selection via importance ranking + cumulative addition + ablation
