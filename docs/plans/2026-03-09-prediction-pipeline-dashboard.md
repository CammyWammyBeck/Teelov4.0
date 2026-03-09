# Prediction Pipeline & Dashboard Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Wire ML predictions into the hourly pipeline and build a comprehensive prediction tracking dashboard behind admin auth.

**Architecture:** Add two pipeline stages (predictions + metrics snapshot), a new `prediction_source` column on Match, a `ModelEvaluationSnapshot` table for pre-computed dashboard metrics, and a full admin dashboard page with Chart.js visualizations. API endpoints serve filtered metrics data; the dashboard uses vanilla JS to render charts and handle filters.

**Tech Stack:** Python/FastAPI, SQLAlchemy, Alembic, Jinja2, Chart.js (CDN), vanilla JS, Tailwind CSS.

---

### Task 1: Database Migration — prediction_source column + ModelEvaluationSnapshot table

**Files:**
- Modify: `src/teelo/db/models.py:607-609` (add prediction_source after prediction fields)
- Modify: `src/teelo/db/models.py:~995` (add ModelEvaluationSnapshot class)
- Create: `alembic/versions/YYYYMMDD_HHMMSS_add_prediction_tracking.py` (auto-generated)

**Step 1: Add `prediction_source` to Match model**

In `src/teelo/db/models.py`, after line 609 (`prediction_updated_at`), add:

```python
    prediction_source: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
```

**Step 2: Add ModelEvaluationSnapshot model**

In `src/teelo/db/models.py`, after the `MatchFeatures` class (after line ~995), add:

```python
class ModelEvaluationSnapshot(Base):
    """Daily snapshot of prediction accuracy metrics."""
    __tablename__ = "model_evaluation_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True)
    model_version: Mapped[str] = mapped_column(String(50), nullable=False)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    source_filter: Mapped[str] = mapped_column(String(20), nullable=False)  # "live", "backfill", "all"
    metrics: Mapped[dict] = mapped_column(JSONB, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("model_version", "snapshot_date", "source_filter", name="uq_eval_snapshot"),
        Index("idx_eval_snapshot_date", "snapshot_date"),
    )
```

Ensure `date` is imported from `datetime` at top of file, and `Date` from `sqlalchemy`.

**Step 3: Generate and apply migration**

Run:
```bash
alembic revision --autogenerate -m "add prediction tracking"
alembic upgrade head
```

**Step 4: Commit**

```bash
git add src/teelo/db/models.py alembic/versions/
git commit -m "feat: add prediction_source column and ModelEvaluationSnapshot table"
```

---

### Task 2: Update BatchPredictor — prediction_source + backfill mode

**Files:**
- Modify: `src/teelo/ml/predictor.py`

**Step 1: Add prediction_source to predict() and add backfill mode**

Update `__init__` to accept `backfill` flag:

```python
class BatchPredictor:
    def __init__(self, model_path: str | None = None, feature_set_name: str | None = None, backfill: bool = False) -> None:
        self.model_path = model_path or latest_model_path()
        self.feature_set_name = feature_set_name or latest_feature_set()
        self.backfill = backfill
```

Import `latest_model_path` and `latest_feature_set` from `teelo.ml.versioning`.

In `predict()`, change the status filter (line 60):

```python
            if self.backfill:
                status_filter = Match.status.in_(("completed", "retired", "walkover", "default"))
                # Only backfill matches that don't already have a prediction
                extra_filter = Match.prediction_a.is_(None)
            else:
                status_filter = Match.status.in_(("upcoming", "scheduled"))
                extra_filter = True  # no extra filter

            stmt = (
                select(Match.id, MatchFeatures.features)
                .select_from(Match)
                .join(MatchFeatures, MatchFeatures.match_id == Match.id)
                .where(MatchFeatures.feature_set_id == feature_set.id)
                .where(status_filter)
                .where(extra_filter)
                .order_by(Match.id.asc())
            )
```

In the payloads section (line 74), add `prediction_source`:

```python
            source = "backfill" if self.backfill else "live"
            now = datetime.utcnow()
            payloads = [
                {
                    "b_match_id": row.id,
                    "b_prediction_a": float(prob),
                    "b_prediction_model_version": model_version,
                    "b_prediction_updated_at": now,
                    "b_prediction_source": source,
                }
                for row, prob in zip(rows, probs)
            ]
```

Add `prediction_source` to the update statement values:

```python
            update_stmt = (
                MatchModel.__table__.update()
                .where(MatchModel.__table__.c.id == bindparam("b_match_id"))
                .values(
                    prediction_a=bindparam("b_prediction_a"),
                    prediction_model_version=bindparam("b_prediction_model_version"),
                    prediction_updated_at=bindparam("b_prediction_updated_at"),
                    prediction_source=bindparam("b_prediction_source"),
                )
            )
```

Update the `__main__` block to support `--backfill`:

```python
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=None)
    parser.add_argument("--feature-set", default=None)
    parser.add_argument("--backfill", action="store_true")
    args = parser.parse_args()
    predictor = BatchPredictor(args.model, args.feature_set, backfill=args.backfill)
    count = predictor.predict()
    print(f"Predicted {count} matches")
```

**Step 2: Commit**

```bash
git add src/teelo/ml/predictor.py
git commit -m "feat: add prediction_source and backfill mode to BatchPredictor"
```

---

### Task 3: Guard prediction fields in results ingestion

**Files:**
- Modify: `src/teelo/services/results_ingestion.py:581-644`

**Step 1: Verify and add guard comment**

The `_update_match_with_result` function (line 581) currently does NOT touch prediction fields — confirmed in research. Add an explicit comment as documentation/guard at the end of the function (before `match.update_temporal_order`):

```python
    # NOTE: Do NOT overwrite prediction_a, prediction_model_version,
    # prediction_updated_at, or prediction_source here. These must be
    # preserved for live prediction accuracy tracking.
```

**Step 2: Commit**

```bash
git add src/teelo/services/results_ingestion.py
git commit -m "docs: add guard comment preserving prediction fields in results ingestion"
```

---

### Task 4: Metrics computation module

**Files:**
- Create: `src/teelo/ml/metrics.py`

**Step 1: Create metrics computation module**

This module computes prediction accuracy metrics from the database and upserts snapshots.

```python
"""Compute and store prediction accuracy metrics."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

import numpy as np
import structlog
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from teelo.db.models import (
    Match, ModelEvaluationSnapshot, Tournament, TournamentEdition,
)
from teelo.db.session import get_session

logger = structlog.get_logger(__name__)

TERMINAL_STATUSES = ("completed", "retired", "walkover", "default")

# Confidence buckets for calibration
CONFIDENCE_BUCKETS = [
    (0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.0),
]

RANKING_TIERS = [
    ("top_10", 1, 10),
    ("top_50", 11, 50),
    ("top_100", 51, 100),
    ("outside_100", 101, None),
]


def compute_snapshot(
    model_version: str | None = None,
    snapshot_dt: date | None = None,
    source_filter: str = "all",
) -> dict[str, Any]:
    """Compute prediction metrics and upsert snapshot row."""
    snapshot_dt = snapshot_dt or date.today()

    with get_session() as session:
        # Base query: matches with predictions that have completed
        stmt = (
            select(
                Match.id,
                Match.prediction_a,
                Match.prediction_source,
                Match.winner_id,
                Match.player_a_id,
                Match.player_b_id,
                Match.match_date,
                Match.round,
                Tournament.tour,
                Tournament.level,
                TournamentEdition.surface,
            )
            .select_from(Match)
            .join(TournamentEdition, TournamentEdition.id == Match.tournament_edition_id)
            .join(Tournament, Tournament.id == TournamentEdition.tournament_id)
            .where(Match.status.in_(TERMINAL_STATUSES))
            .where(Match.prediction_a.is_not(None))
            .where(Match.winner_id.is_not(None))
        )

        if source_filter != "all":
            stmt = stmt.where(Match.prediction_source == source_filter)

        if model_version:
            stmt = stmt.where(Match.prediction_model_version == model_version)

        rows = list(session.execute(stmt).all())

    if not rows:
        logger.info("metrics.no_data", source_filter=source_filter)
        return {}

    # Build arrays
    pred_a = np.array([float(r.prediction_a) for r in rows])
    actual = np.array([1 if r.winner_id == r.player_a_id else 0 for r in rows])
    predicted = (pred_a > 0.5).astype(int)

    # Overall metrics
    metrics: dict[str, Any] = {
        "n_matches": len(rows),
        "accuracy": float(np.mean(predicted == actual)),
        "log_loss": float(_log_loss(actual, pred_a)),
        "brier_score": float(np.mean((pred_a - actual) ** 2)),
        "ece": float(_ece(actual, pred_a)),
    }

    # Per-tour breakdown
    metrics["by_tour"] = _breakdown(rows, pred_a, actual, lambda r: r.tour)

    # Per-surface breakdown
    metrics["by_surface"] = _breakdown(rows, pred_a, actual, lambda r: r.surface)

    # Per-level breakdown
    metrics["by_level"] = _breakdown(rows, pred_a, actual, lambda r: r.level)

    # Per-round breakdown
    metrics["by_round"] = _breakdown(rows, pred_a, actual, lambda r: r.round)

    # Per-confidence-bucket breakdown
    metrics["by_confidence"] = _confidence_breakdown(pred_a, actual)

    # Daily accuracy time series (for charts)
    metrics["daily"] = _daily_breakdown(rows, pred_a, actual)

    # Calibration data
    metrics["calibration"] = _calibration_data(pred_a, actual)

    # Upsert snapshot
    _upsert_snapshot(model_version or "unknown", snapshot_dt, source_filter, metrics)

    logger.info(
        "metrics.snapshot_computed",
        source_filter=source_filter,
        n_matches=metrics["n_matches"],
        accuracy=metrics["accuracy"],
    )
    return metrics


def _log_loss(y_true: np.ndarray, y_prob: np.ndarray, eps: float = 1e-15) -> float:
    y_prob = np.clip(y_prob, eps, 1 - eps)
    return -float(np.mean(y_true * np.log(y_prob) + (1 - y_true) * np.log(1 - y_prob)))


def _ece(y_true: np.ndarray, y_prob: np.ndarray, n_bins: int = 10) -> float:
    bins = np.linspace(0, 1, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        mask = (y_prob >= bins[i]) & (y_prob < bins[i + 1])
        if not mask.any():
            continue
        confidence = float(y_prob[mask].mean())
        observed = float(y_true[mask].mean())
        weight = float(mask.sum() / len(y_true))
        ece += abs(confidence - observed) * weight
    return ece


def _breakdown(rows, pred_a, actual, key_fn):
    groups: dict[str, list[int]] = {}
    for i, r in enumerate(rows):
        k = key_fn(r) or "unknown"
        groups.setdefault(k, []).append(i)

    result = {}
    for k, indices in groups.items():
        idx = np.array(indices)
        p = pred_a[idx]
        a = actual[idx]
        pred = (p > 0.5).astype(int)
        result[k] = {
            "n_matches": len(idx),
            "accuracy": float(np.mean(pred == a)),
            "log_loss": float(_log_loss(a, p)),
            "brier_score": float(np.mean((p - a) ** 2)),
        }
    return result


def _confidence_breakdown(pred_a, actual):
    result = {}
    # Use max(pred, 1-pred) as confidence (how confident in either player)
    confidence = np.maximum(pred_a, 1 - pred_a)
    predicted = (pred_a > 0.5).astype(int)
    for lo, hi in CONFIDENCE_BUCKETS:
        mask = (confidence >= lo) & (confidence < hi)
        if not mask.any():
            continue
        label = f"{int(lo*100)}-{int(hi*100)}%"
        p = pred_a[mask]
        a = actual[mask]
        pred = predicted[mask]
        result[label] = {
            "n_matches": int(mask.sum()),
            "accuracy": float(np.mean(pred == a)),
            "avg_confidence": float(confidence[mask].mean()),
            "actual_win_rate": float(a.mean()),
        }
    return result


def _daily_breakdown(rows, pred_a, actual):
    groups: dict[str, list[int]] = {}
    for i, r in enumerate(rows):
        if r.match_date:
            k = str(r.match_date)
            groups.setdefault(k, []).append(i)

    result = {}
    for k, indices in sorted(groups.items()):
        idx = np.array(indices)
        p = pred_a[idx]
        a = actual[idx]
        pred = (p > 0.5).astype(int)
        result[k] = {
            "n_matches": len(idx),
            "accuracy": float(np.mean(pred == a)),
        }
    return result


def _calibration_data(pred_a, actual, n_bins: int = 10):
    bins = np.linspace(0, 1, n_bins + 1)
    result = []
    for i in range(n_bins):
        mask = (pred_a >= bins[i]) & (pred_a < bins[i + 1])
        if not mask.any():
            continue
        result.append({
            "bin_center": float((bins[i] + bins[i + 1]) / 2),
            "predicted": float(pred_a[mask].mean()),
            "actual": float(actual[mask].mean()),
            "count": int(mask.sum()),
        })
    return result


def _upsert_snapshot(model_version: str, snapshot_dt: date, source_filter: str, metrics: dict):
    with get_session() as session:
        stmt = pg_insert(ModelEvaluationSnapshot).values(
            model_version=model_version,
            snapshot_date=snapshot_dt,
            source_filter=source_filter,
            metrics=metrics,
            computed_at=datetime.utcnow(),
        ).on_conflict_do_update(
            constraint="uq_eval_snapshot",
            set_={
                "metrics": metrics,
                "computed_at": datetime.utcnow(),
            },
        )
        session.execute(stmt)
        session.commit()
```

**Step 2: Commit**

```bash
git add src/teelo/ml/metrics.py
git commit -m "feat: add prediction metrics computation module"
```

---

### Task 5: Pipeline integration — predictions + metrics stages

**Files:**
- Modify: `scripts/run_hourly_update.py:240-266`

**Step 1: Add prediction stage runner**

Add import at top of file and create the stage runner function (near the other `_run_*` functions):

```python
async def _run_predictions_stage(ctx: StageContext) -> None:
    """Run batch predictions on upcoming matches."""
    from teelo.ml.predictor import BatchPredictor
    predictor = BatchPredictor()
    count = predictor.predict()
    logger.info("stage.predictions_done", count=count)


async def _run_metrics_snapshot_stage(ctx: StageContext) -> None:
    """Compute and store prediction accuracy metrics."""
    from teelo.ml.metrics import compute_snapshot
    from teelo.ml.versioning import latest_model_path
    from pathlib import Path
    import json

    model_path = latest_model_path()
    meta_path = Path(f"{model_path}_meta.json")
    model_version = Path(model_path).stem  # e.g. "prediction_v3"
    if meta_path.exists():
        with open(meta_path) as f:
            meta = json.load(f)
            model_version = meta.get("created_at", model_version)

    for source in ("live", "backfill", "all"):
        compute_snapshot(model_version=model_version, source_filter=source)
    logger.info("stage.metrics_snapshot_done")
```

**Step 2: Register stages in `_build_registry()`**

After the `player_enrichment_incremental` registration (line 265), add:

```python
    registry.register(
        StageDefinition(
            name="predictions",
            runner=_run_predictions_stage,
            description="Run ML predictions on upcoming/scheduled matches.",
            enabled_by_default=True,
        )
    )
    registry.register(
        StageDefinition(
            name="metrics_snapshot",
            runner=_run_metrics_snapshot_stage,
            description="Compute and store prediction accuracy metric snapshots.",
            enabled_by_default=True,
        )
    )
```

**Step 3: Commit**

```bash
git add scripts/run_hourly_update.py
git commit -m "feat: add predictions and metrics_snapshot stages to hourly pipeline"
```

---

### Task 6: Backfill script

**Files:**
- Create: `scripts/backfill_predictions.py`

**Step 1: Create backfill script**

```python
"""One-time script to backfill predictions on historical completed matches."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from teelo.ml.predictor import BatchPredictor
from teelo.ml.metrics import compute_snapshot


def main():
    print("Running backfill predictions on historical matches...")
    predictor = BatchPredictor(backfill=True)
    count = predictor.predict()
    print(f"Backfilled {count} matches")

    print("Computing metrics snapshots...")
    for source in ("live", "backfill", "all"):
        compute_snapshot(source_filter=source)
    print("Done.")


if __name__ == "__main__":
    main()
```

**Step 2: Commit**

```bash
git add scripts/backfill_predictions.py
git commit -m "feat: add one-time backfill predictions script"
```

---

### Task 7: Admin dashboard API endpoints

**Files:**
- Create: `src/teelo/web/services/prediction_handlers.py`
- Modify: `src/teelo/web/routers/admin.py`

**Step 1: Create prediction dashboard handlers**

```python
"""Handlers for the prediction tracking dashboard."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import numpy as np
import structlog
from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import select, func, and_
from sqlalchemy.orm import Session

from teelo.db.models import (
    Match, ModelEvaluationSnapshot, Tournament, TournamentEdition,
)
from teelo.db.session import get_session
from teelo.web.app_context import get_templates
from teelo.web.services.legacy_main_handlers import _require_admin, _current_admin_user

logger = structlog.get_logger(__name__)
templates = get_templates()

TERMINAL_STATUSES = ("completed", "retired", "walkover", "default")


async def admin_predictions_page(request: Request):
    """Render the prediction tracking dashboard."""
    with get_session() as db:
        redirect = _require_admin(request, db)
        if redirect:
            return redirect
        admin = _current_admin_user(request, db)

    # Load latest model metadata
    model_meta = {}
    try:
        from teelo.ml.versioning import latest_model_path
        model_path = latest_model_path()
        meta_path = Path(f"{model_path}_meta.json")
        if meta_path.exists():
            with open(meta_path) as f:
                model_meta = json.load(f)
    except Exception:
        pass

    return templates.TemplateResponse(
        "admin_predictions.html",
        {"request": request, "admin": admin, "model_meta": model_meta},
    )


async def admin_predictions_summary(request: Request):
    """Return summary metrics for header cards."""
    with get_session() as db:
        redirect = _require_admin(request, db)
        if redirect:
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        # Get latest snapshot for each source
        result = {}
        for source in ("live", "backfill", "all"):
            snapshot = db.execute(
                select(ModelEvaluationSnapshot)
                .where(ModelEvaluationSnapshot.source_filter == source)
                .order_by(ModelEvaluationSnapshot.snapshot_date.desc())
                .limit(1)
            ).scalar_one_or_none()

            if snapshot:
                metrics = snapshot.metrics or {}
                # Get previous week's snapshot for delta
                week_ago = snapshot.snapshot_date - timedelta(days=7)
                prev = db.execute(
                    select(ModelEvaluationSnapshot)
                    .where(ModelEvaluationSnapshot.source_filter == source)
                    .where(ModelEvaluationSnapshot.snapshot_date <= week_ago)
                    .order_by(ModelEvaluationSnapshot.snapshot_date.desc())
                    .limit(1)
                ).scalar_one_or_none()

                prev_metrics = prev.metrics if prev else {}
                result[source] = {
                    "accuracy": metrics.get("accuracy"),
                    "log_loss": metrics.get("log_loss"),
                    "ece": metrics.get("ece"),
                    "brier_score": metrics.get("brier_score"),
                    "n_matches": metrics.get("n_matches", 0),
                    "snapshot_date": str(snapshot.snapshot_date),
                    "delta_accuracy": (
                        metrics.get("accuracy", 0) - prev_metrics.get("accuracy", 0)
                    ) if prev_metrics.get("accuracy") is not None else None,
                    "delta_log_loss": (
                        metrics.get("log_loss", 0) - prev_metrics.get("log_loss", 0)
                    ) if prev_metrics.get("log_loss") is not None else None,
                }
            else:
                result[source] = None

    return JSONResponse(result)


async def admin_predictions_breakdown(request: Request):
    """Return metrics breakdown by tour/surface/level/round/confidence."""
    with get_session() as db:
        redirect = _require_admin(request, db)
        if redirect:
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        source = request.query_params.get("source", "live")
        snapshot = db.execute(
            select(ModelEvaluationSnapshot)
            .where(ModelEvaluationSnapshot.source_filter == source)
            .order_by(ModelEvaluationSnapshot.snapshot_date.desc())
            .limit(1)
        ).scalar_one_or_none()

        if not snapshot:
            return JSONResponse({"breakdown": {}})

        metrics = snapshot.metrics or {}
        return JSONResponse({
            "by_tour": metrics.get("by_tour", {}),
            "by_surface": metrics.get("by_surface", {}),
            "by_level": metrics.get("by_level", {}),
            "by_round": metrics.get("by_round", {}),
            "by_confidence": metrics.get("by_confidence", {}),
        })


async def admin_predictions_charts_accuracy(request: Request):
    """Return accuracy time series data for charts."""
    with get_session() as db:
        redirect = _require_admin(request, db)
        if redirect:
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        source = request.query_params.get("source", "live")
        snapshot = db.execute(
            select(ModelEvaluationSnapshot)
            .where(ModelEvaluationSnapshot.source_filter == source)
            .order_by(ModelEvaluationSnapshot.snapshot_date.desc())
            .limit(1)
        ).scalar_one_or_none()

        if not snapshot:
            return JSONResponse({"daily": {}, "by_tour": {}})

        metrics = snapshot.metrics or {}
        return JSONResponse({
            "daily": metrics.get("daily", {}),
            "by_tour": metrics.get("by_tour", {}),
        })


async def admin_predictions_charts_calibration(request: Request):
    """Return calibration curve data."""
    with get_session() as db:
        redirect = _require_admin(request, db)
        if redirect:
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        source = request.query_params.get("source", "live")
        snapshot = db.execute(
            select(ModelEvaluationSnapshot)
            .where(ModelEvaluationSnapshot.source_filter == source)
            .order_by(ModelEvaluationSnapshot.snapshot_date.desc())
            .limit(1)
        ).scalar_one_or_none()

        if not snapshot:
            return JSONResponse({"calibration": []})

        metrics = snapshot.metrics or {}
        return JSONResponse({
            "calibration": metrics.get("calibration", []),
        })


async def admin_predictions_charts_distribution(request: Request):
    """Return confidence distribution histogram data."""
    with get_session() as db:
        redirect = _require_admin(request, db)
        if redirect:
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        source = request.query_params.get("source", "live")
        snapshot = db.execute(
            select(ModelEvaluationSnapshot)
            .where(ModelEvaluationSnapshot.source_filter == source)
            .order_by(ModelEvaluationSnapshot.snapshot_date.desc())
            .limit(1)
        ).scalar_one_or_none()

        if not snapshot:
            return JSONResponse({"by_confidence": {}})

        metrics = snapshot.metrics or {}
        return JSONResponse({
            "by_confidence": metrics.get("by_confidence", {}),
        })
```

**Step 2: Register routes in admin router**

Add to `src/teelo/web/routers/admin.py`:

```python
from teelo.web.services import prediction_handlers as pred

# ... existing routes ...

# Prediction dashboard
router.add_api_route('/admin/predictions', pred.admin_predictions_page, methods=['GET'], response_class=pred.HTMLResponse)
router.add_api_route('/admin/api/predictions/summary', pred.admin_predictions_summary, methods=['GET'])
router.add_api_route('/admin/api/predictions/breakdown', pred.admin_predictions_breakdown, methods=['GET'])
router.add_api_route('/admin/api/predictions/charts/accuracy', pred.admin_predictions_charts_accuracy, methods=['GET'])
router.add_api_route('/admin/api/predictions/charts/calibration', pred.admin_predictions_charts_calibration, methods=['GET'])
router.add_api_route('/admin/api/predictions/charts/distribution', pred.admin_predictions_charts_distribution, methods=['GET'])
```

**Step 3: Commit**

```bash
git add src/teelo/web/services/prediction_handlers.py src/teelo/web/routers/admin.py
git commit -m "feat: add prediction dashboard API endpoints"
```

---

### Task 8: Admin home — add predictions card

**Files:**
- Modify: `src/teelo/web/templates/admin_home.html:20-43`

**Step 1: Add predictions dashboard card**

Add a fourth card to the grid (after the SQL Editor card, before the closing `</div>`):

```html
        <a href="/admin/predictions"
           class="block bg-white border border-gray-200 rounded-xl p-5 shadow-sm hover:shadow transition">
            <div class="text-sm font-semibold text-gray-500">Prediction Tracker</div>
            <div class="mt-2 text-3xl font-bold text-teelo-dark">
                <i data-lucide="bar-chart-3" class="w-8 h-8"></i>
            </div>
            <div class="text-sm text-gray-500 mt-1">Model accuracy and prediction analytics</div>
        </a>
```

**Step 2: Commit**

```bash
git add src/teelo/web/templates/admin_home.html
git commit -m "feat: add prediction tracker card to admin home"
```

---

### Task 9: Dashboard template

**Files:**
- Create: `src/teelo/web/templates/admin_predictions.html`

**Step 1: Create the dashboard template**

This is a large template. Key sections:
- Header with back link + model info
- Summary cards row (accuracy, log_loss, ECE, brier, n_matches — with deltas)
- Tab switcher (Live / Backfill / Training CV)
- Charts section (4 charts: accuracy over time, calibration curve, confidence distribution, per-tour accuracy)
- Breakdown tables section (by tour, surface, level, round, confidence bucket)
- Filter bar (source selector, date range — filters applied via JS)

The template should extend `base.html`, include Chart.js CDN in `head_extra` block, and load `admin_predictions.js` in `scripts` block.

Note: The full template HTML will be generated by the implementing agent using the project's design system (Tailwind classes from MEMORY.md: card style `bg-white rounded-2xl shadow-soft border border-gray-100`, tour badge colors, surface colors, etc.). Use Lucide icons where appropriate.

Reference existing admin templates (e.g., `admin_home.html`, `admin_sql.html`) for layout patterns.

**Step 2: Commit**

```bash
git add src/teelo/web/templates/admin_predictions.html
git commit -m "feat: add prediction dashboard template"
```

---

### Task 10: Dashboard JavaScript

**Files:**
- Create: `src/teelo/web/static/js/admin_predictions.js`

**Step 1: Create the dashboard JS module**

Responsibilities:
- On page load, fetch `/admin/api/predictions/summary` and populate header cards
- Tab switching: re-fetch data with `?source=live|backfill` param
- Fetch chart data from the 3 chart endpoints and render with Chart.js:
  - Accuracy over time: line chart with daily points + 7-day/30-day rolling averages
  - Calibration curve: scatter/line chart with ideal diagonal reference line
  - Confidence distribution: bar chart from confidence buckets
  - Per-tour accuracy: multi-line chart (one line per tour)
- Fetch breakdown data and render tables
- Training CV tab: render model metadata (params, CV scores, date range) from the `model_meta` variable passed via template

Helper functions needed:
- `computeRollingAverage(data, window)` for smoothed lines
- `formatPercent(value)` / `formatDelta(value)` for display
- Chart color constants matching the Teelo design system

Reference existing JS files in `src/teelo/web/static/js/` for coding patterns (vanilla JS, DOM manipulation style).

**Step 2: Rebuild Tailwind CSS**

```bash
npx tailwindcss -i src/teelo/web/static/css/input.css -o src/teelo/web/static/css/styles.css --minify
```

**Step 3: Commit**

```bash
git add src/teelo/web/static/js/admin_predictions.js src/teelo/web/static/css/styles.css
git commit -m "feat: add prediction dashboard JS with Chart.js visualizations"
```

---

### Task 11: End-to-end verification

**Step 1: Run migrations**

```bash
alembic upgrade head
```

**Step 2: Run backfill**

```bash
python scripts/backfill_predictions.py
```

**Step 3: Start dev server and verify dashboard**

```bash
uvicorn teelo.api.main:app --reload
```

- Navigate to `/admin` — verify predictions card appears
- Navigate to `/admin/predictions` — verify page loads, charts render
- Check Live tab shows "No data" (no live predictions yet)
- Check Backfill tab shows metrics and charts
- Check Training CV tab shows model metadata

**Step 4: Test pipeline stages (optional dry run)**

```bash
python scripts/run_hourly_update.py --stages predictions,metrics_snapshot
```

**Step 5: Final commit if any fixes needed**

```bash
git add -A
git commit -m "fix: end-to-end verification fixes for prediction dashboard"
```
