"""Handlers for the prediction tracking dashboard."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import structlog
from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse

from teelo.db.models import ModelEvaluationSnapshot
from teelo.db.session import get_session
from teelo.web.app_context import templates
from teelo.web.services.legacy_main_handlers import _require_admin, _current_admin_user
from sqlalchemy import select

logger = structlog.get_logger(__name__)


async def admin_predictions_page(request: Request):
    """Render the prediction tracking dashboard."""
    with get_session() as db:
        redirect = _require_admin(request, db)
        if redirect:
            return redirect
        admin = _current_admin_user(request, db)

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
        {"request": request, "admin": admin, "model_meta": model_meta, "now": datetime.utcnow()},
    )


async def admin_predictions_summary(request: Request):
    """Return summary metrics for header cards."""
    with get_session() as db:
        redirect = _require_admin(request, db)
        if redirect:
            return JSONResponse({"error": "unauthorized"}, status_code=401)

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
        return JSONResponse({"calibration": metrics.get("calibration", [])})


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
        return JSONResponse({"by_confidence": metrics.get("by_confidence", {})})
