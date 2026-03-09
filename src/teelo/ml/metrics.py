"""Compute and store prediction accuracy metrics."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

import numpy as np
import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from teelo.db.models import (
    Match, ModelEvaluationSnapshot, Tournament, TournamentEdition,
)
from teelo.db.session import get_session

logger = structlog.get_logger(__name__)

TERMINAL_STATUSES = ("completed", "retired", "walkover", "default")

CONFIDENCE_BUCKETS = [
    (0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.0),
]


def compute_snapshot(
    model_version: str | None = None,
    snapshot_dt: date | None = None,
    source_filter: str = "all",
) -> dict[str, Any]:
    """Compute prediction metrics and upsert snapshot row."""
    snapshot_dt = snapshot_dt or date.today()

    with get_session() as session:
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

    pred_a = np.array([float(r.prediction_a) for r in rows])
    actual = np.array([1 if r.winner_id == r.player_a_id else 0 for r in rows])
    predicted = (pred_a > 0.5).astype(int)

    metrics: dict[str, Any] = {
        "n_matches": len(rows),
        "accuracy": float(np.mean(predicted == actual)),
        "log_loss": float(_log_loss(actual, pred_a)),
        "brier_score": float(np.mean((pred_a - actual) ** 2)),
        "ece": float(_ece(actual, pred_a)),
    }

    metrics["by_tour"] = _breakdown(rows, pred_a, actual, lambda r: r.tour)
    metrics["by_surface"] = _breakdown(rows, pred_a, actual, lambda r: r.surface)
    metrics["by_level"] = _breakdown(rows, pred_a, actual, lambda r: r.level)
    metrics["by_round"] = _breakdown(rows, pred_a, actual, lambda r: r.round)
    metrics["by_confidence"] = _confidence_breakdown(pred_a, actual)
    metrics["daily"] = _daily_breakdown(rows, pred_a, actual)
    metrics["calibration"] = _calibration_data(pred_a, actual)

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
    confidence = np.maximum(pred_a, 1 - pred_a)
    predicted = (pred_a > 0.5).astype(int)
    for lo, hi in CONFIDENCE_BUCKETS:
        mask = (confidence >= lo) & (confidence < hi)
        if not mask.any():
            continue
        label = f"{int(lo*100)}-{int(hi*100)}%"
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
