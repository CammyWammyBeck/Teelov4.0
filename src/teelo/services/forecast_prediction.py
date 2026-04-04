from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog
import xgboost as xgb
from sqlalchemy import select
from sqlalchemy.orm import Session

from teelo.db.models import FeatureSet, Match
from teelo.features import build_registry
from teelo.features.state import MatchContext, PlayerState
from teelo.ml.randomize import swap_ab_features
from teelo.ml.versioning import latest_feature_set, latest_model_path, latest_preset
from teelo.storage import download_model

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class ForecastModel:
    model: xgb.XGBClassifier
    feature_names: list[str]
    model_version: str
    feature_set_name: str


def load_forecast_model(
    *, model_path: str | None = None, feature_set_name: str | None = None
) -> ForecastModel:
    model_path = model_path or latest_model_path()
    feature_set_name = feature_set_name or latest_feature_set()

    path = Path(model_path)
    if not path.exists():
        downloaded_model_path, _ = download_model(
            path.name, local_dir=str(path.parent) if str(path.parent) != "." else "models"
        )
        path = Path(downloaded_model_path)
        model_path = str(path)

    model = xgb.XGBClassifier()
    model.load_model(model_path)

    meta_path = Path(f"{model_path}_meta.json")
    with meta_path.open("r", encoding="utf-8") as handle:
        metadata = json.load(handle)

    feature_names = list(metadata.get("feature_names", []))
    model_version = str(metadata.get("created_at") or path.name)
    return ForecastModel(
        model=model,
        feature_names=feature_names,
        model_version=model_version,
        feature_set_name=feature_set_name,
    )


def predict_probability_a(model: ForecastModel, features: dict[str, Any]) -> float:
    """Predict P(A wins) using the same swap-average logic as BatchPredictor."""
    X_orig = pd.DataFrame([features]).reindex(columns=model.feature_names)
    X_orig = X_orig.apply(pd.to_numeric, errors="coerce")
    prob_orig = float(model.model.predict_proba(X_orig)[:, 1][0])

    X_swap = pd.DataFrame([swap_ab_features(features)]).reindex(columns=model.feature_names)
    X_swap = X_swap.apply(pd.to_numeric, errors="coerce")
    prob_swap = float(model.model.predict_proba(X_swap)[:, 1][0])

    return float((prob_orig + (1.0 - prob_swap)) / 2.0)


def build_features_for_states(
    *,
    state_a: PlayerState,
    state_b: PlayerState,
    ctx: MatchContext,
    feature_set_name: str | None = None,
) -> dict[str, Any]:
    preset = latest_preset(feature_set_name or latest_feature_set())
    registry = build_registry(preset)
    return registry.compute_all(state_a, state_b, ctx)


def reuse_or_predict_real_match(
    session: Session,
    match: Match,
    *,
    model: ForecastModel,
    ctx: MatchContext,
    state_a: PlayerState,
    state_b: PlayerState,
) -> tuple[dict[str, Any], float, str]:
    """Return (features, prediction_a, model_version), repredicting if needed.

    This is used by forecast to ensure real known matchups are predicted via the
    normal feature/model path (same feature set/model as live).
    """

    # Always defer to an existing prediction — the hourly pipeline is the
    # authoritative source and must never be overwritten by the forecast.
    if match.prediction_a is not None:
        return {}, float(match.prediction_a), match.prediction_model_version or model.model_version

    # No prediction yet — compute one and persist it as a fallback so the
    # forecast has something to work with until the pipeline catches up.
    feature_set = session.execute(
        select(FeatureSet).where(FeatureSet.name == model.feature_set_name)
    ).scalar_one_or_none()
    if feature_set is None:
        raise ValueError(f"Feature set not found: {model.feature_set_name}")

    features = build_features_for_states(
        state_a=state_a, state_b=state_b, ctx=ctx, feature_set_name=model.feature_set_name
    )
    prediction_a = predict_probability_a(model, features)

    match.prediction_a = prediction_a
    match.prediction_model_version = model.model_version
    match.prediction_updated_at = datetime.utcnow()
    match.prediction_source = "forecast_live"

    session.add(match)
    return features, prediction_a, model.model_version
