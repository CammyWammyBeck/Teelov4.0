from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import sqlalchemy as sa
import structlog
import xgboost as xgb
from sqlalchemy import bindparam, select, true, update

from teelo.db.models import FeatureSet, Match, MatchFeatures
from teelo.db.session import get_session
from teelo.ml.randomize import swap_ab_features
from teelo.ml.versioning import latest_feature_set, latest_model_path
from teelo.storage import download_model

logger = structlog.get_logger(__name__)


class BatchPredictor:
    def __init__(self, model_path: str | None = None, feature_set_name: str | None = None, backfill: bool = False) -> None:
        self.model_path = model_path or latest_model_path()
        self.feature_set_name = feature_set_name or latest_feature_set()
        self.backfill = backfill

    def predict(self) -> int:
        model_path = Path(self.model_path)
        if not model_path.exists():
            try:
                downloaded_model_path, _ = download_model(
                    model_path.name,
                    local_dir=str(model_path.parent) if str(model_path.parent) != "." else "models",
                )
                self.model_path = downloaded_model_path
                logger.info("batch_predictor.model_downloaded", model_path=self.model_path)
            except Exception as exc:
                logger.warning(
                    "batch_predictor.model_download_failed",
                    model_path=self.model_path,
                    error=str(exc),
                )

        model = xgb.XGBClassifier()
        model.load_model(self.model_path)
        metadata = self._load_metadata()
        feature_names = metadata.get("feature_names", [])
        model_version = str(metadata.get("created_at") or Path(self.model_path).name)

        with get_session() as session:
            feature_set = session.execute(
                select(FeatureSet).where(FeatureSet.name == self.feature_set_name)
            ).scalar_one_or_none()
            if feature_set is None:
                raise ValueError(f"Feature set not found: {self.feature_set_name}")

            if self.backfill:
                status_filter = Match.status.in_(("completed", "retired", "walkover", "default"))
                extra_filter = true()
            else:
                status_filter = Match.status.in_(("upcoming", "scheduled"))
                extra_filter = Match.prediction_a.is_(None)

            stmt = (
                select(Match.id, MatchFeatures.features)
                .select_from(Match)
                .join(MatchFeatures, MatchFeatures.match_id == Match.id)
                .where(MatchFeatures.feature_set_id == feature_set.id)
                .where(status_filter)
                .where(extra_filter)
                .order_by(Match.id.asc())
            )
            CHUNK_SIZE = 10_000
            total_predicted = 0
            source = "backfill" if self.backfill else "live"
            from teelo.db.models import Match as MatchModel
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

            result = session.execute(stmt)
            chunk: list = []
            for row in result.yield_per(CHUNK_SIZE):
                chunk.append(row)
                if len(chunk) >= CHUNK_SIZE:
                    total_predicted += self._predict_chunk(
                        chunk, model, feature_names, model_version, source, update_stmt, session
                    )
                    logger.info("batch_predictor.progress", predicted=total_predicted)
                    chunk = []

            if chunk:
                total_predicted += self._predict_chunk(
                    chunk, model, feature_names, model_version, source, update_stmt, session
                )

            if total_predicted == 0:
                logger.info("batch_predictor.no_matches")
                return 0

        logger.info(
            "batch_predictor.predicted",
            model_path=self.model_path,
            feature_set=self.feature_set_name,
            count=total_predicted,
        )
        return total_predicted

    def _predict_chunk(self, chunk, model, feature_names, model_version, source, update_stmt, session) -> int:
        raw_features = [row.features or {} for row in chunk]

        # Original orientation: P(A wins)
        X_orig = pd.DataFrame(raw_features).reindex(columns=feature_names)
        X_orig = X_orig.apply(pd.to_numeric, errors="coerce")
        probs_orig = model.predict_proba(X_orig)[:, 1]

        # Swapped orientation: model sees (B, A), returns P(orig-B wins)
        X_swap = pd.DataFrame([swap_ab_features(f) for f in raw_features])
        X_swap = X_swap.reindex(columns=feature_names)
        X_swap = X_swap.apply(pd.to_numeric, errors="coerce")
        probs_swap = model.predict_proba(X_swap)[:, 1]

        # Average: P(A wins) = mean of original and (1 - swapped)
        probs = (probs_orig + (1.0 - probs_swap)) / 2.0

        now = datetime.utcnow()
        payloads = [
            {
                "b_match_id": row.id,
                "b_prediction_a": float(prob),
                "b_prediction_model_version": model_version,
                "b_prediction_updated_at": now,
                "b_prediction_source": source,
            }
            for row, prob in zip(chunk, probs)
        ]
        session.connection().execute(update_stmt, payloads)
        return len(chunk)

    def _load_metadata(self) -> dict[str, Any]:
        path = Path(f"{self.model_path}_meta.json")
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)


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
