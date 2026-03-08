from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog
import xgboost as xgb
from sqlalchemy import bindparam, select, update

from teelo.db.models import FeatureSet, Match, MatchFeatures
from teelo.db.session import get_session

logger = structlog.get_logger(__name__)


class BatchPredictor:
    def __init__(self, model_path: str, feature_set_name: str) -> None:
        self.model_path = model_path
        self.feature_set_name = feature_set_name

    def predict(self) -> int:
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

            stmt = (
                select(Match.id, MatchFeatures.features)
                .select_from(Match)
                .join(MatchFeatures, MatchFeatures.match_id == Match.id)
                .where(MatchFeatures.feature_set_id == feature_set.id)
                .where(Match.status.in_(("upcoming", "scheduled")))
                .order_by(Match.id.asc())
            )
            rows = list(session.execute(stmt).all())
            if not rows:
                logger.info("batch_predictor.no_matches")
                return 0

            X = pd.DataFrame([row.features or {} for row in rows])
            X = X.reindex(columns=feature_names)
            X = X.apply(pd.to_numeric, errors="coerce")
            probs = model.predict_proba(X)[:, 1]

            now = datetime.utcnow()
            payloads = [
                {
                    "b_match_id": row.id,
                    "b_prediction_a": float(prob),
                    "b_prediction_model_version": model_version,
                    "b_prediction_updated_at": now,
                }
                for row, prob in zip(rows, probs)
            ]
            from teelo.db.models import Match as MatchModel
            update_stmt = (
                MatchModel.__table__.update()
                .where(MatchModel.__table__.c.id == bindparam("b_match_id"))
                .values(
                    prediction_a=bindparam("b_prediction_a"),
                    prediction_model_version=bindparam("b_prediction_model_version"),
                    prediction_updated_at=bindparam("b_prediction_updated_at"),
                )
            )
            session.connection().execute(update_stmt, payloads)

        logger.info(
            "batch_predictor.predicted",
            model_path=self.model_path,
            feature_set=self.feature_set_name,
            count=len(rows),
        )
        return len(rows)

    def _load_metadata(self) -> dict[str, Any]:
        path = Path(f"{self.model_path}_meta.json")
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="models/prediction_v1.json")
    parser.add_argument("--feature-set", default="baseline_v1")
    args = parser.parse_args()
    BatchPredictor(args.model, args.feature_set).predict()
