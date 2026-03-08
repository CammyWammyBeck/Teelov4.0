from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import structlog
import xgboost as xgb
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
from sqlalchemy import select

from teelo.config import settings
from teelo.db.models import FeatureSet, Match, MatchFeatures, Tournament, TournamentEdition
from teelo.db.session import get_session
from teelo.ml.randomize import randomize_ab
from teelo.ml.versioning import latest_feature_set, latest_model_path
from teelo.ml.versioning import latest_model_path

logger = structlog.get_logger(__name__)

TERMINAL_STATUSES = ("completed", "retired", "walkover", "default")


class ModelEvaluator:
    def __init__(self, model_path: str | None = None, feature_set_name: str | None = None) -> None:
        self.model_path = model_path or latest_model_path()
        self.feature_set_name = feature_set_name or latest_feature_set()

    def evaluate(self, holdout_year: int = 2025) -> dict[str, Any]:
        model = xgb.XGBClassifier()
        model.load_model(self.model_path)
        metadata = self._load_metadata()
        feature_names = metadata.get("feature_names", [])

        with get_session() as session:
            feature_set = session.execute(
                select(FeatureSet).where(FeatureSet.name == self.feature_set_name)
            ).scalar_one_or_none()
            if feature_set is None:
                raise ValueError(f"Feature set not found: {self.feature_set_name}")

            stmt = (
                select(
                    MatchFeatures.features,
                    Match.winner_id,
                    Match.player_a_id,
                    Tournament.tour,
                )
                .select_from(MatchFeatures)
                .join(Match, Match.id == MatchFeatures.match_id)
                .join(TournamentEdition, TournamentEdition.id == Match.tournament_edition_id)
                .join(Tournament, Tournament.id == TournamentEdition.tournament_id)
                .where(MatchFeatures.feature_set_id == feature_set.id)
                .where(Match.status.in_(TERMINAL_STATUSES))
                .where(Match.winner_id.is_not(None))
                .where(TournamentEdition.year == holdout_year)
            )
            rows = list(session.execute(stmt).all())

        if not rows:
            raise ValueError(f"No completed rows found for holdout_year={holdout_year}")

        X = pd.DataFrame([row.features or {} for row in rows]).reindex(columns=feature_names)
        X = X.apply(pd.to_numeric, errors="coerce")
        y_true = pd.Series(
            [1 if row.winner_id == row.player_a_id else 0 for row in rows],
            dtype="int64",
        )
        tours = pd.Series([row.tour or "unknown" for row in rows], dtype="object")

        X, y_true = randomize_ab(X, y_true)

        y_prob = model.predict_proba(X)[:, 1]
        y_pred = (y_prob > 0.5).astype(int)

        accuracy = float(accuracy_score(y_true, y_pred))
        ll = float(log_loss(y_true, y_prob, labels=[0, 1]))
        brier = float(brier_score_loss(y_true, y_prob))
        ece = float(self._expected_calibration_error(y_true, y_prob))

        tour_breakdown: dict[str, float] = {}
        frame = pd.DataFrame({"tour": tours, "y_true": y_true, "y_pred": y_pred})
        for tour, group in frame.groupby("tour"):
            tour_breakdown[str(tour)] = float((group["y_true"] == group["y_pred"]).mean())

        quality_gates = {
            "accuracy_threshold": float(settings.ml_accuracy_threshold),
            "calibration_threshold": float(settings.ml_calibration_threshold),
            "accuracy_pass": accuracy >= float(settings.ml_accuracy_threshold),
            "calibration_pass": ece <= float(settings.ml_calibration_threshold),
        }

        metrics = {
            "holdout_year": holdout_year,
            "n_matches": int(len(y_true)),
            "log_loss": ll,
            "accuracy": accuracy,
            "brier_score": brier,
            "ece": ece,
            "per_tour_accuracy": tour_breakdown,
            "quality_gates": quality_gates,
        }
        logger.info("model_evaluator.metrics", **metrics)
        return metrics

    def _expected_calibration_error(
        self,
        y_true: pd.Series,
        y_prob: Any,
        n_bins: int = 10,
    ) -> float:
        frame = pd.DataFrame(
            {"y_true": y_true.astype(float), "y_prob": pd.Series(y_prob, dtype="float64")}
        )
        bins = pd.cut(frame["y_prob"], bins=n_bins, labels=False, include_lowest=True)
        frame["bin"] = bins

        total = len(frame)
        ece = 0.0
        for _, group in frame.groupby("bin"):
            if group.empty:
                continue
            confidence = float(group["y_prob"].mean())
            observed = float(group["y_true"].mean())
            weight = float(len(group) / total)
            ece += abs(confidence - observed) * weight
        return ece

    def _load_metadata(self) -> dict[str, Any]:
        path = Path(f"{self.model_path}_meta.json")
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=None)
    parser.add_argument("--feature-set", default=None)
    parser.add_argument("--holdout-year", type=int, default=2025)
    args = parser.parse_args()
    ModelEvaluator(args.model, args.feature_set).evaluate(args.holdout_year)
