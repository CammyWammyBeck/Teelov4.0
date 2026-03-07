from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import structlog
import xgboost as xgb
from sklearn.metrics import log_loss
from sqlalchemy import select

from teelo.db.models import FeatureSet, Match, MatchFeatures
from teelo.db.session import get_session

logger = structlog.get_logger(__name__)

TERMINAL_STATUSES = ("completed", "retired", "walkover", "default")
ABLATION_GROUP_PREFIXES = (
    "surface_",
    "level_",
    "round_",
    "tour_",
    "year",
    "elo_",
    "peak_",
    "win_rate_",
    "match_count_",
    "career_",
    "h2h_",
    "days_",
    "matches_",
    "games_",
    "seed_",
    "both_",
)


class FeatureSelector:
    def __init__(self, feature_set_name: str) -> None:
        self.feature_set_name = feature_set_name

    def run(self) -> dict[str, Any]:
        X, y, temporal_order = self._load_data()
        full_model = self._train_model(X, y)

        importance_gain = full_model.get_booster().get_score(importance_type="gain")
        normalized_importance: dict[str, float] = {}
        columns = list(X.columns)
        for feature, gain in importance_gain.items():
            resolved = feature
            if feature.startswith("f") and feature[1:].isdigit():
                idx = int(feature[1:])
                if 0 <= idx < len(columns):
                    resolved = str(columns[idx])
            normalized_importance[resolved] = float(gain)

        importance_ranking = sorted(
            (
                {"feature": feature, "gain": float(gain)}
                for feature, gain in normalized_importance.items()
            ),
            key=lambda row: row["gain"],
            reverse=True,
        )
        ranked_features = [row["feature"] for row in importance_ranking]
        for column in X.columns:
            if column not in normalized_importance:
                importance_ranking.append({"feature": column, "gain": 0.0})
                ranked_features.append(column)

        cumulative_results = self._run_cumulative_test(X, y, temporal_order, ranked_features)
        full_split_log_loss = self._temporal_split_log_loss(X, y, temporal_order)
        ablation_results = self._run_ablation(X, y, temporal_order, full_split_log_loss)

        report = {
            "feature_set": self.feature_set_name,
            "n_rows": int(len(X)),
            "n_features": int(len(X.columns)),
            "full_split_log_loss": full_split_log_loss,
            "importance_ranking": importance_ranking,
            "cumulative_results": cumulative_results,
            "ablation_results": ablation_results,
        }
        self._save_report(report)
        logger.info(
            "feature_selector.completed", report_path="models/feature_selection_report.json"
        )
        return report

    def _load_data(self) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
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
                    Match.temporal_order,
                )
                .select_from(MatchFeatures)
                .join(Match, Match.id == MatchFeatures.match_id)
                .where(MatchFeatures.feature_set_id == feature_set.id)
                .where(Match.status.in_(TERMINAL_STATUSES))
                .where(Match.winner_id.is_not(None))
                .where(Match.temporal_order.is_not(None))
                .order_by(Match.temporal_order.asc())
            )
            rows = list(session.execute(stmt).all())

        if not rows:
            raise ValueError("No rows found for feature selection.")

        X = pd.DataFrame([row.features or {} for row in rows]).apply(pd.to_numeric, errors="coerce")
        y = pd.Series(
            [1.0 if row.winner_id == row.player_a_id else 0.0 for row in rows],
            dtype="float64",
        )
        temporal_order = pd.Series([int(row.temporal_order) for row in rows], dtype="int64")

        logger.info(
            "feature_selector.data_loaded",
            feature_set=self.feature_set_name,
            rows=len(X),
            columns=len(X.columns),
        )
        return X, y, temporal_order

    def _train_model(self, X: pd.DataFrame, y: pd.Series) -> xgb.XGBClassifier:
        model = xgb.XGBClassifier(
            n_estimators=500,
            max_depth=6,
            learning_rate=0.05,
            use_label_encoder=False,
            eval_metric="logloss",
            enable_categorical=False,
            n_jobs=-1,
        )
        model.fit(X, y)
        return model

    def _temporal_split_log_loss(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        temporal_order: pd.Series,
    ) -> float:
        order = temporal_order.sort_values().index
        split_idx = max(1, int(len(order) * 0.8))
        if split_idx >= len(order):
            split_idx = len(order) - 1

        train_idx = order[:split_idx]
        test_idx = order[split_idx:]
        X_train = X.loc[train_idx]
        y_train = y.loc[train_idx]
        X_test = X.loc[test_idx]
        y_test = y.loc[test_idx]

        if y_train.nunique() < 2 or y_test.nunique() < 2:
            return float("inf")

        model = self._train_model(X_train, y_train)
        y_prob = model.predict_proba(X_test)[:, 1]
        return float(log_loss(y_test, y_prob, labels=[0.0, 1.0]))

    def _run_cumulative_test(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        temporal_order: pd.Series,
        ranked_features: list[str],
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        if not ranked_features:
            return results

        start = min(5, len(ranked_features))
        for n_features in range(start, len(ranked_features) + 1, 5):
            selected = ranked_features[:n_features]
            loss = self._temporal_split_log_loss(X[selected], y, temporal_order)
            results.append(
                {
                    "n_features": n_features,
                    "features": selected,
                    "log_loss": loss,
                }
            )

        if results[-1]["n_features"] != len(ranked_features):
            selected = ranked_features
            loss = self._temporal_split_log_loss(X[selected], y, temporal_order)
            results.append(
                {
                    "n_features": len(ranked_features),
                    "features": selected,
                    "log_loss": loss,
                }
            )
        return results

    def _run_ablation(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        temporal_order: pd.Series,
        full_split_log_loss: float,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        columns = list(X.columns)

        for prefix in ABLATION_GROUP_PREFIXES:
            removed = [column for column in columns if column.startswith(prefix)]
            if not removed or len(removed) >= len(columns):
                continue

            kept_columns = [column for column in columns if column not in removed]
            ablated_loss = self._temporal_split_log_loss(X[kept_columns], y, temporal_order)
            degraded = ablated_loss > full_split_log_loss
            results.append(
                {
                    "group_prefix": prefix,
                    "removed_count": len(removed),
                    "log_loss": ablated_loss,
                    "delta_log_loss": float(ablated_loss - full_split_log_loss),
                    "degraded": degraded,
                    "keep_without_group": not degraded,
                }
            )
        return results

    def _save_report(self, report: dict[str, Any]) -> None:
        output = Path("models/feature_selection_report.json")
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--feature-set", default="baseline_v1")
    args = parser.parse_args()
    FeatureSelector(args.feature_set).run()
