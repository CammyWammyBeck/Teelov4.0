from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import optuna
import pandas as pd
import structlog
import xgboost as xgb
from sklearn.metrics import log_loss
from sqlalchemy import select

from teelo.db.models import FeatureSet, Match, MatchFeatures
from teelo.db.session import get_session
from teelo.ml.randomize import randomize_ab
from teelo.ml.versioning import latest_feature_set, next_model_path
from teelo.storage import upload_model

logger = structlog.get_logger(__name__)

TERMINAL_STATUSES = ("completed", "retired", "walkover", "default")
TEMPORAL_FOLDS: list[tuple[int, int]] = [
    (2015, 2016),
    (2017, 2018),
    (2019, 2020),
    (2021, 2022),
    (2023, 2024),
]


class ModelTrainer:
    def __init__(
        self, feature_set_name: str | None = None, output_path: str | None = None
    ) -> None:
        logger.info("initialising trainer")
        self.feature_set_name = feature_set_name or latest_feature_set()
        self.output_path = output_path or next_model_path()
        self.cv_scores: dict[str, float] = {}

    def train(self, holdout_year: int | None = None) -> dict[str, Any]:
        logger.info("loading data")
        X, y, years = self._load_data()

        if holdout_year is not None:
            train_mask = years != holdout_year
            X_train = X.loc[train_mask].reset_index(drop=True)
            y_train = y.loc[train_mask].reset_index(drop=True)
            years_train = years.loc[train_mask].reset_index(drop=True)
            logger.info(
                "trainer.holdout_excluded",
                holdout_year=holdout_year,
                excluded_rows=int((~train_mask).sum()),
            )
        else:
            X_train, y_train, years_train = X, y, years

        logger.info("beginning training")
        best_params = self._optimize(X_train, y_train, years_train)
        model = self._train_final(X_train, y_train, best_params)
        metadata = self._save(model, best_params, X_train, y_train, years_train)

        try:
            upload_model(self.output_path)
            logger.info("trainer.s3_upload_done", model_path=self.output_path)
        except Exception as exc:
            logger.warning(
                "trainer.s3_upload_failed",
                model_path=self.output_path,
                error=str(exc),
            )

        return metadata

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
                    Match.match_date,
                )
                .select_from(MatchFeatures)
                .join(Match, Match.id == MatchFeatures.match_id)
                .where(MatchFeatures.feature_set_id == feature_set.id)
                .where(Match.status.in_(TERMINAL_STATUSES))
                .where(Match.winner_id.is_not(None))
                .where(Match.temporal_order.is_not(None))
                .order_by(Match.temporal_order.asc())
            )

            # Stream in chunks to avoid loading all JSONB dicts into memory
            feature_chunks: list[pd.DataFrame] = []
            y_vals: list[float] = []
            date_vals: list[Any] = []
            chunk_size = 50_000
            total_loaded = 0

            result = session.execute(stmt).yield_per(chunk_size)
            for partition in result.partitions(chunk_size):
                rows = list(partition)
                if not rows:
                    break
                chunk_df = pd.DataFrame([r.features or {} for r in rows])
                feature_chunks.append(chunk_df)
                y_vals.extend(1.0 if r.winner_id == r.player_a_id else 0.0 for r in rows)
                date_vals.extend(r.match_date for r in rows)
                total_loaded += len(rows)
                logger.info("trainer.loading_data", loaded=total_loaded)
                del rows

        if not feature_chunks:
            raise ValueError("No training rows found for the requested feature set and filters.")

        X = pd.concat(feature_chunks, ignore_index=True).apply(pd.to_numeric, errors="coerce")
        del feature_chunks
        y = pd.Series(y_vals, dtype="float64")
        del y_vals
        years = pd.to_datetime(pd.Series(date_vals), errors="coerce").dt.year
        del date_vals

        valid_mask = years.notna()
        if not valid_mask.all():
            dropped = int((~valid_mask).sum())
            logger.warning("trainer.rows_dropped_missing_year", dropped=dropped)
            X = X.loc[valid_mask].reset_index(drop=True)
            y = y.loc[valid_mask].reset_index(drop=True)
            years = years.loc[valid_mask].reset_index(drop=True)

        years = years.astype("int64")

        if X.empty:
            raise ValueError("No valid rows left after filtering missing match_date years.")

        X, y = randomize_ab(X, y)

        logger.info(
            "trainer.data_loaded",
            feature_set=self.feature_set_name,
            rows=len(X),
            columns=len(X.columns),
        )
        return X, y, years

    def _optimize(self, X: pd.DataFrame, y: pd.Series, years: pd.Series) -> dict[str, Any]:
        n_trials = 25
        t_start = time.monotonic()

        def objective(trial: optuna.Trial) -> float:
            params = {
                "n_estimators": trial.suggest_int("n_estimators", 100, 500),
                "max_depth": trial.suggest_int("max_depth", 3, 10),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "subsample": trial.suggest_float("subsample", 0.6, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
                "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
                "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
                "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
            }
            score = self._temporal_cv_score(params, X, y, years)
            done = trial.number + 1
            elapsed = time.monotonic() - t_start
            rate = elapsed / done if done > 0 else 0
            eta_s = rate * (n_trials - done)
            logger.info(
                "trainer.trial_done",
                trial=done,
                total=n_trials,
                pct=f"{done / n_trials * 100:.0f}%",
                score=f"{score:.5f}",
                eta=f"{eta_s / 60:.1f} min",
            )
            return score

        logger.info("trainer.optimization_start", n_trials=n_trials)
        optuna.logging.set_verbosity(optuna.logging.WARNING)
        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=n_trials)

        best_params: dict[str, Any] = dict(study.best_params)
        self.cv_scores = {"mean_log_loss": float(study.best_value)}

        logger.info(
            "trainer.optimization_done",
            best_value=study.best_value,
            best_params=best_params,
        )
        return best_params

    def _temporal_cv_score(
        self,
        params: dict[str, Any],
        X: pd.DataFrame,
        y: pd.Series,
        years: pd.Series,
    ) -> float:
        scores = self._temporal_cv_scores(params, X, y, years)
        mean_score = scores.get("mean_log_loss")
        if mean_score is None:
            return float("inf")
        return float(mean_score)

    def _temporal_cv_scores(
        self,
        params: dict[str, Any],
        X: pd.DataFrame,
        y: pd.Series,
        years: pd.Series,
    ) -> dict[str, float]:
        fold_scores: dict[str, float] = {}

        for idx, (train_year_max, test_year) in enumerate(TEMPORAL_FOLDS, start=1):
            train_mask = years <= train_year_max
            test_mask = years == test_year

            if int(train_mask.sum()) == 0 or int(test_mask.sum()) == 0:
                logger.warning(
                    "trainer.fold_skipped_empty_split",
                    fold=idx,
                    train_year_max=train_year_max,
                    test_year=test_year,
                )
                continue

            X_train = X.loc[train_mask]
            y_train = y.loc[train_mask]
            X_test = X.loc[test_mask]
            y_test = y.loc[test_mask]

            if y_train.nunique() < 2:
                logger.warning(
                    "trainer.fold_skipped_single_class_train",
                    fold=idx,
                    train_year_max=train_year_max,
                    test_year=test_year,
                )
                continue

            model = xgb.XGBClassifier(
                **params,
                use_label_encoder=False,
                eval_metric="logloss",
                enable_categorical=False,
                early_stopping_rounds=20,
            )
            model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
            y_prob = model.predict_proba(X_test)[:, 1]
            fold_score = log_loss(y_test, y_prob, labels=[0.0, 1.0])
            fold_scores[f"fold_{idx}"] = float(fold_score)

        if not fold_scores:
            return {}

        mean_score = float(sum(fold_scores.values()) / len(fold_scores))
        fold_scores["mean_log_loss"] = mean_score
        return fold_scores

    def _train_final(
        self, X: pd.DataFrame, y: pd.Series, params: dict[str, Any]
    ) -> xgb.XGBClassifier:
        model = xgb.XGBClassifier(
            **params,
            use_label_encoder=False,
            eval_metric="logloss",
            enable_categorical=False,
        )
        model.fit(X, y)
        return model

    def _save(
        self,
        model: xgb.XGBClassifier,
        params: dict[str, Any],
        X: pd.DataFrame,
        y: pd.Series,
        years: pd.Series,
    ) -> dict[str, Any]:
        output = Path(self.output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        model.save_model(str(output))

        min_year = int(years.min())
        max_year = int(years.max())
        metadata = {
            "feature_names": list(X.columns),
            "params": params,
            "cv_scores": self.cv_scores or self._temporal_cv_scores(params, X, y, years),
            "train_size": int(len(y)),
            "date_range": f"{min_year}-{max_year}",
            "created_at": datetime.utcnow().isoformat(),
        }

        meta_path = Path(f"{self.output_path}_meta.json")
        with meta_path.open("w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, sort_keys=True)

        logger.info("trainer.saved", model_path=str(output), metadata_path=str(meta_path))
        return metadata


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--feature-set", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--holdout-year", type=int, default=None)
    args = parser.parse_args()
    output_path = args.output or next_model_path()
    trainer = ModelTrainer(args.feature_set, output_path)
    trainer.train(holdout_year=args.holdout_year)
