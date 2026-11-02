"""Load validated Gold data, split by date, preprocess, train, evaluate, save.

Every learned preprocessing step (imputation, scaling, one-hot encoding) sits
inside the scikit-learn Pipeline, so it is fitted on the training dates only.
Intermediate artifacts go to data/ml/ so each Airflow task can run on its own.
"""

import json
import os
import shutil
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.config import Settings, settings
from src.logging_config import get_logger
from src.schemas import (
    CATEGORICAL_FEATURES,
    ENGINEERED_FEATURES,
    FEATURES,
    NUMERIC_FEATURES,
    TARGET,
    ModelReport,
    SplitInfo,
    TrainingMetrics,
)

logger = get_logger(__name__)

COMBINED_FILE = "gold_combined.csv"
CANDIDATE_MODEL_FILE = "candidate_model.joblib"
SPLIT_FILE = "split.json"
CANDIDATE_METRICS_FILE = "candidate_metrics.json"


def _write_json(data: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    os.replace(tmp, path)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def load_gold_data(cfg: Settings = settings) -> dict[str, Any]:
    """Concatenate every validated Gold partition into one training table."""
    partitions = sorted((cfg.data_dir / "gold").glob("date=*/orders.csv"))
    if not partitions:
        raise FileNotFoundError(
            f"no Gold partitions under {cfg.data_dir / 'gold'}; run data_pipeline first"
        )
    df = pd.concat([pd.read_csv(p) for p in partitions], ignore_index=True)
    dupes = int(df["order_id"].duplicated().sum())
    if dupes:
        raise ValueError(f"{dupes} order_id values appear in more than one partition")
    cfg.ml_dir.mkdir(parents=True, exist_ok=True)
    path = cfg.ml_dir / COMBINED_FILE
    df.to_csv(path, index=False)
    summary = {
        "path": str(path),
        "rows": len(df),
        "partitions": len(partitions),
        "first_date": str(df["order_date"].min()),
        "last_date": str(df["order_date"].max()),
    }
    logger.info("Loaded Gold data: %s", summary)
    return summary


def time_split(
    df: pd.DataFrame, test_fraction: float
) -> tuple[pd.DataFrame, pd.DataFrame, SplitInfo]:
    """Train on the earliest dates, test on the latest: no future leakage."""
    dates = sorted(df["order_date"].astype(str).unique())
    if len(dates) < 2:
        raise ValueError(f"need at least 2 Gold dates for a time split, got {dates}")
    n_test = min(len(dates) - 1, max(1, round(len(dates) * test_fraction)))
    train_dates, test_dates = dates[:-n_test], dates[-n_test:]
    is_test = df["order_date"].astype(str).isin(test_dates)
    train, test = df[~is_test], df[is_test]
    info = SplitInfo(
        strategy=f"time-based: latest {n_test} of {len(dates)} dates held out",
        train_dates=train_dates,
        test_dates=test_dates,
        train_rows=len(train),
        test_rows=len(test),
        train_late_rate=round(float(train[TARGET].mean()), 4),
        test_late_rate=round(float(test[TARGET].mean()), 4),
    )
    return train, test, info


def build_pipeline(random_state: int) -> Pipeline:
    preprocess = ColumnTransformer(
        [
            (
                "numeric",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="median")),
                        ("scale", StandardScaler()),
                    ]
                ),
                list(NUMERIC_FEATURES),
            ),
            (
                "categorical",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="most_frequent")),
                        ("encode", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                list(CATEGORICAL_FEATURES),
            ),
        ]
    )
    model = LogisticRegression(
        class_weight="balanced", max_iter=1000, random_state=random_state
    )
    return Pipeline([("preprocess", preprocess), ("model", model)])


def _features(df: pd.DataFrame) -> pd.DataFrame:
    X = df[list(FEATURES)].copy()
    X[list(CATEGORICAL_FEATURES)] = X[list(CATEGORICAL_FEATURES)].astype(object)
    return X


def _combined(cfg: Settings) -> pd.DataFrame:
    path = cfg.ml_dir / COMBINED_FILE
    if not path.exists():
        raise FileNotFoundError(f"{path} not found; run load_gold_data first")
    return pd.read_csv(path)


def train_model(cfg: Settings = settings) -> dict[str, Any]:
    train, _, split = time_split(_combined(cfg), cfg.test_fraction)
    pipeline = build_pipeline(cfg.random_state)
    logger.info("Training on %d rows (%s)", split.train_rows, split.strategy)
    pipeline.fit(_features(train), train[TARGET])
    joblib.dump(pipeline, cfg.ml_dir / CANDIDATE_MODEL_FILE)
    _write_json(asdict(split), cfg.ml_dir / SPLIT_FILE)
    return asdict(split)


def compute_metrics(y_true: pd.Series, y_pred) -> TrainingMetrics:
    return TrainingMetrics(
        accuracy=round(float(accuracy_score(y_true, y_pred)), 4),
        precision=round(float(precision_score(y_true, y_pred, zero_division=0)), 4),
        recall=round(float(recall_score(y_true, y_pred, zero_division=0)), 4),
        f1=round(float(f1_score(y_true, y_pred, zero_division=0)), 4),
        confusion_matrix=confusion_matrix(y_true, y_pred, labels=[0, 1]).tolist(),
    )


def evaluate_model(cfg: Settings = settings) -> dict[str, Any]:
    _, test, _ = time_split(_combined(cfg), cfg.test_fraction)
    pipeline = joblib.load(cfg.ml_dir / CANDIDATE_MODEL_FILE)
    metrics = compute_metrics(test[TARGET], pipeline.predict(_features(test)))
    logger.info("Test metrics on %d rows: %s", len(test), metrics)
    _write_json(asdict(metrics), cfg.ml_dir / CANDIDATE_METRICS_FILE)
    return asdict(metrics)


def save_model(cfg: Settings = settings) -> dict[str, Any]:
    """Publish the evaluated candidate: fixed-name artifacts plus a version."""
    candidate = cfg.ml_dir / CANDIDATE_MODEL_FILE
    combined = _combined(cfg)
    now = datetime.now(timezone.utc)
    version = now.strftime("v%Y%m%dT%H%M%SZ")
    report = ModelReport(
        model_version=version,
        model_type="LogisticRegression(class_weight='balanced') in a sklearn Pipeline",
        trained_at=now.isoformat(timespec="seconds"),
        gold_rows=len(combined),
        gold_partitions=combined["order_date"].nunique(),
        features={
            "categorical": list(CATEGORICAL_FEATURES),
            "numeric": [f for f in NUMERIC_FEATURES if f not in ENGINEERED_FEATURES],
            "engineered": list(ENGINEERED_FEATURES),
        },
        split=SplitInfo(**_read_json(cfg.ml_dir / SPLIT_FILE)),
        metrics=TrainingMetrics(**_read_json(cfg.ml_dir / CANDIDATE_METRICS_FILE)),
        model_path=str(cfg.model_path.relative_to(cfg.models_dir.parent)),
    )

    cfg.models_dir.mkdir(parents=True, exist_ok=True)
    tmp = cfg.model_path.with_name(cfg.model_path.name + ".tmp")
    shutil.copyfile(candidate, tmp)
    os.replace(tmp, cfg.model_path)
    _write_json(report.to_dict(), cfg.metrics_path)

    # immutable copy per training run; the fixed-name files always point at the latest
    version_dir = cfg.models_dir / "versions" / version
    version_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(candidate, version_dir / "model.joblib")
    _write_json(report.to_dict(), version_dir / "metrics.json")

    # self-check: the published artifact must reproduce the candidate's predictions
    _, test, _ = time_split(combined, cfg.test_fraction)
    X_test = _features(test)
    assert (
        joblib.load(cfg.model_path).predict(X_test)
        == joblib.load(candidate).predict(X_test)
    ).all(), "saved model does not reproduce the evaluated candidate"
    logger.info(
        "Saved %s and %s (version %s)", cfg.model_path, cfg.metrics_path, version
    )
    return report.to_dict()


def run_training(cfg: Settings = settings) -> dict[str, Any]:
    """The whole ML pipeline, outside Airflow."""
    load_gold_data(cfg)
    train_model(cfg)
    evaluate_model(cfg)
    return save_model(cfg)
