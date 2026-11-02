import json

import joblib
import pandas as pd
import pytest

from src.etl import run_batch
from src.schemas import NUMERIC_FEATURES
from src.train import (
    build_pipeline,
    load_gold_data,
    run_training,
    time_split,
    train_model,
)
from tests.conftest import batch_dates, make_orders


@pytest.fixture
def gold_days(cfg, write_raw):
    days = batch_dates(5)
    for i, day in enumerate(days):
        write_raw(day, make_orders(day, n=40, seed=i))
        run_batch(day, cfg)
    return days


def test_load_gold_data_combines_all_partitions(cfg, gold_days):
    summary = load_gold_data(cfg)
    assert summary["rows"] == 200 and summary["partitions"] == 5
    assert summary["first_date"] == gold_days[0].isoformat()


def test_load_gold_data_requires_gold(cfg):
    with pytest.raises(FileNotFoundError, match="no Gold partitions"):
        load_gold_data(cfg)


def test_time_split_keeps_test_dates_after_train_dates(cfg, gold_days):
    load_gold_data(cfg)
    df = pd.read_csv(cfg.ml_dir / "gold_combined.csv")
    train, test, info = time_split(df, 0.2)
    assert max(info.train_dates) < min(info.test_dates)
    assert set(train["order_date"]).isdisjoint(set(test["order_date"]))
    assert info.train_rows + info.test_rows == len(df) == 200


def test_time_split_needs_two_dates():
    df = make_orders(batch_dates(1)[0])
    with pytest.raises(ValueError, match="at least 2 Gold dates"):
        time_split(df, 0.2)


def test_preprocessing_is_fitted_on_training_rows_only(cfg, gold_days):
    load_gold_data(cfg)
    train_model(cfg)
    df = pd.read_csv(cfg.ml_dir / "gold_combined.csv")
    train, _, _ = time_split(df, cfg.test_fraction)
    pipeline = joblib.load(cfg.ml_dir / "candidate_model.joblib")
    imputer = pipeline.named_steps["preprocess"].named_transformers_["numeric"][
        "impute"
    ]
    expected = train[list(NUMERIC_FEATURES)].median().to_numpy()
    assert imputer.statistics_ == pytest.approx(expected)


def test_run_training_saves_model_and_metrics(cfg, gold_days):
    report = run_training(cfg)
    assert cfg.model_path.exists() and cfg.metrics_path.exists()
    saved = json.loads(cfg.metrics_path.read_text())
    assert set(saved["metrics"]) >= {"accuracy", "precision", "recall", "f1"}
    assert saved["split"]["test_rows"] == report["split"]["test_rows"] > 0


def test_build_pipeline_is_unfitted():
    pipeline = build_pipeline(42)
    assert not hasattr(pipeline.named_steps["model"], "coef_")
