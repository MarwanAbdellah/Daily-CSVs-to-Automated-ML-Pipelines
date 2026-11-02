import numpy as np
import pandas as pd
import pytest

from src.etl import (
    check_gold,
    clean_orders,
    extract_to_bronze,
    maybe_simulate_failure,
    prepare_gold,
    run_batch,
    transform_to_silver,
    validate_gold,
)
from src.schemas import GOLD_COLUMNS, BatchPaths
from tests.conftest import BATCH_DATE, make_orders


def messy_orders() -> pd.DataFrame:
    df = make_orders(BATCH_DATE, n=10)
    df = pd.concat([df, df.iloc[[0, 1]]], ignore_index=True)  # exact duplicates
    df.loc[2, "shipping_method"] = " standard "
    df.loc[3, "shipping_method"] = "EXPRESS"
    df.loc[4, "shipping_method"] = "same_day"
    df.loc[5, "shipping_method"] = np.nan
    df.loc[6, "distance_km"] = -8.0
    df.loc[7, "items_count"] = 0
    df.loc[8, "order_value_egp"] = -110.0
    df.loc[9, "weather_risk_level"] = np.nan
    return df


def test_bronze_is_an_unchanged_copy(cfg, write_raw):
    write_raw(BATCH_DATE, messy_orders())
    result = extract_to_bronze(BATCH_DATE, cfg)
    paths = BatchPaths.for_date(BATCH_DATE, cfg)
    assert paths.bronze.read_bytes() == paths.raw.read_bytes()
    assert result.rows_out == 12  # duplicates preserved


def test_missing_source_file_fails_clearly(cfg):
    with pytest.raises(FileNotFoundError, match="no source file for 2026-10-01"):
        extract_to_bronze(BATCH_DATE, cfg)


def test_silver_deduplicates_and_standardizes():
    silver, dropped, fixed = clean_orders(messy_orders(), BATCH_DATE)
    assert dropped["duplicate_order_id"] == 2
    assert silver["order_id"].is_unique and len(silver) == 10
    assert set(silver["shipping_method"]) <= {
        "Standard",
        "Express",
        "Same_Day",
        "Unknown",
    }
    assert (silver["shipping_method"] == "Unknown").sum() == 1
    assert (silver["weather_risk_level"] == "Unknown").sum() == 1


def test_silver_turns_invalid_numbers_into_missing():
    silver, _, fixed = clean_orders(messy_orders(), BATCH_DATE)
    assert fixed["distance_km_invalid_to_missing"] == 1
    assert fixed["items_count_invalid_to_missing"] == 1
    assert fixed["order_value_egp_invalid_to_missing"] == 1
    assert silver["distance_km"].isna().sum() == 1
    assert (silver["distance_km"].dropna() > 0).all()


def test_silver_drops_wrong_dates_and_labels():
    df = make_orders(BATCH_DATE, n=5)
    df.loc[0, "order_date"] = "2026-10-02"
    df.loc[1, "order_date"] = "not-a-date"
    df.loc[2, "late_delivery"] = 7
    silver, dropped, _ = clean_orders(df, BATCH_DATE)
    assert dropped["invalid_or_wrong_order_date"] == 2
    assert dropped["invalid_label"] == 1
    assert len(silver) == 2


def test_silver_fails_on_missing_fields():
    with pytest.raises(ValueError, match="missing required columns"):
        clean_orders(make_orders(BATCH_DATE).drop(columns="order_id"), BATCH_DATE)


def test_gold_has_engineered_features_and_target(cfg, write_raw):
    write_raw(BATCH_DATE)
    extract_to_bronze(BATCH_DATE, cfg)
    transform_to_silver(BATCH_DATE, cfg)
    prepare_gold(BATCH_DATE, cfg)
    gold = pd.read_csv(BatchPaths.for_date(BATCH_DATE, cfg).gold)
    assert tuple(gold.columns) == GOLD_COLUMNS
    assert gold["is_weekend"].isin([0, 1]).all()
    assert set(gold["late_delivery"]) == {0, 1}


@pytest.mark.parametrize(
    "mutate, check",
    [
        (lambda g: g.iloc[0:0], "not_empty"),
        (lambda g: g.drop(columns="late_delivery"), "required_columns"),
        (lambda g: pd.concat([g, g.iloc[[0]]]), "unique_order_id"),
        (lambda g: g.assign(order_date="2026-10-09"), "order_date_matches_batch"),
        (lambda g: g.assign(late_delivery=2), "valid_labels"),
    ],
)
def test_validation_fails_clearly(cfg, write_raw, mutate, check):
    write_raw(BATCH_DATE)
    run_batch(BATCH_DATE, cfg)
    gold_path = BatchPaths.for_date(BATCH_DATE, cfg).gold
    mutate(pd.read_csv(gold_path)).to_csv(gold_path, index=False)
    report = check_gold(pd.read_csv(gold_path), BATCH_DATE)
    assert report.checks[check] is False
    with pytest.raises(ValueError, match=f"Gold validation failed.*{check}"):
        validate_gold(BATCH_DATE, cfg)


def test_same_date_rerun_is_idempotent(cfg, write_raw):
    write_raw(BATCH_DATE, messy_orders())
    paths = BatchPaths.for_date(BATCH_DATE, cfg)
    run_batch(BATCH_DATE, cfg)
    first = [p.read_bytes() for p in (paths.bronze, paths.silver, paths.gold)]
    run_batch(BATCH_DATE, cfg)
    second = [p.read_bytes() for p in (paths.bronze, paths.silver, paths.gold)]
    assert first == second
    gold = pd.read_csv(paths.gold)
    assert len(gold) == 10 and gold["order_id"].is_unique


def test_simulated_failure_only_hits_the_first_attempt(cfg):
    from dataclasses import replace

    failing = replace(cfg, simulate_failure_dates=(BATCH_DATE.isoformat(),))
    with pytest.raises(RuntimeError, match="Simulated transient failure"):
        maybe_simulate_failure(BATCH_DATE, 1, failing)
    maybe_simulate_failure(BATCH_DATE, 2, failing)
    maybe_simulate_failure(BATCH_DATE, 1, cfg)
