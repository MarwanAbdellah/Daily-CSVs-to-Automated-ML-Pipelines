"""Bronze, Silver, Gold and validation as reusable, dated, idempotent functions.

Every function takes the batch (logical) date and rewrites that date's
partition in full, so rerunning a date replaces its files instead of
appending to them.
"""

import os
import shutil
from datetime import date
from pathlib import Path

import pandas as pd

from src.config import Settings, settings
from src.logging_config import get_logger
from src.schemas import (
    CUSTOMER_REGIONS,
    GOLD_COLUMNS,
    INTEGER_COLUMNS,
    NUMERIC_RULES,
    RAW_COLUMNS,
    SHIPPING_METHODS,
    TARGET,
    UNKNOWN,
    VALID_LABELS,
    WAREHOUSES,
    WEATHER_RISK_LEVELS,
    BatchPaths,
    LayerResult,
    ValidationReport,
)

logger = get_logger(__name__)

# Orders placed at or after this hour are usually handed to the carrier next day
CUTOFF_HOUR = 18
# Egyptian weekend: Friday (4) and Saturday (5)
WEEKEND_DAYS = (4, 5)


def parse_batch_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write to a temp file, then replace: a rerun never leaves half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)


def _read_layer(path: Path, layer: str) -> pd.DataFrame:
    if not path.exists():
        logger.error("%s partition not found: %s", layer, path)
        raise FileNotFoundError(f"{layer} partition not found: {path}")
    return pd.read_csv(path)


def _require_columns(df: pd.DataFrame, columns: tuple[str, ...], where: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"{where} is missing required columns: {missing}")


def maybe_simulate_failure(
    batch_date: date, try_number: int, cfg: Settings = settings
) -> None:
    """Fail the first attempt for configured dates, to prove Airflow retries."""
    if batch_date.isoformat() in cfg.simulate_failure_dates and try_number <= 1:
        raise RuntimeError(
            f"Simulated transient failure for {batch_date} (attempt {try_number}); "
            "the retry is expected to succeed"
        )


def extract_to_bronze(batch_date: date, cfg: Settings = settings) -> LayerResult:
    """Copy the day's raw CSV unchanged, bad records and duplicates included."""
    paths = BatchPaths.for_date(batch_date, cfg)
    if not paths.raw.exists():
        logger.error("No source file for %s: %s", batch_date, paths.raw)
        raise FileNotFoundError(f"no source file for {batch_date}: {paths.raw}")
    paths.bronze.parent.mkdir(parents=True, exist_ok=True)
    tmp = paths.bronze.with_name(paths.bronze.name + ".tmp")
    shutil.copyfile(paths.raw, tmp)
    os.replace(tmp, paths.bronze)
    rows = len(pd.read_csv(paths.bronze))
    logger.info("Bronze %s: copied %d rows to %s", batch_date, rows, paths.bronze)
    return LayerResult("bronze", batch_date.isoformat(), str(paths.bronze), rows, rows)


def _normalize_category(
    series: pd.Series, allowed: tuple[str, ...]
) -> tuple[pd.Series, int]:
    """Case/space/dash-insensitive match to the allowed spelling, else Unknown."""
    lookup = {a.lower().replace("-", "_"): a for a in allowed}
    keys = series.astype("string").str.strip().str.lower().str.replace("-", "_")
    keys = keys.str.replace(r"\s+", "_", regex=True)
    normalized = keys.map(lookup).fillna(UNKNOWN)
    changed = int((normalized != series.astype("string").fillna("")).sum())
    return normalized.astype(str), changed


def clean_orders(df: pd.DataFrame, batch_date: date) -> tuple[pd.DataFrame, dict, dict]:
    """Silver rules on one day of orders. Returns (clean frame, dropped, fixed)."""
    _require_columns(df, RAW_COLUMNS, "Bronze data")
    df = df[list(RAW_COLUMNS)].copy()
    dropped: dict[str, int] = {}
    fixed: dict[str, int] = {}

    df["order_id"] = df["order_id"].astype("string").str.strip()
    missing_id = df["order_id"].isna() | (df["order_id"] == "")
    dropped["missing_order_id"] = int(missing_id.sum())
    df = df[~missing_id]
    before = len(df)
    df = df.drop_duplicates(subset="order_id", keep="first")
    dropped["duplicate_order_id"] = before - len(df)

    order_date = pd.to_datetime(df["order_date"], format="%Y-%m-%d", errors="coerce")
    wrong_date = order_date.dt.date != batch_date
    dropped["invalid_or_wrong_order_date"] = int(wrong_date.sum())
    df = df[~wrong_date]

    label = pd.to_numeric(df[TARGET], errors="coerce")
    bad_label = ~label.isin(VALID_LABELS)
    dropped["invalid_label"] = int(bad_label.sum())
    df = df[~bad_label].copy()
    df[TARGET] = label[~bad_label].astype(int)

    for column, allowed in (
        ("customer_region", CUSTOMER_REGIONS),
        ("warehouse_id", WAREHOUSES),
        ("shipping_method", SHIPPING_METHODS),
        ("weather_risk_level", WEATHER_RISK_LEVELS),
    ):
        df[column], fixed[f"{column}_standardized"] = _normalize_category(
            df[column], allowed
        )

    for rule in NUMERIC_RULES:
        values = pd.to_numeric(df[rule.column], errors="coerce")
        invalid = pd.Series(False, index=df.index)
        if rule.low is not None:
            invalid |= values < rule.low if rule.low_inclusive else values <= rule.low
        if rule.high is not None:
            invalid |= values > rule.high
        fixed[f"{rule.column}_invalid_to_missing"] = int(invalid.sum())
        df[rule.column] = values.mask(invalid)
    for column in INTEGER_COLUMNS:
        df[column] = df[column].round().astype("Int64")

    df = df.sort_values("order_id").reset_index(drop=True)
    return df, dropped, fixed


def transform_to_silver(batch_date: date, cfg: Settings = settings) -> LayerResult:
    paths = BatchPaths.for_date(batch_date, cfg)
    bronze = _read_layer(paths.bronze, "Bronze")
    silver, dropped, fixed = clean_orders(bronze, batch_date)
    _atomic_write_csv(silver, paths.silver)
    logger.info(
        "Silver %s: %d -> %d rows, dropped=%s fixed=%s",
        batch_date,
        len(bronze),
        len(silver),
        dropped,
        fixed,
    )
    return LayerResult(
        "silver",
        batch_date.isoformat(),
        str(paths.silver),
        len(bronze),
        len(silver),
        dropped,
        fixed,
    )


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """Row-wise engineered features (no statistics learned across rows)."""
    df = df.copy()
    weekday = pd.to_datetime(df["order_date"]).dt.dayofweek
    df["day_of_week"] = weekday
    df["is_weekend"] = weekday.isin(WEEKEND_DAYS).astype(int)
    df["is_after_cutoff"] = (df["order_hour"] >= CUTOFF_HOUR).astype("Int64")
    df["weight_per_item"] = (df["package_weight_kg"] / df["items_count"]).round(4)
    df["distance_per_promised_day"] = (
        df["distance_km"] / df["promised_delivery_days"]
    ).round(4)
    return df


def prepare_gold(batch_date: date, cfg: Settings = settings) -> LayerResult:
    paths = BatchPaths.for_date(batch_date, cfg)
    silver = _read_layer(paths.silver, "Silver")
    _require_columns(silver, RAW_COLUMNS, "Silver data")
    gold = add_features(silver)[list(GOLD_COLUMNS)]
    _atomic_write_csv(gold, paths.gold)
    logger.info("Gold %s: %d rows -> %s", batch_date, len(gold), paths.gold)
    return LayerResult(
        "gold", batch_date.isoformat(), str(paths.gold), len(silver), len(gold)
    )


def check_gold(df: pd.DataFrame, batch_date: date) -> ValidationReport:
    report = ValidationReport(batch_date.isoformat(), len(df))
    report.add("not_empty", len(df) > 0, "Gold partition has no rows")
    missing = [c for c in GOLD_COLUMNS if c not in df.columns]
    report.add("required_columns", not missing, f"missing columns {missing}")
    if "order_id" in df.columns:
        dupes = df["order_id"][df["order_id"].duplicated()].unique().tolist()
        report.add("unique_order_id", not dupes, f"duplicate order_id {dupes[:5]}")
    if "order_date" in df.columns:
        parsed = pd.to_datetime(df["order_date"], format="%Y-%m-%d", errors="coerce")
        bad = df["order_date"][parsed.dt.date != batch_date].unique().tolist()
        report.add(
            "order_date_matches_batch",
            not bad,
            f"order_date values {bad[:5]} do not equal {batch_date}",
        )
    if TARGET in df.columns:
        bad = df[TARGET][~df[TARGET].isin(VALID_LABELS)].unique().tolist()
        report.add("valid_labels", not bad, f"labels {bad[:5]} not in {VALID_LABELS}")
    return report


def validate_gold(batch_date: date, cfg: Settings = settings) -> ValidationReport:
    """Fail clearly on empty data, missing fields, duplicate IDs, dates, labels."""
    paths = BatchPaths.for_date(batch_date, cfg)
    report = check_gold(_read_layer(paths.gold, "Gold"), batch_date)
    if report.passed:
        logger.info(
            "Gold %s valid: %d rows, %s", batch_date, report.row_count, report.checks
        )
    else:
        logger.error("Gold %s invalid: %s", batch_date, report.failures)
    report.raise_if_failed()
    return report


def run_batch(batch_date: date, cfg: Settings = settings) -> ValidationReport:
    """The whole data pipeline for one date, outside Airflow."""
    extract_to_bronze(batch_date, cfg)
    transform_to_silver(batch_date, cfg)
    prepare_gold(batch_date, cfg)
    return validate_gold(batch_date, cfg)
