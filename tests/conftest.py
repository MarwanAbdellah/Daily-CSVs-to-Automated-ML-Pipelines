from datetime import date, timedelta

import pandas as pd
import pytest

from src.config import Settings
from src.schemas import RAW_COLUMNS

BATCH_DATE = date(2026, 10, 1)


def make_orders(batch_date: date, n: int = 20, seed: int = 0) -> pd.DataFrame:
    """n clean orders for one day, with both labels present."""
    rows = []
    for i in range(n):
        k = i + seed
        rows.append(
            {
                "order_id": f"ORD-{batch_date:%Y%m%d}-{i:04d}",
                "order_date": batch_date.isoformat(),
                "order_hour": k % 24,
                "customer_region": ("Cairo", "Giza", "Delta")[k % 3],
                "warehouse_id": f"WH-0{k % 4 + 1}",
                "shipping_method": ("Standard", "Express", "Same_Day")[k % 3],
                "distance_km": 10.0 + k,
                "items_count": k % 5 + 1,
                "package_weight_kg": 1.5 + k % 7,
                "order_value_egp": 100.0 + 10 * k,
                "promised_delivery_days": k % 4 + 1,
                "warehouse_load_pct": 50.0 + k % 40,
                "weather_risk_level": ("Low", "Medium", "High")[k % 3],
                "carrier_on_time_rate": 0.7 + (k % 3) / 10,
                "late_delivery": int(k % 4 == 0),
            }
        )
    return pd.DataFrame(rows, columns=list(RAW_COLUMNS))


@pytest.fixture
def cfg(tmp_path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        models_dir=tmp_path / "models",
        metrics_dir=tmp_path / "metrics",
    )


@pytest.fixture
def write_raw(cfg):
    """Write a raw daily file the way the source system delivers it."""

    def _write(batch_date: date, df: pd.DataFrame | None = None) -> pd.DataFrame:
        df = make_orders(batch_date) if df is None else df
        cfg.raw_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(cfg.raw_dir / f"orders_{batch_date.isoformat()}.csv", index=False)
        return df

    return _write


def batch_dates(n: int) -> list[date]:
    return [BATCH_DATE + timedelta(days=i) for i in range(n)]
