"""Column contracts for every layer, plus the dataclasses passed between steps."""

from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from src.config import Settings

# Raw / Bronze / Silver: the 15-column source schema (data_dictionary.csv)
RAW_COLUMNS = (
    "order_id",
    "order_date",
    "order_hour",
    "customer_region",
    "warehouse_id",
    "shipping_method",
    "distance_km",
    "items_count",
    "package_weight_kg",
    "order_value_egp",
    "promised_delivery_days",
    "warehouse_load_pct",
    "weather_risk_level",
    "carrier_on_time_rate",
    "late_delivery",
)
ID_COLUMNS = ("order_id", "order_date")
TARGET = "late_delivery"
VALID_LABELS = (0, 1)

UNKNOWN = "Unknown"
CUSTOMER_REGIONS = ("Cairo", "Giza", "Alexandria", "Delta", "Upper_Egypt", "Canal")
WAREHOUSES = ("WH-01", "WH-02", "WH-03", "WH-04")
SHIPPING_METHODS = ("Standard", "Express", "Same_Day")
WEATHER_RISK_LEVELS = ("Low", "Medium", "High")


@dataclass(frozen=True)
class ValidRange:
    """Silver rule: values outside [low, high] are invalid and become missing."""

    column: str
    low: float | None = None
    high: float | None = None
    low_inclusive: bool = True


# Integer columns are stored as nullable integers in Silver
INTEGER_COLUMNS = ("order_hour", "items_count", "promised_delivery_days")
NUMERIC_RULES = (
    ValidRange("order_hour", 0, 23),
    ValidRange("distance_km", 0, None, low_inclusive=False),
    ValidRange("items_count", 1, None),
    ValidRange("package_weight_kg", 0, None, low_inclusive=False),
    ValidRange("order_value_egp", 0, None, low_inclusive=False),
    ValidRange("promised_delivery_days", 1, None),
    ValidRange("warehouse_load_pct", 0, 100),
    ValidRange("carrier_on_time_rate", 0, 1),
)

# Gold: model inputs. Engineered features are row-wise only, so they can be
# computed per daily partition without leaking information across rows.
ENGINEERED_FEATURES = (
    "day_of_week",
    "is_weekend",
    "is_after_cutoff",
    "weight_per_item",
    "distance_per_promised_day",
)
CATEGORICAL_FEATURES = (
    "customer_region",
    "warehouse_id",
    "shipping_method",
    "weather_risk_level",
)
NUMERIC_FEATURES = (
    "order_hour",
    "distance_km",
    "items_count",
    "package_weight_kg",
    "order_value_egp",
    "promised_delivery_days",
    "warehouse_load_pct",
    "carrier_on_time_rate",
    *ENGINEERED_FEATURES,
)
FEATURES = CATEGORICAL_FEATURES + NUMERIC_FEATURES
GOLD_COLUMNS = ID_COLUMNS + FEATURES + (TARGET,)


@dataclass(frozen=True)
class BatchPaths:
    """Where one batch date lives in every layer (one partition per date)."""

    batch_date: date
    raw: Path
    bronze: Path
    silver: Path
    gold: Path

    @classmethod
    def for_date(cls, batch_date: date, settings: Settings) -> "BatchPaths":
        partition = f"date={batch_date.isoformat()}"
        layer = settings.data_dir
        return cls(
            batch_date=batch_date,
            raw=settings.raw_dir / f"orders_{batch_date.isoformat()}.csv",
            bronze=layer / "bronze" / partition / "orders.csv",
            silver=layer / "silver" / partition / "orders.csv",
            gold=layer / "gold" / partition / "orders.csv",
        )


@dataclass
class LayerResult:
    """What one layer step did; small enough to travel through XCom."""

    layer: str
    batch_date: str
    path: str
    rows_in: int
    rows_out: int
    dropped: dict[str, int] = field(default_factory=dict)
    fixed: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationReport:
    """Outcome of every Gold check for one batch date."""

    batch_date: str
    row_count: int
    checks: dict[str, bool] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)

    def add(self, name: str, passed: bool, detail: str) -> None:
        self.checks[name] = passed
        if not passed:
            self.failures.append(f"{name}: {detail}")

    @property
    def passed(self) -> bool:
        return not self.failures

    def raise_if_failed(self) -> None:
        if self.failures:
            raise ValueError(
                f"Gold validation failed for {self.batch_date}: "
                + "; ".join(self.failures)
            )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SplitInfo:
    """Time-based split: earlier dates train, later dates test."""

    strategy: str
    train_dates: list[str]
    test_dates: list[str]
    train_rows: int
    test_rows: int
    train_late_rate: float
    test_late_rate: float


@dataclass
class TrainingMetrics:
    accuracy: float
    precision: float
    recall: float
    f1: float
    confusion_matrix: list[list[int]]


@dataclass
class ModelReport:
    """Everything written to metrics/metrics.json next to the saved model."""

    model_type: str
    trained_at: str
    gold_rows: int
    gold_partitions: int
    features: dict[str, list[str]]
    split: SplitInfo
    metrics: TrainingMetrics
    model_path: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
