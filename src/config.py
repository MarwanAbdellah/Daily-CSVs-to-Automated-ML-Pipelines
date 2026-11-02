import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Repository root locally, /opt/airflow inside the containers (src/ is mounted there)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    """Every runtime setting, in one object, with safe defaults."""

    data_dir: Path = PROJECT_ROOT / "data"
    models_dir: Path = PROJECT_ROOT / "models"
    metrics_dir: Path = PROJECT_ROOT / "metrics"
    log_level: str = "INFO"
    environment: str = "development"
    test_fraction: float = 0.2
    random_state: int = 42
    # Batch dates whose first attempt fails on purpose (retry evidence)
    simulate_failure_dates: tuple[str, ...] = field(default_factory=tuple)

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def ml_dir(self) -> Path:
        return self.data_dir / "ml"

    @property
    def model_path(self) -> Path:
        return self.models_dir / "delivery_delay_model.joblib"

    @property
    def metrics_path(self) -> Path:
        return self.metrics_dir / "metrics.json"

    @classmethod
    def from_env(cls) -> "Settings":
        """Build settings from environment variables (.env), else use defaults."""
        defaults = cls()
        failure_dates = os.getenv("SIMULATE_FAILURE_DATES", "")
        return cls(
            data_dir=Path(os.getenv("DATA_DIR", str(defaults.data_dir))),
            models_dir=Path(os.getenv("MODELS_DIR", str(defaults.models_dir))),
            metrics_dir=Path(os.getenv("METRICS_DIR", str(defaults.metrics_dir))),
            log_level=os.getenv("LOG_LEVEL", defaults.log_level),
            environment=os.getenv("ENVIRONMENT", defaults.environment),
            test_fraction=float(os.getenv("TEST_FRACTION", defaults.test_fraction)),
            random_state=int(os.getenv("RANDOM_STATE", defaults.random_state)),
            simulate_failure_dates=tuple(
                d.strip() for d in failure_dates.split(",") if d.strip()
            ),
        )


settings = Settings.from_env()
