# src/

Reusable, Airflow-free Python package. The DAGs in [`dags/`](../dags/README.md) and the local CLI [`run.py`](../run.py) both call these functions.

| Module | Responsibility |
|---|---|
| `config.py` | Frozen `Settings` dataclass built from environment variables / `.env` (paths, split fraction, seed, log level, `SIMULATE_FAILURE_DATES`). `PROJECT_ROOT` resolves to the repo locally and to `/opt/airflow` in Docker. |
| `logging_config.py` | `configure_logging()` / `get_logger()`: one log format everywhere. |
| `schemas.py` | Column contracts per layer (`RAW_COLUMNS`, `GOLD_COLUMNS`, feature lists, allowed categories), the Silver range rules (`ValidRange`), and the dataclasses passed between steps. |
| `etl.py` | `extract_to_bronze`, `transform_to_silver` (`clean_orders`), `prepare_gold` (`add_features`), `validate_gold` (`check_gold`), `run_batch`. Every function takes a batch date and atomically rewrites that date's partition, which makes it idempotent. |
| `db.py` | `load_gold_partition`: mirrors a validated Gold partition into the `gold_orders` table of the `processed` PostgreSQL database. It deletes that date's rows and then inserts the partition in one transaction, so it is idempotent. It is skipped when `PROCESSED_DB_URI` is empty. |
| `train.py` | `load_gold_data`, `time_split`, `build_pipeline`, `train_model`, `evaluate_model` (`compute_metrics`), `save_model`, `run_training`. |

## Dataclasses (`schemas.py`)

| Dataclass | Purpose |
|---|---|
| `ValidRange` | One Silver range rule: out-of-range values become missing |
| `BatchPaths` | Where one batch date lives in every layer |
| `LayerResult` | Rows in/out plus dropped/fixed counts of a layer step (sent to XCom) |
| `ValidationReport` | Named Gold checks, their failures, `raise_if_failed()` |
| `SplitInfo` | Time-based split: train/test dates, row counts, late rates |
| `TrainingMetrics` | Accuracy, precision, recall, F1, confusion matrix |
| `ModelReport` | Everything written to `metrics/metrics.json` |

Every function takes an optional `cfg: Settings`, so it can be pointed at another data directory, such as a temp dir in tests.
