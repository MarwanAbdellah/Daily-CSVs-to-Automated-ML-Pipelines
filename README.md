# Daily CSVs to Automated ML Pipelines

Date-aware **delivery-delay prediction** for an Egyptian e-commerce operation.
46 daily order files go through a **Bronze → Silver → Gold** medallion pipeline orchestrated by **Apache Airflow 3**, then a second DAG trains, evaluates and saves a **scikit-learn** classifier for `late_delivery` (0 = on time, 1 = late). Everything runs in **Docker Compose**.

> The goal is a reproducible, idempotent, date-aware pipeline. Model accuracy comes second.

| Stage | DAG | Tasks |
|---|---|---|
| 01 Data engineering | `data_pipeline` (daily) | `extract_to_bronze → transform_to_silver → prepare_gold → validate_gold` |
| 02 Orchestrated ML | `ml_training_pipeline` (manual) | `load_gold_data → train_model → evaluate_model → save_model` |

## Repository layout

```
dags/                 Airflow DAG definitions (thin wrappers)       → dags/README.md
src/                  Reusable, Airflow-free Python modules         → src/README.md
data/raw/             46 source CSVs, never modified                → data/README.md
data/bronze|silver|gold/date=YYYY-MM-DD/orders.csv   one partition per batch date
data/ml/              training intermediates (git-ignored)
models/               delivery_delay_model.joblib                   → models/README.md
metrics/              metrics.json                                  → metrics/README.md
screenshots/          Airflow UI evidence                           → screenshots/README.md
Dockerfile            Airflow 3.3.2 image + project requirements
docker-compose.yml    Postgres + API server + scheduler + DAG processor + triggerer
requirements.txt      Pinned runtime deps (installed with Airflow constraints)
pyproject.toml        Local dev environment (uv), ruff + black config
run.py                Run the same pipelines without Airflow
data_dictionary.csv   Column definitions and validation notes
.env.example          Every setting, documented
```

The architecture follows the previous ML-production project. Settings live in a frozen `Settings` dataclass built from `.env` ([src/config.py](src/config.py)). Logging is centralised, modules are small and single-purpose, and DAGs only orchestrate: all logic is plain Python you can import, run or test without Airflow.

## Dataset

46 files `data/raw/orders_2026-10-01.csv` … `orders_2026-11-15.csv`, 10,219 rows in total, all with the same 15 columns ([data_dictionary.csv](data_dictionary.csv)).

| Kind | Columns |
|---|---|
| Identifier / date | `order_id`, `order_date` (equals the file's batch date) |
| Categorical | `customer_region`, `warehouse_id`, `shipping_method`, `weather_risk_level` |
| Numeric | `order_hour`, `distance_km`, `items_count`, `package_weight_kg`, `order_value_egp`, `promised_delivery_days`, `warehouse_load_pct`, `carrier_on_time_rate` |
| Label | `late_delivery` (binary) |

## Stage A: data exploration and assessment

Three dated batches were inspected in pandas, then the whole dataset:

| Batch | Rows | Duplicate `order_id` | Missing values | Invalid values | Non-canonical `shipping_method` | Late % |
|---|---|---|---|---|---|---|
| 2026-10-01 | 183 | 3 | distance 3, warehouse_load 4, weather 2 | distance ≤ 0: 1, items = 0: 2 | 3 | 24.6 |
| 2026-10-02 | 196 | 3 | shipping 1, distance 3, weather 1, carrier_rate 1 | order_value ≤ 0: 2, items = 0: 1 | 3 | 20.9 |
| 2026-10-03 | 209 | 3 | shipping 1, distance 1, warehouse_load 3, weather 1, carrier_rate 2 | distance ≤ 0: 3 | 1 | 21.5 |
| **All 46** | **10,219** | **166** | shipping 74, distance 142, warehouse_load 149, weather 94, carrier_rate 87 | distance ≤ 0: 81, order_value ≤ 0: 55, items = 0: 98 | 288 (` standard `, `EXPRESS`, `same_day`) | **23.7** |

* Every duplicate is an exact copy of another row.
* All labels are valid 0/1, and every `order_date` matches its file's date.
* The classes are imbalanced, about 76 / 24.

### What is fixed in Silver, and what is learned in training

| Problem | Where | Why |
|---|---|---|
| Duplicate `order_id` | **Silver**: keep first | Deterministic rule; the same answer whatever data comes later |
| Category spelling (`EXPRESS`, ` standard `) | **Silver**: map to canonical value | Deterministic rule |
| Missing / unknown category | **Silver**: `Unknown` | An explicit level instead of a guess |
| Out-of-range numbers (≤ 0 distance/value, 0 items, rate ∉ [0,1] …) | **Silver**: set to missing | Invalid values are errors, not data |
| Wrong/unparseable date, label ∉ {0,1} | **Silver**: drop row (counted) | No trustworthy record left |
| Missing numeric values | **Training pipeline**: `SimpleImputer(median)` | A median is *learned*; fitting it on all data would leak test info |
| Scaling, one-hot encoding | **Training pipeline** | Learned from training dates only |

## Stage B: medallion layers ([src/etl.py](src/etl.py))

* **Bronze**: an unchanged copy of the day's raw file, bad records and duplicates included.
* **Silver**: the rules in the table above. Every rule's count is logged and returned as a `LayerResult` (XCom).
* **Gold**: IDs, model features and target. It adds five row-wise engineered features:
  * `day_of_week`
  * `is_weekend` (Fri/Sat, the Egyptian weekend)
  * `is_after_cutoff` (ordered at or after 18:00, so it usually ships the next day)
  * `weight_per_item`
  * `distance_per_promised_day` (how tight the delivery promise is)
* **Validation**: fails with a clear message listing every failed check:
  * empty partition
  * missing columns
  * duplicate `order_id`
  * `order_date` ≠ batch date
  * label ∉ {0,1}

**Idempotency.** Every write goes to a temp file and is then atomically renamed to `data/<layer>/date=YYYY-MM-DD/orders.csv`. A rerun replaces a date's partition and never appends to it.

## The two DAGs

**`data_pipeline`** ([dags/data_pipeline.py](dags/data_pipeline.py))
* Schedule: `@daily`, `start_date` 2026-10-01, `end_date` 2026-11-15, `catchup=False`, `max_active_runs=1`.
* Retries: `retries=2`, `retry_delay` 30 s.
* The batch date is the run's **logical date**, so each run processes exactly one source file.

**`ml_training_pipeline`** ([dags/ml_training_pipeline.py](dags/ml_training_pipeline.py))
* Triggered manually once Gold is valid; `retries=2`.
* Tasks pass small JSON summaries through XCom. Data and the candidate model go through `data/ml/`.

## ML approach and leakage prevention ([src/train.py](src/train.py))

* **Split**: time-based. The latest 20 % of Gold dates are the test set, and all earlier dates are training data. A model is never evaluated on days older than the ones it learned from.
* **Pipeline**: `ColumnTransformer` (numeric: median impute + `StandardScaler`; categorical: most-frequent impute + `OneHotEncoder(handle_unknown="ignore")`) → `LogisticRegression(class_weight="balanced")`.
  * The whole pipeline is fitted on training rows only.
  * The balanced class weights handle the 76/24 imbalance, which favours recall on late orders.
* **Artifacts**: `models/delivery_delay_model.joblib` and `metrics/metrics.json`. The metrics file records accuracy / precision / recall / F1, the confusion matrix, the split strategy, the train/test dates and row counts, and the feature lists. **Versioning** (`feature/model-versioning`): every run also stores `models/versions/<UTC timestamp>/{model.joblib,metrics.json}`, and `metrics.json` carries the `model_version`. After saving, a round-trip check confirms the reloaded model reproduces the evaluated predictions.

### Results (current run: 8 Gold dates)

| Split | Train rows | Test rows | Accuracy | Precision | Recall | F1 |
|---|---|---|---|---|---|---|
| latest 2 of 8 dates held out | 1,236 | 399 | 0.694 | 0.313 | 0.687 | 0.430 |

The model catches about 69 % of late deliveries at the cost of false alarms. That is a sensible trade-off when a missed late order costs more than a proactive customer notice.

## Setup and reproduction

Prerequisites: Docker Desktop (≥ 4 GB RAM for Docker) and Git.

```bash
git clone https://github.com/MarwanAbdellah/Daily-CSVs-to-Automated-ML-Pipelines.git
```

```bash
cd Daily-CSVs-to-Automated-ML-Pipelines && cp .env.example .env
```

```bash
docker compose up -d --build
```

Open http://localhost:8080 and log in as `airflow` / `airflow` (set in `.env`). Then, with every command run inside the scheduler container:

```bash
docker compose exec airflow-scheduler airflow dags unpause data_pipeline
```

```bash
docker compose exec airflow-scheduler airflow dags trigger data_pipeline --logical-date 2026-10-01T00:00:00+00:00
```

(Repeat for 2026-10-02 and 2026-10-03, or pick a logical date in the UI's *Trigger* dialog.)

```bash
docker compose exec airflow-scheduler airflow dags trigger ml_training_pipeline
```

### Reliability evidence

| Check | How |
|---|---|
| **Retry** | Set `SIMULATE_FAILURE_DATES=2026-10-05` in `.env`, run `docker compose up -d`, then trigger that date. `extract_to_bronze` fails attempt 1 with *Simulated transient failure* and succeeds on attempt 2. Clear the variable afterwards. |
| **Same-date rerun** | `airflow tasks clear data_pipeline -s 2026-10-01 -e 2026-10-01 -y`, twice. Bronze/Silver/Gold checksums and row counts are identical (181 Gold rows) and there are no duplicate `order_id`s. |
| **Backfill** | `airflow backfill create --dag-id data_pipeline --from-date 2026-10-18 --to-date 2026-10-20` produces `gold/date=2026-10-18`, `-19` and `-20`. |
| **Monitoring** | Graph view, task states and task logs for both DAGs in the UI. Raw logs are in `logs/`. |

All four checks above were run for this repository; the committed `data/` partitions and `metrics/` come from those runs.

### Without Airflow

```bash
uv sync
```

```bash
uv run python run.py etl --date 2026-10-01 --date 2026-10-02 --date 2026-10-03
```

```bash
uv run python run.py train
```

## Optional bonus work

Each bonus lives on its own branch off `main`:

| Branch | Adds |
|---|---|
| `feature/etl-tests` | pytest suite for Silver rules, validation failures, idempotency, leakage-safe training and DAG integrity |
| `feature/auto-trigger-ml` | `validate_gold` emits an Airflow Asset that schedules `ml_training_pipeline` |
| `feature/model-versioning` | Each training also stores `models/versions/<UTC timestamp>/` with model + metrics |
| `feature/postgres-processed-data` | Gold partitions are also loaded idempotently into a PostgreSQL `processed` database |
