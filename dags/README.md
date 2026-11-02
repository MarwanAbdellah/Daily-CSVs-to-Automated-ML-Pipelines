# dags/

Airflow 3 DAG definitions. The DAGs only **orchestrate**. Every task calls a function from [`src/`](../src/README.md), so the logic can be run and tested without Airflow. The Airflow containers mount this folder at `/opt/airflow/dags`, with `PYTHONPATH=/opt/airflow` so that `import src...` works.

| Module | DAG id | Schedule | Tasks |
|---|---|---|---|
| `data_pipeline.py` | `data_pipeline` | `@daily`, 2026-10-01 → 2026-11-15, `catchup=False`, `max_active_runs=1` | `extract_to_bronze → transform_to_silver → prepare_gold → validate_gold` |
| `ml_training_pipeline.py` | `ml_training_pipeline` | manual (`schedule=None`) | `load_gold_data → train_model → evaluate_model → save_model` |

Both DAGs use `retries=2` with a 30-second `retry_delay`.

## `data_pipeline.py`

- The batch date is the run's **logical date**. Scheduled runs, manual runs with a chosen logical date, task clears and backfills therefore each process exactly one source file.
- Each task returns a `LayerResult` / `ValidationReport` as a small dict. That dict goes to XCom and is visible in the UI.
- `extract_to_bronze` calls `maybe_simulate_failure`. When the batch date is listed in `SIMULATE_FAILURE_DATES`, the first attempt fails on purpose. This is safe retry evidence and is off by default.

- `load_gold_to_postgres` (bonus) runs only after validation passes. It replaces the date's rows in `processed.gold_orders`.

## `ml_training_pipeline.py`

- XCom carries only summaries: row counts, the split and the metrics. The data and the candidate model move through files in `data/ml/`, so any task can be retried on its own.
- `save_model` publishes `models/delivery_delay_model.joblib` and `metrics/metrics.json`.
