# tests/

A pytest suite for the ETL, the training and the DAG wiring. The ETL and training tests build tiny synthetic batches in a temp directory, so they never touch `data/`.

| Module | Covers |
|---|---|
| `conftest.py` | `make_orders()` synthetic daily batch, a `cfg` fixture (a `Settings` pointing at `tmp_path`) and `write_raw` to drop a raw file |
| `test_etl.py` | Bronze is a byte-for-byte copy; a missing source file fails; Silver deduplication, category standardisation, invalid → missing, wrong date / label rows dropped, missing fields fail; Gold columns and features; every validation failure (empty, missing column, duplicate ID, wrong date, bad label); same-date rerun gives identical files; simulated failure hits only attempt 1 |
| `test_train.py` | Gold partitions combine; the time split keeps every test date after every train date; **imputer statistics equal the training-set medians**, which shows there is no leakage; `run_training` writes the model and `metrics.json` |
| `test_dags.py` | Both DAGs import cleanly, tasks are chained in the required order, `retries >= 2` everywhere, and `data_pipeline` is daily. It is skipped automatically when Airflow is not installed. |

Run locally (ETL + training tests; the DAG tests are skipped):

```bash
uv run pytest
```

Run everything, including the DAG tests, inside Airflow:

```bash
docker compose exec airflow-scheduler python -m pytest -p no:cacheprovider tests
```
