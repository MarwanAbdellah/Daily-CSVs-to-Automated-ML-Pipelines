# screenshots/

Evidence for the reliability, monitoring and bonus requirements. They were captured from the Airflow UI (http://localhost:8080) and VS Code.

| File | What it shows |
|---|---|
| `01_dags_list.png` | Both DAGs unpaused. `ml_training_pipeline` is scheduled on the `gold_orders` Asset |
| `02_data_pipeline_graph.png` | Graph view of the 2026-10-01 run: all five `data_pipeline` tasks green, including `load_gold_to_postgres` |
| `03_data_pipeline_task_log.png` | The `transform_to_silver` log, with Silver going from 183 to 180 rows and the dropped/fixed counts |
| `04_retry_proof.png` | **Retry**: `extract_to_bronze` for 2026-10-05, where try 1 failed (simulated transient failure) and try 2 succeeded |
| `05_same_date_rerun.png` | **Idempotency**: the 2026-10-01 run executed five times, and `validate_gold` reports the same 180 rows every time |
| `06_backfill.png` | **Backfill**: the three `backfill__2026-10-18/19/20` runs succeeded |
| `07_gold_partitions.png` | `data/gold/` with one `date=YYYY-MM-DD` partition per processed day |
| `08_ml_pipeline_graph.png` | `ml_training_pipeline` run of type *Asset Triggered*, with all four tasks green |
| `09_ml_pipeline_log.png` | The `evaluate_model` log, with accuracy, precision, recall, F1 and the confusion matrix on the held-out dates |
| `10_postgres_gold_orders.png` | **PostgreSQL bonus**: `processed.gold_orders` queried in VS Code. All 11 dates are present, with `rows = unique_ids` on every date |
| `11_assets.png` | **Auto-trigger bonus**: the `gold_orders` Asset, produced by one task and scheduling `ml_training_pipeline` |
