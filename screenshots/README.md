# screenshots/

Airflow UI evidence (http://localhost:8080). Expected files:

| File | What it shows |
|---|---|
| `01_dags_list.png` | Both DAGs listed and unpaused |
| `02_data_pipeline_graph.png` | `data_pipeline` Graph view with all four tasks green |
| `03_data_pipeline_task_log.png` | A `transform_to_silver` log with the dropped/fixed counts |
| `04_retry_proof.png` | `extract_to_bronze` for 2026-10-05: attempt 1 failed (*Simulated transient failure*), attempt 2 succeeded |
| `05_same_date_rerun.png` | The 2026-10-01 run cleared and re-run, with unchanged row counts in the logs/XCom |
| `06_backfill.png` | Three `backfill__2026-10-18/19/20` runs succeeded |
| `07_gold_partitions.png` | `data/gold/` showing the `date=` partitions |
| `08_ml_pipeline_graph.png` | `ml_training_pipeline` Graph view, all tasks green |
| `09_ml_pipeline_log.png` | `evaluate_model` / `save_model` logs with the metrics |
