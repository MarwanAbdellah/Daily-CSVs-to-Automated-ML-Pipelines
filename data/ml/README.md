# data/ml/

Hand-off files between the tasks of `ml_training_pipeline`. They are re-created on every run, and git ignores everything except this README.

| File | Written by | Content |
|---|---|---|
| `gold_combined.csv` | `load_gold_data` | All Gold partitions concatenated |
| `candidate_model.joblib` | `train_model` | Fitted pipeline awaiting evaluation |
| `split.json` | `train_model` | `SplitInfo`: train/test dates and counts |
| `candidate_metrics.json` | `evaluate_model` | `TrainingMetrics` on the test dates |
