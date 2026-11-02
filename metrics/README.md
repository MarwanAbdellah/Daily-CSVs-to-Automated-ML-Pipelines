# metrics/

`metrics.json` is written by `save_model` next to the model, as a separate artifact. It serialises the `ModelReport` dataclass:

| Field | Content |
|---|---|
| `model_version`, `model_type`, `trained_at`, `model_path` | What was saved, and when |
| `gold_rows`, `gold_partitions` | How much validated data was used |
| `features` | The categorical, numeric and engineered feature lists |
| `split` | Time-based strategy, train/test dates, row counts, late rate per side |
| `metrics` | Accuracy, precision, recall, F1 and the confusion matrix on the held-out dates |

A copy of each version's report is kept in `models/versions/<model_version>/metrics.json`.
