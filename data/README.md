# data/

The medallion layers. Each processed layer holds **one partition per batch date**, `date=YYYY-MM-DD/orders.csv`. A partition is rewritten in full on every run, so reruns never append or duplicate rows.

| Folder | Content | Written by |
|---|---|---|
| [`raw/`](raw/README.md) | The 46 source CSVs `orders_YYYY-MM-DD.csv`, never modified | provided |
| [`bronze/`](bronze/README.md) | Unchanged copy of the day's raw file | `extract_to_bronze` |
| [`silver/`](silver/README.md) | Deduplicated, standardised, type-cast data with invalid values set to missing | `transform_to_silver` |
| [`gold/`](gold/README.md) | Model-ready features + target, validated | `prepare_gold`, `validate_gold` |
| [`ml/`](ml/README.md) | Training intermediates (git-ignored) | `ml_training_pipeline` |

The committed partitions come from the evidence runs:

- 2026-10-01, -02, -03 and -05: manual runs, including the retry
- 2026-10-18, -19 and -20: backfill
- 2026-10-04: the manual run whose valid Gold auto-triggered training
- 2026-11-02, -03 and -04: scheduled daily runs
