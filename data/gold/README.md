# data/gold/

`date=YYYY-MM-DD/orders.csv` holds the model-ready rows of one day:

- IDs: `order_id`, `order_date`
- 4 categorical and 8 numeric features
- 5 engineered features
- the target `late_delivery`

| Engineered feature | Meaning |
|---|---|
| `day_of_week` | 0 = Monday … 6 = Sunday |
| `is_weekend` | Friday or Saturday (Egyptian weekend) |
| `is_after_cutoff` | Ordered at or after 18:00, so it usually ships the next day |
| `weight_per_item` | `package_weight_kg / items_count` |
| `distance_per_promised_day` | `distance_km / promised_delivery_days`: how tight the promise is |

All engineered features are row-wise, so a day can be computed without the other days and without leakage.

`validate_gold` fails the run with a message listing every failed check. The checks are:

- the partition is empty
- required columns are missing
- `order_id` is duplicated
- an `order_date` differs from the batch date
- a label is not 0 or 1
