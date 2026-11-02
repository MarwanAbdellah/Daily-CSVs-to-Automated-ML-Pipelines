# data/silver/

`date=YYYY-MM-DD/orders.csv` holds the cleaned orders of one day. The rules come from `src/etl.py::clean_orders` and `src/schemas.py::NUMERIC_RULES`:

1. Drop rows without an `order_id`. Then **deduplicate** on `order_id`, keeping the first row.
2. Drop rows whose `order_date` cannot be parsed or differs from the batch date.
3. Drop rows whose label is not 0 or 1.
4. **Standardise categories** to their canonical spelling, ignoring case, spaces and dashes (` standard ` → `Standard`, `EXPRESS` → `Express`). Missing or unknown values become `Unknown`.
5. **Invalid numbers become missing**:
   - `distance_km ≤ 0`
   - `order_value_egp ≤ 0`
   - `package_weight_kg ≤ 0`
   - `items_count < 1`
   - `promised_delivery_days < 1`
   - `order_hour ∉ [0, 23]`
   - `warehouse_load_pct ∉ [0, 100]`
   - `carrier_on_time_rate ∉ [0, 1]`
6. Cast types (nullable integers for count and hour columns) and sort by `order_id`.

Missing numeric values are **not** imputed here. Imputation is learned, so it belongs inside the training pipeline. The task's XCom (`LayerResult`) carries every dropped and fixed count.
