# data/raw/

The 46 supplied daily batches, `orders_2026-10-01.csv` … `orders_2026-11-15.csv`. All of them share the 15-column schema in [`data_dictionary.csv`](../../data_dictionary.csv).

**Never edited.** `extract_to_bronze` picks the file whose name matches the run's logical date. A missing file fails the task with a clear `FileNotFoundError`.
