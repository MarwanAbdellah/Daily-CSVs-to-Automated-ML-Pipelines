# data/bronze/

`date=YYYY-MM-DD/orders.csv` is a byte-for-byte copy of `raw/orders_YYYY-MM-DD.csv`, bad records and duplicates included. It is the immutable landing record of what the pipeline received for that date. It is written atomically (temp file + rename), so a rerun replaces it and never appends to it.
