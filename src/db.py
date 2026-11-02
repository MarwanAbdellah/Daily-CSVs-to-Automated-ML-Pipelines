"""Optional bonus: mirror each validated Gold partition into a SQL table.

The load is idempotent per date: inside one transaction the date's rows are
deleted, then the partition is inserted, so reruns never duplicate rows.
"""

from datetime import date

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from src.config import Settings, settings
from src.logging_config import get_logger
from src.schemas import BatchPaths

logger = get_logger(__name__)

GOLD_TABLE = "gold_orders"


def load_gold_partition(batch_date: date, cfg: Settings = settings) -> int:
    """Replace the date's rows in the processed database; returns rows stored."""
    if not cfg.processed_db_uri:
        logger.info("PROCESSED_DB_URI not set, skipping database load")
        return 0
    gold = pd.read_csv(BatchPaths.for_date(batch_date, cfg).gold)
    engine = create_engine(cfg.processed_db_uri)
    try:
        with engine.begin() as conn:
            if inspect(conn).has_table(GOLD_TABLE):
                conn.execute(
                    text(f"DELETE FROM {GOLD_TABLE} WHERE order_date = :d"),
                    {"d": batch_date.isoformat()},
                )
            gold.to_sql(GOLD_TABLE, conn, if_exists="append", index=False)
            stored = conn.execute(
                text(f"SELECT COUNT(*) FROM {GOLD_TABLE} WHERE order_date = :d"),
                {"d": batch_date.isoformat()},
            ).scalar_one()
    finally:
        engine.dispose()
    logger.info("Stored %d Gold rows for %s in %s", stored, batch_date, GOLD_TABLE)
    return int(stored)
