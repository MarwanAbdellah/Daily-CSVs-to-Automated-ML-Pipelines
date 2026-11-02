"""Run the pipelines without Airflow (local debugging and quick checks)."""

import argparse
import sys
from datetime import date

from src.config import settings
from src.etl import parse_batch_date, run_batch
from src.logging_config import configure_logging, get_logger
from src.train import run_training


def main() -> None:
    parser = argparse.ArgumentParser(description="Delivery-delay pipelines")
    sub = parser.add_subparsers(dest="command", required=True)
    etl = sub.add_parser("etl", help="Bronze -> Silver -> Gold -> validate")
    etl.add_argument(
        "--date",
        dest="dates",
        action="append",
        required=True,
        help="Batch date YYYY-MM-DD (repeatable)",
    )
    sub.add_parser("train", help="Train, evaluate and save the model")
    args = parser.parse_args()

    configure_logging(settings.log_level)
    logger = get_logger(__name__)
    logger.info("Environment: %s", settings.environment)

    try:
        if args.command == "etl":
            for value in args.dates:
                batch_date: date = parse_batch_date(value)
                run_batch(batch_date)
        else:
            run_training()
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
