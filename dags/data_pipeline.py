"""Daily medallion pipeline: one raw CSV per logical date -> Bronze/Silver/Gold.

extract_to_bronze -> transform_to_silver -> prepare_gold -> validate_gold
    -> load_gold_to_postgres

The batch date is the run's logical date (``ds``), so scheduled runs, manual
runs with a chosen logical date, reruns and backfills all process exactly one
dated source file and overwrite exactly one partition per layer.
"""

from datetime import date, datetime, timedelta

from airflow.sdk import dag, get_current_context, task

from src.config import settings
from src.db import load_gold_partition
from src.etl import (
    extract_to_bronze,
    maybe_simulate_failure,
    parse_batch_date,
    prepare_gold,
    transform_to_silver,
    validate_gold,
)
from src.logging_config import configure_logging

configure_logging(settings.log_level)


def _batch_date() -> date:
    context = get_current_context()
    logical_date = context.get("logical_date") or context["dag_run"].run_after
    return parse_batch_date(logical_date.date())


@dag(
    dag_id="data_pipeline",
    description="Daily orders CSV -> Bronze -> Silver -> Gold -> validate -> Postgres",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    end_date=datetime(2026, 11, 15),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["assignment04", "data-engineering", "medallion"],
)
def data_pipeline():
    @task
    def extract_to_bronze_task() -> dict:
        batch_date = _batch_date()
        maybe_simulate_failure(batch_date, get_current_context()["ti"].try_number)
        return extract_to_bronze(batch_date).to_dict()

    @task
    def transform_to_silver_task(bronze: dict) -> dict:
        return transform_to_silver(parse_batch_date(bronze["batch_date"])).to_dict()

    @task
    def prepare_gold_task(silver: dict) -> dict:
        return prepare_gold(parse_batch_date(silver["batch_date"])).to_dict()

    @task
    def validate_gold_task(gold: dict) -> dict:
        return validate_gold(parse_batch_date(gold["batch_date"])).to_dict()

    bronze = extract_to_bronze_task.override(task_id="extract_to_bronze")()
    silver = transform_to_silver_task.override(task_id="transform_to_silver")(bronze)
    gold = prepare_gold_task.override(task_id="prepare_gold")(silver)

    @task
    def load_gold_to_postgres_task(report: dict) -> int:
        return load_gold_partition(parse_batch_date(report["batch_date"]))

    report = validate_gold_task.override(task_id="validate_gold")(gold)
    load_gold_to_postgres_task.override(task_id="load_gold_to_postgres")(report)


data_pipeline()
