"""Train the delivery-delay model from every validated Gold partition.

load_gold_data -> train_model -> evaluate_model -> save_model

Scheduled on the ``gold_orders`` Asset: it runs automatically after every
valid Gold update from data_pipeline, and can still be triggered manually.
Tasks hand over small summaries via XCom; data and the candidate model travel
through files in data/ml/.
"""

from datetime import datetime, timedelta

from airflow.sdk import Asset, dag, task

from src.config import GOLD_ASSET_NAME, settings
from src.logging_config import configure_logging
from src.train import evaluate_model, load_gold_data, save_model, train_model

configure_logging(settings.log_level)


@dag(
    dag_id="ml_training_pipeline",
    description="Gold -> time-based split -> sklearn Pipeline -> metrics -> joblib",
    schedule=[Asset(GOLD_ASSET_NAME)],
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["assignment04", "machine-learning"],
)
def ml_training_pipeline():
    @task
    def load_gold_data_task() -> dict:
        return load_gold_data()

    @task
    def train_model_task(gold: dict) -> dict:
        return train_model()

    @task
    def evaluate_model_task(split: dict) -> dict:
        return evaluate_model()

    @task
    def save_model_task(metrics: dict) -> dict:
        return save_model()

    gold = load_gold_data_task.override(task_id="load_gold_data")()
    split = train_model_task.override(task_id="train_model")(gold)
    metrics = evaluate_model_task.override(task_id="evaluate_model")(split)
    save_model_task.override(task_id="save_model")(metrics)


ml_training_pipeline()
