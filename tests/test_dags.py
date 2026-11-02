"""DAG integrity. Needs Airflow, so it runs inside the containers:

docker compose exec airflow-scheduler pytest tests/test_dags.py
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


@pytest.mark.parametrize(
    "dag_id, chain",
    [
        (
            "data_pipeline",
            [
                "extract_to_bronze",
                "transform_to_silver",
                "prepare_gold",
                "validate_gold",
                "load_gold_to_postgres",
            ],
        ),
        (
            "ml_training_pipeline",
            ["load_gold_data", "train_model", "evaluate_model", "save_model"],
        ),
    ],
)
def test_task_order_and_retries(dagbag, dag_id, chain):
    dag = dagbag.get_dag(dag_id)
    assert dag is not None
    for upstream, downstream in zip(chain, chain[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids
    assert all(task.retries >= 2 for task in dag.tasks)


def test_data_pipeline_is_daily(dagbag):
    dag = dagbag.get_dag("data_pipeline")
    assert dag.max_active_runs == 1
    assert dag.timetable.expression == "0 0 * * *"
