import json
import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default Config
DEFAULT_DATASET = os.getenv("EVAL_DEFAULT_DATASET", "/app/queries/golden_dataset.jsonl")
ARTIFACT_ROOT = os.getenv("EVAL_ARTIFACT_ROOT", "/opt/airflow/logs/eval_artifacts")

logger = logging.getLogger("eval_dag")

default_args = {
    "owner": "eval_team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def prepare_run_context(**context):
    """Generate run metadata."""
    run_id = context["run_id"]
    conf = context["dag_run"].conf or {}

    # Merge default config with runtime params
    eval_config = {
        "dataset_path": conf.get("dataset_path", DEFAULT_DATASET),
        "run_id": run_id,
        "tenant_id": conf.get("tenant_id", 1),
        "concurrency": conf.get("concurrency", 1),
        "seed": conf.get("seed", 42),
        "limit": conf.get("limit", None),
        "output_dir": ARTIFACT_ROOT,
    }

    # Log context
    logger.info(f"Preparing run {run_id} with config: {json.dumps(eval_config)}")

    # Pass config to next task via XCom
    return eval_config


def run_evaluation_task(**context):
    """Invoke the evaluation runner library."""
    import asyncio

    from airflow_evals.runner.config import EvaluationConfig
    from airflow_evals.runner.core import run_evaluation

    # Retrieve config from XCom
    ti = context["ti"]
    eval_config_dict = ti.xcom_pull(task_ids="prepare_run_context")

    # Convert to Pydantic
    config = EvaluationConfig(**eval_config_dict)

    # Run async runner in sync context
    logger.info(f"Invoking runner for run_id={config.run_id}")
    summary = asyncio.run(run_evaluation(config))

    # Check if regression report was generated
    regression_file = os.path.join(config.output_dir, config.run_id, "regression_report.json")
    is_regression = False
    if os.path.exists(regression_file):
        with open(regression_file, "r") as f:
            report_data = json.load(f)
            is_regression = report_data.get("is_regression", False)

    # Push key metrics to XCom for downstream checks
    return {
        "run_id": summary.run_id,
        "total": summary.total_cases,
        "success": summary.successful_cases,
        "failed": summary.failed_cases,
        "accuracy": summary.accuracy,
        "is_regression": is_regression,
    }


def compute_regression_task(**context):
    """Fail if regression detected."""
    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids="run_evaluation")
    logger.info(f"Checking regression for metrics: {metrics}")

    if metrics.get("is_regression"):
        raise ValueError(
            f"Regression detected in run {metrics['run_id']}! Check artifacts for details."
        )

    logger.info("No regression detected. proceeding.")


with DAG(
    "text2sql_evaluation",
    default_args=default_args,
    description="Run automated evaluations for Text-to-SQL agent",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["evals", "agent"],
    doc_md=__doc__,
) as dag:

    t1 = PythonOperator(
        task_id="prepare_run_context",
        python_callable=prepare_run_context,
    )

    t2 = PythonOperator(
        task_id="run_evaluation",
        python_callable=run_evaluation_task,
    )

    t3 = PythonOperator(
        task_id="compute_regression",
        python_callable=compute_regression_task,
    )

    t1 >> t2 >> t3
