"""Smoke tests for Airflow DAG integrity."""

import os
import sys
from unittest.mock import MagicMock

# Mock airflow and other non-test dependencies
mock_airflow = MagicMock()
mock_python = MagicMock()
sys.modules["airflow"] = mock_airflow
sys.modules["airflow.operators"] = MagicMock()
sys.modules["airflow.operators.python"] = mock_python

active_dag = None


class MockDAG:
    def __init__(self, *args, **kwargs):
        self.dag_id = args[0] if args else kwargs.get("dag_id", "unknown")
        self.tasks = []

    def __enter__(self):
        global active_dag
        active_dag = self
        return self

    def __exit__(self, *args):
        global active_dag
        active_dag = None


mock_airflow.DAG = MockDAG


class MockTask:
    def __init__(self, task_id, **kwargs):
        self.task_id = task_id
        if active_dag:
            active_dag.tasks.append(self)

    def __rshift__(self, other):
        return other


mock_python.PythonOperator = MockTask

import pytest


def test_dag_import():
    """Verify that the evaluation DAG can be imported and has the expected structure."""

    dag_path = os.path.join(os.path.dirname(__file__), "../airflow/dags")
    sys.path.append(dag_path)

    try:
        from eval_dag import dag

        assert dag is not None
        assert dag.dag_id == "text2sql_evaluation"

        # Verify tasks
        task_ids = [t.task_id for t in dag.tasks]
        assert "prepare_run_context" in task_ids
        assert "run_evaluation" in task_ids
        assert "compute_regression" in task_ids

    except ImportError as e:
        pytest.fail(f"Failed to import DAG: {e}")
    finally:
        if dag_path in sys.path:
            sys.path.remove(dag_path)
