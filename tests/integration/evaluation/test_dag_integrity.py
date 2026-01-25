"""Smoke tests for Airflow DAG integrity."""

import sys
from unittest.mock import MagicMock

import pytest


class MockDAG:
    """Mock Airflow DAG class."""

    def __init__(self, *args, **kwargs):
        """Initialize mock DAG."""
        self.dag_id = args[0] if args else kwargs.get("dag_id", "unknown")
        self.tasks = []

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, *args):
        """Exit context manager."""
        pass


class MockTask:
    """Mock Airflow Task/Operator class."""

    def __init__(self, task_id, **kwargs):
        """Initialize mock task."""
        self.task_id = task_id

    def __rshift__(self, other):
        """Handle >> operator for task dependency."""
        return other


def test_dag_import():
    """Verify that the evaluation DAG can be imported and has the expected structure."""
    mock_airflow = MagicMock()
    mock_airflow.DAG = MockDAG
    mock_python = MagicMock()
    mock_python.PythonOperator = MockTask
    sys.modules["airflow"] = mock_airflow
    sys.modules["airflow.operators"] = MagicMock()
    sys.modules["airflow.operators.python"] = mock_python

    try:
        from evaluation.dags import eval_dag

        dag = eval_dag.dag
        assert dag is not None
        assert dag.dag_id == "text2sql_evaluation"

        assert hasattr(eval_dag, "prepare_run_context")
        assert hasattr(eval_dag, "run_evaluation_task")
        assert hasattr(eval_dag, "compute_regression_task")
        assert callable(eval_dag.prepare_run_context)
        assert callable(eval_dag.run_evaluation_task)
        assert callable(eval_dag.compute_regression_task)
    except ImportError as e:
        pytest.fail(f"Failed to import DAG: {e}")
    finally:
        for mod in ("airflow", "airflow.operators", "airflow.operators.python"):
            sys.modules.pop(mod, None)
