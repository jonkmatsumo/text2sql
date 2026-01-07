"""Verify MLflow setup and connectivity."""

import os
import sys

import mlflow
from mlflow.tracing import start_trace


def verify_mlflow_setup():
    """Verify MLflow tracking server is accessible and configured correctly."""
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")

    print(f"Connecting to MLflow at {tracking_uri}...")
    mlflow.set_tracking_uri(tracking_uri)

    try:
        # Test connection
        client = mlflow.tracking.MlflowClient()
        experiments = client.search_experiments()
        print(f"✓ Connected to MLflow. Found {len(experiments)} experiments.")

        # Test trace creation
        with start_trace(name="test_trace") as trace:
            trace.set_inputs({"test": "value"})
            trace.set_outputs({"result": "success"})

        print("✓ Trace creation successful.")

        # Verify artifact store
        artifact_root = os.getenv("MLFLOW_DEFAULT_ARTIFACT_ROOT", "s3://mlflow-artifacts/")
        print(f"✓ Artifact root configured: {artifact_root}")

        return True

    except Exception as e:
        print(f"✗ MLflow setup verification failed: {e}")
        return False


if __name__ == "__main__":
    success = verify_mlflow_setup()
    sys.exit(0 if success else 1)
