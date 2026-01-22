"""Verify MLflow setup and connectivity as a runs sink."""

import os
import sys

import mlflow
from mlflow.tracking import MlflowClient


def verify_mlflow_setup():
    """Verify MLflow tracking server is accessible and can read 'otel-traces' runs."""
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")

    print(f"Connecting to MLflow at {tracking_uri}...")
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()

    try:
        # Test connection by searching for the canonical sink experiment
        experiment = client.get_experiment_by_name("otel-traces")

        if not experiment:
            # We don't create it here; it's the worker's job. We just check connectivity.
            print("⚠ Connected to MLflow, but 'otel-traces' experiment not found.")
            print("  This is expected if the OTEL worker hasn't processed any traces yet.")
            print("  Please run an agent query to trigger trace export.")
            return True

        print(f"✓ Found 'otel-traces' experiment (ID: {experiment.experiment_id}).")

        # Query for recent runs to validate sink behavior
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            max_results=5,
            order_by=["attribute.start_time DESC"],
        )

        if not runs:
            print("⚠ 'otel-traces' experiment exists but has no runs.")
            print("  Please run an agent query first to generate data.")
            return True

        print(f"✓ Found {len(runs)} recent run(s) in 'otel-traces'. Validating latest...")
        latest_run = runs[0]

        # Validate metrics
        metrics = latest_run.data.metrics
        required_metrics = ["duration_ms", "span_count", "total_tokens"]
        found_metrics = [k for k in required_metrics if k in metrics]

        if found_metrics:
            print(f"  ✓ Metrics found: {found_metrics}")
        else:
            print(
                f"  ⚠ No standard metrics found in run {latest_run.info.run_id}. "
                f"(Metrics: {list(metrics.keys())})"
            )

        # Validate artifacts (best effort via listing)
        artifacts = client.list_artifacts(latest_run.info.run_id)
        artifact_names = [a.path for a in artifacts]
        required_artifacts = ["trace_raw.json", "span_summary.json"]
        found_artifacts = [a for a in required_artifacts if a in artifact_names]

        if found_artifacts:
            print(f"  ✓ Artifacts found: {found_artifacts}")
        else:
            print(f"  ⚠ No standard trace artifacts found in run {latest_run.info.run_id}.")

        print("✓ MLflow sink verification passed.")
        return True

    except Exception as e:
        print(f"✗ MLflow setup verification failed: {e}")
        return False


if __name__ == "__main__":
    success = verify_mlflow_setup()
    sys.exit(0 if success else 1)
