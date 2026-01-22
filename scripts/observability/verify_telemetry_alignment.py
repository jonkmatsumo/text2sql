import os
import sys

import mlflow
import requests


def verify_alignment():
    """Verify OTEL as canonical trace store and MLflow as summarized sink."""
    print("--- Telemetry Alignment Verification (OTEL-Canonical) ---")

    # 1. Setup environment
    # Force agent to use OTEL backend
    os.environ["TELEMETRY_BACKEND"] = "otel"

    # Defaults
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
    otel_worker_url = os.getenv("OTEL_WORKER_URL", "http://localhost:4320")

    mlflow.set_tracking_uri(tracking_uri)

    print("\n[Step 1] Triggering Trace (Requires Agent)...")
    print("  Note: This script assumes an agent query has been run recently or will be run now.")
    print("  Ensuring backend is OTEL-only...")

    # We won't import agent code here to avoid side effects or complex setup.
    # We assume the user has run a query or the environment is live.
    # If we wanted to trigger one, we'd need to mock it or call the agent API.
    # For verification, we'll look for *existing* recent traces.

    print("  Querying OTEL worker for recent traces...")

    # 2. Verify OTEL Worker Persistence
    print(f"\n[Step 2] Verifying OTEL Worker Persistence ({otel_worker_url})...")
    latest_trace_id = None
    try:
        resp = requests.get(f"{otel_worker_url}/api/v1/traces", params={"limit": 1})
        resp.raise_for_status()
        data = resp.json()
        traces = data.get("items", [])

        if not traces:
            print("✗ No traces found in OTEL worker.")
            print("  Please run an agent query to generate a trace, then re-run this script.")
            return False

        latest_trace = traces[0]
        latest_trace_id = latest_trace["trace_id"]
        print(f"✓ Found latest trace in OTEL worker: {latest_trace_id}")
    except Exception as e:
        print(f"✗ OTEL Worker verification failed: {e}")
        print("  Ensure the OTEL stack is running (make otel-up).")
        return False

    # 3. Verify MLflow Run Sink
    print("\n[Step 3] Verifying MLflow Run Sink (Experiment: otel-traces)...")
    try:
        client = mlflow.tracking.MlflowClient()
        experiment = client.get_experiment_by_name("otel-traces")

        if not experiment:
            print("✗ Experiment 'otel-traces' not found.")
            print("  This implies the OTEL worker has not successfully exported any traces yet.")
            return False

        # Look for the specific run corresponding to our trace_id
        # We search by tag or just check recent runs if tags aren't indexed perfectly yet
        # The worker sets the run_name to "trace-{trace_id}"
        print(f"  Searching for run with name: trace-{latest_trace_id} ...")

        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter_string=f"attributes.run_name = 'trace-{latest_trace_id}'",
            max_results=1,
        )

        matching_run = None
        if runs:
            matching_run = runs[0]
        else:
            # Fallback: check recent runs manually in case filter_string fails (sometimes elusive)
            recent_runs = client.search_runs(
                experiment_ids=[experiment.experiment_id],
                max_results=10,
                order_by=["attribute.start_time DESC"],
            )
            for r in recent_runs:
                if latest_trace_id in r.info.run_name:
                    matching_run = r
                    break

        if not matching_run:
            print(f"✗ No MLflow run found matching trace {latest_trace_id}")
            # Check if export might be disabled
            if os.getenv("ENABLE_MLFLOW_EXPORT", "true").lower() == "false":
                print("  Note: MLflow export is explicitly disabled via ENABLE_MLFLOW_EXPORT=false")
                return True  # Pass if disabled intentionally
            return False

        print(f"✓ Found MLflow run: {matching_run.info.run_name} (ID: {matching_run.info.run_id})")

        # Verify Metrics
        metrics = matching_run.data.metrics
        required_metrics = ["duration_ms", "span_count"]  # error_count might be missing if 0?
        for m in required_metrics:
            if m not in metrics:
                print(f"✗ Missing metric in MLflow: {m}")
                return False
        print(f"✓ Metrics verified: {list(metrics.keys())}")

        # Verify Artifacts
        artifacts = [a.path for a in client.list_artifacts(matching_run.info.run_id)]
        required_artifacts = ["trace_raw.json", "span_summary.json"]
        for a in required_artifacts:
            if a not in artifacts:
                print(f"✗ Missing artifact in MLflow: {a}")
                return False
        print(f"✓ Artifacts verified: {artifacts}")

    except Exception as e:
        print(f"✗ MLflow verification failed: {e}")
        return False

    print("\n✓ TELEMETRY ALIGNMENT VERIFIED SUCCESSFUL")
    return True


if __name__ == "__main__":
    success = verify_alignment()
    sys.exit(0 if success else 1)
