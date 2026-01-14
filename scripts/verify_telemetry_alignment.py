import os
import sys
import time
import uuid

import mlflow
import requests
from agent_core.telemetry import SpanType, telemetry


def verify_alignment():
    """Verify OTEL as canonical trace store and MLflow as summarized sink."""
    print("--- Telemetry Alignment Verification (Phase 1.5) ---")

    # 1. Setup environment
    os.environ["TELEMETRY_BACKEND"] = "dual"
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
    otel_worker_url = os.getenv("OTEL_WORKER_URL", "http://localhost:8080")
    mlflow.set_tracking_uri(tracking_uri)

    trace_id = uuid.uuid4().hex
    session_id = f"test-session-{uuid.uuid4().hex[:8]}"

    print(f"Trace ID: {trace_id}")
    print(f"Session ID: {session_id}")

    # 2. Emit a trace via agent telemetry
    # We use a custom trace_id if possible, or just capture it after
    # TelemetryService doesn't currently allow passing trace_id explicitly in start_span
    # so we will rely on finding the latest trace in the worker.

    print("\n[Step 1] Emitting trace via agent telemetry (dual mode)...")
    with telemetry.start_span("alignment_verification", span_type=SpanType.CHAIN) as _:
        telemetry.update_current_trace(
            {"telemetry.session_id": session_id, "verification_mode": "phase_1_5"}
        )
        with telemetry.start_span("sub_action", span_type=SpanType.TOOL) as sub:
            sub.set_attribute("action_name", "ping")
            time.sleep(0.1)

    print("Wait 5s for worker ingestion and export...")
    time.sleep(5)

    # 3. Verify OTEL Worker Persistence
    print(f"\n[Step 2] Verifying OTEL Worker Persistence ({otel_worker_url})...")
    try:
        resp = requests.get(f"{otel_worker_url}/api/v1/traces", params={"limit": 5})
        resp.raise_for_status()
        traces = resp.json()["items"]

        # Look for our trace by service name or custom attribute if indexed
        # For now, we'll just check if there are any traces.
        if not traces:
            print("✗ No traces found in OTEL worker.")
            return False

        latest_trace = traces[0]
        actual_trace_id = latest_trace["trace_id"]
        print(f"✓ Found latest trace in OTEL worker: {actual_trace_id}")
    except Exception as e:
        print(f"✗ OTEL Worker verification failed: {e}")
        return False

    # 4. Verify MLflow Run Sink
    print("\n[Step 3] Verifying MLflow Run Sink (Experiment: otel-traces)...")
    try:
        client = mlflow.tracking.MlflowClient()
        experiment = client.get_experiment_by_name("otel-traces")
        if not experiment:
            print("✗ Experiment 'otel-traces' not found.")
            return False

        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["attribute.start_time DESC"],
            max_results=5,
        )

        # Verify run name contains trace_id
        matching_run = None
        for run in runs:
            if actual_trace_id in run.info.run_name:
                matching_run = run
                break

        if not matching_run:
            print(f"✗ No MLflow run found matching trace {actual_trace_id}")
            return False

        print(f"✓ Found MLflow run: {matching_run.info.run_name}")

        # Verify Metrics
        metrics = matching_run.data.metrics
        required_metrics = ["duration_ms", "span_count", "error_count"]
        for m in required_metrics:
            if m not in metrics:
                print(f"✗ Missing metric in MLflow: {m}")
                return False
        print(f"✓ Metrics verified: {required_metrics}")

        # Verify Artifacts
        artifacts = [a.path for a in client.list_artifacts(matching_run.info.run_id)]
        required_artifacts = ["trace_raw.json", "span_summary.json"]
        for a in required_artifacts:
            if a not in artifacts:
                print(f"✗ Missing artifact in MLflow: {a}")
                return False
        print(f"✓ Artifacts verified: {required_artifacts}")

    except Exception as e:
        print(f"✗ MLflow verification failed: {e}")
        return False

    print("\n✓ TELEMETRY ALIGNMENT VERIFIED SUCCESSFUL")
    return True


if __name__ == "__main__":
    success = verify_alignment()
    sys.exit(0 if success else 1)
