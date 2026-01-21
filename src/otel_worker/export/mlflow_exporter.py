import json
import logging
import os
import tempfile

import mlflow

from otel_worker.config import settings

logger = logging.getLogger(__name__)


def export_to_mlflow(trace_id: str, service_name: str, summaries: list[dict], raw_payload: dict):
    """
    Export trace summary and raw payload to MLflow.

    Each trace becomes a run in the 'otel-traces' experiment.
    """
    if not settings.ENABLE_MLFLOW_EXPORT:
        return

    try:
        mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
        mlflow.set_experiment("otel-traces")

        # Calculate metrics
        start_ts = min(int(s["start_time_unix_nano"]) for s in summaries)
        end_ts = max(int(s["end_time_unix_nano"]) for s in summaries)
        duration_ms = (end_ts - start_ts) // 1_000_000
        error_count = sum(1 for s in summaries if s["status"] == "STATUS_CODE_ERROR")

        # Extract token metrics if present in any span attributes
        input_tokens = 0
        output_tokens = 0
        for s in summaries:
            attrs = s.get("attributes", {})
            input_tokens += int(attrs.get("llm.token_usage.input_tokens", 0))
            output_tokens += int(attrs.get("llm.token_usage.output_tokens", 0))

        # Extract tags
        tenant_id = None
        interaction_id = None
        for s in summaries:
            attrs = s.get("attributes", {})
            tenant_id = tenant_id or attrs.get("tenant_id")
            interaction_id = interaction_id or attrs.get("interaction_id")

        with mlflow.start_run(run_name=f"trace-{trace_id}"):
            # Log tags
            mlflow.set_tags(
                {
                    "trace_id": trace_id,
                    "service_name": service_name,
                    "environment": settings.OTEL_ENVIRONMENT,
                    "tenant_id": tenant_id or "unknown",
                    "interaction_id": interaction_id or "unknown",
                }
            )

            # Log metrics
            mlflow.log_metrics(
                {
                    "duration_ms": float(duration_ms),
                    "span_count": float(len(summaries)),
                    "error_count": float(error_count),
                    "input_tokens": float(input_tokens),
                    "output_tokens": float(output_tokens),
                    "total_tokens": float(input_tokens + output_tokens),
                }
            )

            # Log artifacts
            with tempfile.TemporaryDirectory() as tmpdir:
                # 1. Raw payload (JSON)
                payload_path = os.path.join(tmpdir, "trace_raw.json")
                with open(payload_path, "w") as f:
                    json.dump(raw_payload, f)

                # 2. Span summary (JSON)
                summary_path = os.path.join(tmpdir, "span_summary.json")
                with open(summary_path, "w") as f:
                    json.dump(summaries, f)

                mlflow.log_artifacts(tmpdir)

        logger.info(f"Exported trace {trace_id} to MLflow")
    except Exception as e:
        logger.error(f"Failed to export to MLflow: {e}")
