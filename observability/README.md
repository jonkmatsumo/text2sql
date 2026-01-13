# Observability Infrastructure

This directory contains the OpenTelemetry (OTEL) infrastructure for the Text2SQL project, including a custom dual-write pipeline.

## Architecture

1.  **OTEL Collector**: Receives OTLP traces (gRPC/HTTP) from instrumented services.
2.  **OTEL Worker**: A custom FastAPI service that receives traces from the collector and performs three actions:
    -   **Postgres**: Indexes trace and span metadata in a dedicated `otel` schema for fast querying/debugging.
    -   **MinIO**: Stores raw gzipped JSON trace payloads for full data retention.
    -   **MLflow**: Dual-writes derived summaries, metrics (duration, error count, tokens), and trace artifacts to MLflow for transition/comparasion studies.

## Getting Started

### Prerequisites

-   Existing Postgres (used as `agent-control-db`)
-   Existing MinIO
-   Existing MLflow Tracking Server

### Running Locally

1.  Copy `.env.example` to `.env` (if not already managed by root compose).
2.  Start the observability stack using the override:
    ```bash
    docker compose -f docker-compose.infra.yml -f docker-compose.app.yml -f observability/docker-compose.observability.yml up -d otel-collector otel-worker
    ```

### Instrumented Services

To enable OTEL export in your services, set the following environment variables:

```bash
ENABLE_OTEL=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
OTEL_SERVICE_NAME=your-service-name
OTEL_RESOURCE_ATTRIBUTES=environment=local,repo=text2sql
```

## Schema & Storage

### Postgres (otel schema)

-   `otel.traces`: High-level trace metadata, service names, and aggregate metrics.
-   `otel.spans`: Detailed span information, including parent-child relationships, attributes (JSONB), and events (JSONB).

### MinIO (otel-traces bucket)

-   Objects stored at: `{environment}/{service_name}/{YYYY-MM-DD}/{trace_id}.json.gz`

## MLflow Integration

-   **Experiment**: `otel-traces`
-   **Run Name**: `trace-{trace_id}`
-   **Metrics**: `duration_ms`, `span_count`, `error_count`, `input_tokens`, `output_tokens`.
-   **Artifacts**: `trace_raw.json`, `span_summary.json`.

To disable MLflow export after cutover, set `ENABLE_MLFLOW_EXPORT=false` in the `otel-worker` environment.
