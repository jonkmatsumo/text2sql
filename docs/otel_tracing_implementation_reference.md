# OTEL Tracing & Worker: Implementation Reference

This document provides a comprehensive summary of the work performed on the `feature/otel` branch and detailed technical documentation of the OTEL worker's architecture and implementation.

## Feature Branch Summary

Starting from baseline commit `289a64417c219f0aa5b9128597e36f31dcfc4ddc`, the following milestones were achieved:

| Commit Hash | Milestone | Description |
| :--- | :--- | :--- |
| `289a644` | **Baseline Boundary** | Refactored telemetry to stabilize the MLflow backend boundary. |
| `0ea937f` | **OTEL Backend Foundation** | Initial implementation of the OTEL backend with in-memory exporter tests. |
| `8caeeef` | **Dual-Write Support** | Enabled support for dual-writing to both OTEL and legacy backends. |
| `35c527d` | **Smoke Verification** | Added opt-in smoke tests to verify OTLP ingestion end-to-to. |
| `7cd3fc8` | **Context API** | Introduced explicit capture/restore API for manual trace context management. |
| `70db6d6` | **Node Propagation** | Fixed trace context propagation across Agent Graph node boundaries. |
| `acd9c76` | **Tool Propagation** | Ensured trace parentage is maintained during tool invocations. |
| `35144d5` | **Design Update** | Updated central design docs with context propagation specs. |
| `297fc11` | **JSON Ingestion** | Support for `application/json` OTLP payloads in addition to Protobuf. |
| `afe8c89` | **Durable Ingestion** | Implemented `PersistenceCoordinator` for async, queued, and retry-safe ingestion. |
| `45631ee` | **Schema Migrations** | Integrated Alembic for versioned schema management of the `otel` schema. |
| *Latest* | **Query API** | Exposed read-only HTTP APIs for trace/span discovery and raw blob retrieval. |

---

## OTEL Worker: Architecture & Implementation

The OTEL worker is a high-performance, asynchronous service designed to ingest, process, and persist OTLP-compliant trace data.

### 1. Ingestion Layer (`app.py`)
- **Endpoints**:
    - `POST /v1/traces`: Accepts both `application/x-protobuf` and `application/json` payloads.
    - `GET /healthz`: Standard liveness probe.
- **Parsing**: Leverages `otel_worker.otlp.parser` to convert raw payloads into structured Python summaries before enqueuing.

### 2. Async Processing (`PersistenceCoordinator`)
The worker uses a decoupled ingestion model to ensure low latency for clients:
- **Enqueueing**: Payloads and their summaries are placed into an in-memory `asyncio.Queue`.
- **Worker Loop**: A background task drains the queue and processes batches.
- **Dual-Write Logic**:
    - **Postgres**: Summarized trace and span data are upserted into indexed tables.
    - **MinIO**: The raw OTLP payload is gzipped and uploaded as an immutable blob.

### 3. Storage Layer (`postgres.py`, `minio.py`)
- **Postgres (`otel` schema)**:
    - `traces` table: Stores trace-level metadata (`trace_id`, `service_name`, `duration_ms`, `span_count`).
    - `spans` table: Stores individual span details with `trace_id` foreign key.
    - **JSONB Optimization**: Attributes and events are stored as `JSONB` for flexible querying and efficient storage.
- **MinIO**:
    - Bucket: `otel-traces` (configurable).
    - Pathing: `{environment}/{service_name}/{date}/{trace_id}.json.gz`.

### 4. Query API (`app.py`)
Exposes bounded, read-only access to persisted telemetry:
- **`GET /api/v1/traces`**: Paginated listing with filters for `service_name`, `trace_id`, and `start_time` ranges.
- **`GET /api/v1/traces/{trace_id}`**: Retrieves summarized trace metadata and optional attributes.
- **`GET /api/v1/traces/{trace_id}/spans`**: Returns chronological span lists for a specific trace.
- **`GET /api/v1/traces/{trace_id}/raw`**: Direct retrieval of gzipped OTLP blobs from MinIO.

### 5. Schema Management
- **Alembic**: All database changes are handled via versioned migrations in `observability/otel-worker/migrations`.
- **Dynamic Schema**: The code respects `OTEL_DB_SCHEMA` (defaulting to `otel`), allowing for schema-level isolation (e.g., for multi-tenancy or testing).

### 6. Implementation Considerations
- **Performance**: High-traffic service-level indexing on `trace_id` and `service_name`.
- **Safety**: Bounded query limits (max 200 traces, 500 spans) to prevent OOM or performance degradation.
- **Resilience**: Idempotent upserts in Postgres allow for retry-safe re-ingestion of the same trace data.
