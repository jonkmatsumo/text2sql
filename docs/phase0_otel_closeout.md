# Phase 0: OTEL Tracing Closeout - Scaffolding & Wiring Prep

## Overview
This phase establishes the repository structure, dependency management, and developer experience hooks required for the OTEL tracing closeout (Issues A-F). No functional changes to trace emission or ingestion logic were made in this phase.

## Changes

### 1. Agent Dependencies (Issue A)
- Updated `agent/pyproject.toml` with `opentelemetry-api`, `opentelemetry-sdk`, and `opentelemetry-exporter-otlp`.
- Added `agent/tests/test_otel_scaffolding.py` to verify that OTEL SDK symbols can be imported and instantiated without a running collector.

### 2. Standardized Configuration (Issue A/B Scaffolding)
- Defined stable environment variable names and default values in `agent/src/agent_core/telemetry.py`:
  - `OTEL_EXPORTER_OTLP_ENDPOINT`: Defaults to `http://localhost:4317`
  - `OTEL_EXPORTER_OTLP_PROTOCOL`: Defaults to `grpc`
  - `OTEL_SERVICE_NAME`: Defaults to `text2sql-agent`

### 3. Developer Experience Hooks (Issue D)
- Added new targets to the root `Makefile`:
  - `make otel-up`: Brings up the OTEL collector and worker.
  - `make otel-migrate`: Runs Alembic migrations for the OTEL worker database.
- Updated `observability/README.md` to document the local bring-up process using these new commands.

### 4. Integration Test Harness (Issue F)
- Created `observability/tests_integration/` directory.
- Added `test_pipeline_e2e.py` as a skipped skeleton test with a detailed TODO for implementing the end-to-end pipeline verification.
- Added `observability/tests_integration/README.md` explaining the testing strategy.

## Verification Results

### Agent Unit Tests
- `tests/test_otel_scaffolding.py`: **PASSED**
- `tests/test_telemetry.py`: **PASSED**

```text
tests/test_otel_scaffolding.py .                                         [ 10%]
tests/test_telemetry.py .........                                        [100%]
============================== 10 passed in 0.10s ==============================
```

### Worker Unit Tests
- Note: Worker unit tests were attempted but encountered `ModuleNotFoundError: No module named 'pydantic_settings'` in the local environment. These tests typically run within the Docker container. No worker logic was changed in this phase.

## How to run the new helper commands
1.  **Bring up observability stack**:
    ```bash
    make otel-up
    ```
2.  **Migrate OTEL database**:
    ```bash
    make otel-migrate
    ```
