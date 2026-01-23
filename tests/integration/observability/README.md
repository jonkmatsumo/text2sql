# Observability Integration Tests

This directory contains integration tests for the observability pipeline.

## E2E Pipeline Test (`test_pipeline_e2e.py`)

This test is designed to verify the entire flow from trace emission to indexing and storage.

### Intended Approach (Issue F):
1.  **Environment Setup**: Bring up the observability stack using `make otel-up`.
2.  **Trace Emission**: Use a test script or instrumented client to send OTLP traces to the collector.
3.  **Verification**:
    -   Query the `otel` schema in the `agent-control-db` to confirm spans are recorded.
    -   Check the MinIO `otel-traces` bucket for the raw JSON payload.
    -   Verify MLflow experiment tracking if enabled.

### Running Tests:
Currently, these tests are skipped by default as they require a running environment.
To run them once the stack is up:
```bash
pytest observability/tests_integration/
```
