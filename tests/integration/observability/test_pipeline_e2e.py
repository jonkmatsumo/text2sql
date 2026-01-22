import time
import uuid

import httpx
import pytest
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

COLLECTOR_URL = "http://localhost:4318/v1/traces"
WORKER_URL = "http://localhost:5050/api/v1/traces"


def test_integration_import_sanity():
    """CI-safe check that the integration test module imports."""
    import opentelemetry

    assert opentelemetry is not None


def test_otel_pipeline_e2e():
    """
    End-to-end integration test for the OTEL pipeline.

    Validates: Client -> Collector -> Worker -> Postgres/MinIO -> Query API
    """
    # 1. Setup OTEL SDK to emit through the collector
    service_name = f"test-e2e-{uuid.uuid4().hex[:8]}"
    resource = Resource.create({SERVICE_NAME: service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=COLLECTOR_URL)
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)
    tracer = provider.get_tracer("test-tracer")

    # 2. Emit a test trace
    trace_id_hex = uuid.uuid4().hex
    with tracer.start_as_current_span("e2e-parent") as parent:
        parent.set_attribute("test.run_id", trace_id_hex)
        with tracer.start_as_current_span("e2e-child") as child:
            child.set_attribute("test.child_val", 42)

    # Force flush to ensure it reaches the collector
    provider.force_flush()
    print(f"Emitted trace {trace_id_hex} for service {service_name}")

    # 3. Poll Worker API for the trace
    # We give it some time for: Collector -> Worker Ingestion -> Processing
    max_retries = 15
    found = False
    last_error = None
    for i in range(max_retries):
        time.sleep(2)
        try:
            response = httpx.get(f"{WORKER_URL}", params={"service": service_name})
            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])
                if any(item["service_name"] == service_name for item in items):
                    found = True
                    # Verify details
                    found_trace = next(
                        item for item in items if item["service_name"] == service_name
                    )
                    tid = found_trace["trace_id"]

                    # Fetch detailed trace
                    detail_resp = httpx.get(f"{WORKER_URL}/{tid}", params={"include": "attributes"})
                    assert detail_resp.status_code == 200
                    detail = detail_resp.json()
                    assert detail["span_count"] >= 2

                    # Fetch spans
                    spans_resp = httpx.get(f"{WORKER_URL}/{tid}/spans")
                    assert spans_resp.status_code == 200
                    spans = spans_resp.json()["items"]
                    assert len(spans) >= 2
                    assert any(s["name"] == "e2e-parent" for s in spans)
                    assert any(s["name"] == "e2e-child" for s in spans)

                    print(f"Successfully verified trace {tid} in worker API")
                    break
        except Exception as e:
            last_error = e
            print(f"Attempt {i+1} failed: {e}")

    if not found:
        msg = f"Trace for service {service_name} not found after {max_retries} attempts."
        pytest.fail(f"{msg} Last error: {last_error}")
