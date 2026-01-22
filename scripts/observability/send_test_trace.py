import time

import requests
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import Span


def create_test_trace():
    """Create a serialized OTLP trace request for testing."""
    request = ExportTraceServiceRequest()

    rs = request.resource_spans.add()
    rs.resource.attributes.append(
        KeyValue(key="service.name", value=AnyValue(string_value="smoke-test-service"))
    )

    ss = rs.scope_spans.add()
    span = ss.spans.add()

    # Random-ish IDs
    trace_id = b"smoke_test_trace_id_123"[:16]
    span_id = b"smoke_span_id_456"[:8]

    span.trace_id = trace_id.ljust(16, b"\0")
    span.span_id = span_id.ljust(8, b"\0")
    span.name = "smoke-test-span"
    span.kind = Span.SpanKind.SPAN_KIND_INTERNAL
    span.start_time_unix_nano = int(time.time() * 1e9)
    span.end_time_unix_nano = span.start_time_unix_nano + 100_000_000  # 100ms

    span.attributes.append(KeyValue(key="test.key", value=AnyValue(string_value="test.value")))

    return request.SerializeToString()


def send_trace(endpoint="http://localhost:4318/v1/traces"):
    """Send a test trace to the specified OTLP/HTTP endpoint."""
    payload = create_test_trace()
    headers = {"Content-Type": "application/x-protobuf"}

    print(f"Sending test trace to {endpoint}...")
    response = requests.post(endpoint, data=payload, headers=headers)

    if response.status_code == 200:
        print("Successfully sent test trace!")
    else:
        print(f"Failed to send test trace: {response.status_code} - {response.text}")


if __name__ == "__main__":
    send_trace()
