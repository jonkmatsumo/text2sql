import unittest

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from otel_worker.otlp.parser import extract_trace_summaries, parse_otlp_traces


class TestOTLPParser(unittest.TestCase):
    """Unit tests for OTLP trace parsing and summary extraction."""

    def test_parse_and_extract(self):
        """Verify that OTLP binary protobuf can be parsed and summarized."""
        request = ExportTraceServiceRequest()
        rs = request.resource_spans.add()
        rs.resource.attributes.add(key="service.name").value.string_value = "test-service"

        ss = rs.scope_spans.add()
        span = ss.spans.add()
        span.trace_id = b"1234567812345678"
        span.span_id = b"12345678"
        span.name = "test-span"
        span.start_time_unix_nano = 1000
        span.end_time_unix_nano = 2000

        serialized = request.SerializeToString()
        parsed = parse_otlp_traces(serialized)
        summaries = extract_trace_summaries(parsed)

        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]["service_name"], "test-service")
        self.assertEqual(summaries[0]["name"], "test-span")
        self.assertEqual(
            summaries[0]["trace_id"], "MTIzNDU2NzgxMjM0NTY3OA=="
        )  # Base64 encoded by MessageToDict

    def test_parse_json(self):
        """Verify that OTLP JSON can be parsed and summarized."""
        json_payload = """
        {
          "resourceSpans": [
            {
              "resource": {
                "attributes": [
                  { "key": "service.name", "value": { "stringValue": "json-service" } }
                ]
              },
              "scopeSpans": [
                {
                  "spans": [
                    {
                      "traceId": "MTIzNDU2NzgxMjM0NTY3OA==",
                      "spanId": "MTIzNDU2Nzg=",
                      "name": "json-span",
                      "startTimeUnixNano": "1000",
                      "endTimeUnixNano": "2000"
                    }
                  ]
                }
              ]
            }
          ]
        }
        """
        from otel_worker.otlp.parser import parse_otlp_json_traces

        parsed = parse_otlp_json_traces(json_payload.encode())
        summaries = extract_trace_summaries(parsed)

        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]["service_name"], "json-service")
        self.assertEqual(summaries[0]["name"], "json-span")

    def test_parse_invalid_proto(self):
        """Verify that invalid protobuf fails with ValueError."""
        with self.assertRaises(ValueError) as cm:
            parse_otlp_traces(b"not a protobuf")
        self.assertIn("Invalid OTLP protobuf payload", str(cm.exception))

    def test_parse_invalid_json(self):
        """Verify that invalid JSON fails with ValueError."""
        from otel_worker.otlp.parser import parse_otlp_json_traces

        with self.assertRaises(ValueError) as cm:
            parse_otlp_json_traces(b"{malformed json")
        self.assertIn("Invalid OTLP JSON payload", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
