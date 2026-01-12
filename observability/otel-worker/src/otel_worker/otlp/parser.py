import logging

from google.protobuf.json_format import MessageToDict
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

logger = logging.getLogger(__name__)


def parse_otlp_traces(body: bytes) -> dict:
    """Parse OTLP binary protobuf trace request into a dictionary."""
    request = ExportTraceServiceRequest()
    request.ParseFromString(body)
    return MessageToDict(request)


def extract_trace_summaries(parsed_data: dict) -> list[dict]:
    """Extract a flat list of traces with basic metadata for easy processing."""
    summaries = []

    resource_spans = parsed_data.get("resourceSpans", [])
    for rs in resource_spans:
        resource = rs.get("resource", {})
        attributes = {
            attr["key"]: attr["value"].get("stringValue") for attr in resource.get("attributes", [])
        }
        service_name = attributes.get("service.name", "unknown")

        scope_spans = rs.get("scopeSpans", [])
        for ss in scope_spans:
            spans = ss.get("spans", [])
            for span in spans:
                # We group by trace_id later, for now just extract span details
                summaries.append(
                    {
                        "service_name": service_name,
                        "trace_id": span.get("traceId"),
                        "span_id": span.get("spanId"),
                        "parent_span_id": span.get("parentSpanId"),
                        "name": span.get("name"),
                        "start_time_unix_nano": span.get("startTimeUnixNano"),
                        "end_time_unix_nano": span.get("endTimeUnixNano"),
                        "status": span.get("status", {}).get("code", "STATUS_CODE_UNSET"),
                        "attributes": {
                            attr["key"]: str(
                                attr["value"].get(
                                    "stringValue",
                                    attr["value"].get(
                                        "intValue", attr["value"].get("boolValue", "")
                                    ),
                                )
                            )
                            for attr in span.get("attributes", [])
                        },
                        "events": span.get("events", []),
                    }
                )
    return summaries
