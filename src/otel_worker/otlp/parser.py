import logging

from google.protobuf.json_format import MessageToDict, Parse, ParseError
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

logger = logging.getLogger(__name__)


def parse_otlp_traces(body: bytes) -> dict:
    """Parse OTLP binary protobuf trace request into a dictionary."""
    request = ExportTraceServiceRequest()
    try:
        request.ParseFromString(body)
        return MessageToDict(request)
    except Exception as e:
        logger.error(f"Failed to parse OTLP protobuf: {e}")
        raise ValueError(f"Invalid OTLP protobuf payload: {e}")


def parse_otlp_json_traces(body: bytes) -> dict:
    """Parse OTLP JSON trace request into a dictionary."""
    request = ExportTraceServiceRequest()
    try:
        Parse(body, request, ignore_unknown_fields=True)
        return MessageToDict(request)
    except ParseError as e:
        logger.error(f"Failed to parse OTLP JSON: {e}")
        raise ValueError(f"Invalid OTLP JSON payload: {e}")
    except Exception as e:
        logger.error(f"Unexpected error parsing OTLP JSON: {e}")
        raise ValueError(f"Malformed JSON payload: {e}")


def extract_trace_summaries(parsed_data: dict) -> list[dict]:
    """Extract a flat list of traces with basic metadata for easy processing."""
    summaries = []

    resource_spans = parsed_data.get("resourceSpans", [])
    for rs in resource_spans:
        resource = rs.get("resource", {})
        resource_attributes = {
            attr["key"]: str(
                attr["value"].get(
                    "stringValue",
                    attr["value"].get("intValue", attr["value"].get("boolValue", "")),
                )
            )
            for attr in resource.get("attributes", [])
        }
        service_name = resource_attributes.get("service.name", "unknown")

        scope_spans = rs.get("scopeSpans", [])
        for ss in scope_spans:
            spans = ss.get("spans", [])
            for span in spans:
                # We group by trace_id later, for now just extract span details
                summaries.append(
                    {
                        "service_name": service_name,
                        "resource_attributes": resource_attributes,
                        "trace_id": span.get("traceId"),
                        "span_id": span.get("spanId"),
                        "parent_span_id": span.get("parentSpanId"),
                        "name": span.get("name"),
                        "kind": span.get("kind"),
                        "start_time_unix_nano": span.get("startTimeUnixNano"),
                        "end_time_unix_nano": span.get("endTimeUnixNano"),
                        "status": span.get("status", {}).get("code", "STATUS_CODE_UNSET"),
                        "status_message": span.get("status", {}).get("message"),
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
