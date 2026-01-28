import base64
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


def _parse_any_value(value: dict):
    """Parse OTLP AnyValue into native Python types."""
    if not isinstance(value, dict):
        return value

    if "stringValue" in value:
        return value["stringValue"]
    if "intValue" in value:
        return int(value["intValue"])
    if "boolValue" in value:
        return bool(value["boolValue"])
    if "doubleValue" in value:
        return float(value["doubleValue"])
    if "bytesValue" in value:
        try:
            return base64.b64decode(value["bytesValue"]).decode("utf-8", errors="replace")
        except Exception:
            return value["bytesValue"]
    if "arrayValue" in value:
        return [
            _parse_any_value(v.get("value", v))
            for v in value.get("arrayValue", {}).get("values", [])
        ]
    if "kvlistValue" in value:
        return {
            kv.get("key"): _parse_any_value(kv.get("value", {}))
            for kv in value.get("kvlistValue", {}).get("values", [])
        }
    return value


def _parse_attributes(attributes: list) -> dict:
    """Parse OTLP attributes list into a dict with native values."""
    parsed = {}
    for attr in attributes or []:
        key = attr.get("key")
        if not key:
            continue
        parsed[key] = _parse_any_value(attr.get("value", {}))
    return parsed


def _parse_events(events: list) -> list[dict]:
    """Parse OTLP span events to normalized dicts."""
    parsed = []
    for event in events or []:
        parsed.append(
            {
                "name": event.get("name"),
                "time_unix_nano": event.get("timeUnixNano"),
                "attributes": _parse_attributes(event.get("attributes", [])),
                "dropped_attributes_count": event.get("droppedAttributesCount"),
            }
        )
    return parsed


def _parse_links(links: list) -> list[dict]:
    """Parse OTLP span links to normalized dicts."""
    parsed = []
    for link in links or []:
        parsed.append(
            {
                "trace_id": link.get("traceId"),
                "span_id": link.get("spanId"),
                "attributes": _parse_attributes(link.get("attributes", [])),
                "dropped_attributes_count": link.get("droppedAttributesCount"),
            }
        )
    return parsed


def extract_trace_summaries(parsed_data: dict) -> list[dict]:
    """Extract a flat list of traces with basic metadata for easy processing."""
    summaries = []

    resource_spans = parsed_data.get("resourceSpans", [])
    for rs in resource_spans:
        resource = rs.get("resource", {})
        resource_attributes = _parse_attributes(resource.get("attributes", []))
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
                        "attributes": _parse_attributes(span.get("attributes", [])),
                        "events": _parse_events(span.get("events", [])),
                        "links": _parse_links(span.get("links", [])),
                    }
                )
    return summaries
