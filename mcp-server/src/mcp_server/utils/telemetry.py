import contextlib
import logging
from typing import Any, Dict, Optional

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

logger = logging.getLogger(__name__)


class Telemetry:
    """Helper for OTEL tracing in MCP Server."""

    @staticmethod
    @contextlib.contextmanager
    def start_span(
        name: str,
        attributes: Optional[Dict[str, Any]] = None,
    ):
        """Start an OTEL span as a context manager."""
        tracer = trace.get_tracer("text2sql-mcp")

        with tracer.start_as_current_span(
            name=name, kind=trace.SpanKind.INTERNAL, attributes=attributes or {}
        ) as span:
            yield span

    @staticmethod
    def set_span_status(span, success: bool, error: Optional[Exception] = None):
        """Set span status based on success/error."""
        if success:
            span.set_status(Status(StatusCode.OK))
        else:
            span.set_status(Status(StatusCode.ERROR, description=str(error)))
            if error:
                span.record_exception(error)
