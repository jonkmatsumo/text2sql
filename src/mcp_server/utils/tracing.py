import functools
import json
import logging
from typing import Awaitable, Callable, ParamSpec, TypeVar

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from common.config.env import get_env_str

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


def trace_tool(tool_name: str) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Add OpenTelemetry tracing to an MCP tool handler.

    Args:
        tool_name: The name of the tool (e.g. "execute_sql_query").
    """

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            # Check enforcement mode
            mode = get_env_str("TELEMETRY_ENFORCEMENT_MODE", "warn").lower()

            # Get a tracer
            tracer = trace.get_tracer("mcp.server")

            # Check if tracing is actually enabled/configured
            # This is a heuristic: if we get a NoOpSpan, maybe tracing is off.
            # But start_as_current_span returns a Span.
            # We can check if the tracer provider is NoOp?
            # Or just assume if we are here we want to trace.

            span_name = f"mcp.tool.{tool_name}"

            with tracer.start_as_current_span(
                span_name,
                kind=trace.SpanKind.SERVER,
            ) as span:
                # 1. Record basic attributes
                span.set_attribute("mcp.tool.name", tool_name)

                # Capture tenant_id if present
                tenant_id = kwargs.get("tenant_id")
                if tenant_id is not None:
                    span.set_attribute("mcp.tenant_id", str(tenant_id))

                # Estimate request size (approximate)
                try:
                    req_size = len(json.dumps(kwargs, default=str).encode("utf-8"))
                    span.set_attribute("mcp.tool.request.size_bytes", req_size)
                except Exception:
                    pass

                if not span.is_recording():
                    if mode in ("warn", "error"):
                        logger.warning(
                            f"Tool {tool_name} executed without active recording span. "
                            f"Mode={mode}."
                        )

                try:
                    # Execute the tool
                    response = await func(*args, **kwargs)

                    # 2. Apply Output Bounding (Phase 7)
                    # We only apply this to non-execute tools or if the response is not a string
                    # But most tools return JSON strings.
                    from mcp_server.utils.tool_output import bound_tool_output

                    actual_response = response
                    is_truncated = False
                    resp_size = 0

                    if tool_name != "execute_sql_query":
                        # If it's a JSON string, we parse it to bound it properly
                        if isinstance(response, str):
                            try:
                                data = json.loads(response)
                                bounded_data, meta = bound_tool_output(data)
                                is_truncated = meta["truncated"]
                                resp_size = meta["returned_bytes"]
                                if is_truncated:
                                    actual_response = json.dumps(
                                        bounded_data, default=str, separators=(",", ":")
                                    )
                            except Exception:
                                # Fallback to raw string length if parsing fails
                                resp_size = len(str(response).encode("utf-8"))
                        else:
                            # If it's already an object, bound it
                            bounded_obj, meta = bound_tool_output(response)
                            actual_response = bounded_obj
                            is_truncated = meta["truncated"]
                            resp_size = meta["returned_bytes"]
                    else:
                        # For execute_sql_query, we just record the size (it handles truncation)
                        resp_size = len(str(response).encode("utf-8"))
                        # We try to detect if it was truncated from the string (heuristic or parse)
                        if '"is_truncated":true' in str(response).lower():
                            is_truncated = True

                    # 3. Record response attributes
                    span.set_attribute("mcp.tool.response.size_bytes", resp_size)
                    span.set_attribute("mcp.tool.response.truncated", is_truncated)

                    return actual_response

                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    # Try to classify error if possible
                    err_cls = getattr(e, "category", "unknown")
                    span.set_attribute("mcp.tool.error.category", err_cls)
                    raise

        return wrapper

    return decorator
