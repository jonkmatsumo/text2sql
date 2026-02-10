import functools
import json
import logging
from typing import Any, Awaitable, Callable, ParamSpec, TypeVar

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from common.config.env import get_env_str

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
    return None


def _read_is_truncated(payload: dict[str, Any]) -> bool:
    direct = _coerce_bool(payload.get("is_truncated"))
    if direct is not None:
        return direct

    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        nested = _coerce_bool(metadata.get("is_truncated"))
        if nested is not None:
            return nested
        fallback = _coerce_bool(metadata.get("truncated"))
        if fallback is not None:
            return fallback

    return False


def _extract_truncation_signal(response: Any) -> tuple[bool, bool]:
    """Return (is_truncated, parse_failed) from tool response payload."""
    if isinstance(response, dict):
        return _read_is_truncated(response), False

    if hasattr(response, "model_dump"):
        try:
            dumped = response.model_dump()
            if isinstance(dumped, dict):
                return _read_is_truncated(dumped), False
        except Exception:
            return False, True

    if isinstance(response, str):
        try:
            payload = json.loads(response)
        except json.JSONDecodeError:
            return False, True
        except Exception:
            return False, True
        if isinstance(payload, dict):
            return _read_is_truncated(payload), False
        return False, False

    return False, False


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

                    # 2. Apply Output Bounding
                    from mcp_server.utils.tool_output import bound_non_execute_tool_response

                    actual_response = response
                    is_truncated = False
                    truncation_parse_failed = False
                    resp_size = 0

                    if tool_name != "execute_sql_query":
                        bounded_response, bound_meta = bound_non_execute_tool_response(response)
                        actual_response = bounded_response
                        is_truncated = bool(bound_meta.get("truncated", False))
                        truncation_parse_failed = bool(bound_meta.get("parse_failed", False))
                        resp_size = int(bound_meta.get("returned_bytes", 0))
                    else:
                        # For execute_sql_query, we just record the size (it handles truncation)
                        resp_size = len(str(response).encode("utf-8"))
                        is_truncated, truncation_parse_failed = _extract_truncation_signal(response)

                    # 3. Record response attributes
                    span.set_attribute("mcp.tool.response.size_bytes", resp_size)
                    span.set_attribute("mcp.tool.response.truncated", is_truncated)
                    span.set_attribute(
                        "mcp.tool.response.truncation_parse_failed", truncation_parse_failed
                    )

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
