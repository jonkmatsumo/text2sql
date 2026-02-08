import functools
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
                # Enforcement: specific attributes must be present?
                # Or just checks.

                if not span.is_recording():
                    # If span is not recording, it usually means sampling dropped it
                    # OR tracing is disabled.
                    # In "error" mode, we might be strict, but we shouldn't crash on sampling.
                    # However, if we expected a parent span and didn't get one, that's different.
                    # For now, just log warning if mode is strict.
                    if mode in ("warn", "error"):
                        logger.warning(
                            f"Tool {tool_name} executed without active recording span. "
                            f"Mode={mode}."
                        )

                span.set_attribute("mcp.tool.name", tool_name)

                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise

        return wrapper

    return decorator
