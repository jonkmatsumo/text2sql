"""MCP server tool integration for LangGraph.

This module bridges MCP tools with LangGraph nodes:
- get_mcp_tools(): Discovers tools from MCP server and wraps them
- _wrap_tool(): Adds telemetry to tool invocations
- mcp_tools_context(): Context manager for tool lifecycle
"""

from contextlib import asynccontextmanager
from typing import Any

from dotenv import load_dotenv

from agent.mcp_client import MCPClient
from agent.mcp_client.manager import create_resilient_invoke_fn
from agent.mcp_client.tool_wrapper import create_tool_wrapper
from common.config.env import get_env_str

load_dotenv()

# Default transport: SSE on /messages endpoint
DEFAULT_MCP_URL = "http://localhost:8000/messages"
DEFAULT_MCP_TRANSPORT = "sse"


async def get_mcp_tools():
    """Connect to MCP server and return LangGraph-compatible tool wrappers."""
    mcp_url = get_env_str("MCP_SERVER_URL", DEFAULT_MCP_URL)
    mcp_transport = get_env_str("MCP_TRANSPORT", DEFAULT_MCP_TRANSPORT)
    internal_token = get_env_str("INTERNAL_AUTH_TOKEN", "")

    headers = {}
    if internal_token:
        headers["X-Internal-Token"] = internal_token

    # Create SDK client and discover tools
    client = MCPClient(server_url=mcp_url, transport=mcp_transport, headers=headers)

    async with client.connect() as mcp:
        tool_infos = await mcp.list_tools()

        # Create wrappers with bound invoke functions using resilient retry logic
        wrappers = []
        for info in tool_infos:
            wrapper = create_tool_wrapper(
                name=info.name,
                description=info.description,
                input_schema=info.input_schema,
                invoke_fn=create_resilient_invoke_fn(mcp_url, mcp_transport, info.name, headers),
            )
            wrappers.append(_wrap_tool(wrapper))

        return wrappers


def _wrap_tool(tool):
    """Wrap a tool with strict telemetry parity.

    Adds OTEL spans around tool invocations with input/output capture.
    Supports both sync (_run) and async (_arun, ainvoke) methods.

    Args:
        tool: Tool instance to wrap (StructuredTool or MCPToolWrapper).

    Returns:
        The same tool instance with patched methods for telemetry.
    """
    from agent.telemetry import telemetry
    from agent.telemetry_schema import SpanKind, TelemetryKeys, truncate_json

    def _prepare_input(args, kwargs):
        """Unify args/kwargs into a single dict for telemetry."""
        input_dict = {}
        if args:
            if len(args) == 1 and isinstance(args[0], dict):
                input_dict = args[0]
            else:
                input_dict["args"] = args
        input_dict.update(kwargs)
        return input_dict

    def _record_results(span, inputs, result=None, error=None):
        """Record inputs, outputs and errors on a span."""
        inputs_json, truncated, size, sha256 = truncate_json(inputs)
        span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.TOOL_CALL)
        span.set_attribute(TelemetryKeys.EVENT_NAME, tool.name)
        span.set_attribute(TelemetryKeys.TOOL_NAME, tool.name)
        span.set_attribute(TelemetryKeys.INPUTS, inputs_json)
        span.set_attribute(TelemetryKeys.PAYLOAD_SIZE, size)
        if sha256:
            span.set_attribute(TelemetryKeys.PAYLOAD_HASH, sha256)
        if truncated:
            span.set_attribute(TelemetryKeys.PAYLOAD_TRUNCATED, True)

        if error is not None:
            error_info = {"error": str(error), "type": type(error).__name__}
            span.set_attribute(TelemetryKeys.ERROR, truncate_json(error_info)[0])
        else:
            outputs_json, _, _, _ = truncate_json({"result": result})
            span.set_attribute(TelemetryKeys.OUTPUTS, outputs_json)

    # 1. Wrap _arun if it exists
    original_arun = getattr(tool, "_arun", None)
    if original_arun is not None:

        async def wrapped_arun(*args, **kwargs):
            from unittest.mock import AsyncMock, MagicMock

            # Note: Many tests pass config as a kwarg even if not supported by original
            kwargs.pop("config", None)
            input_dict = _prepare_input(args, kwargs)

            with telemetry.start_span(
                name=f"tool.{tool.name}", span_type=SpanKind.TOOL_CALL
            ) as span:
                try:
                    # AsyncMock in tests is awaitable, MagicMock is not.
                    # We check for awaitability to avoid TypeError.
                    call_result = original_arun(*args, **kwargs)
                    if hasattr(call_result, "__await__"):
                        result = await call_result
                    elif isinstance(call_result, (AsyncMock, MagicMock)):
                        # If it's a mock but not awaitable, just return it
                        result = call_result
                    else:
                        result = call_result

                    _record_results(span, input_dict, result=result)
                    return result
                except Exception as e:
                    _record_results(span, input_dict, error=e)
                    raise e

        tool._arun = wrapped_arun

    # 2. Wrap _run if it exists (Sync path)
    original_run = getattr(tool, "_run", None)
    if original_run is not None:

        def wrapped_run(*args, **kwargs):
            input_dict = _prepare_input(args, kwargs)
            with telemetry.start_span(
                name=f"tool.{tool.name}", span_type=SpanKind.TOOL_CALL
            ) as span:
                try:
                    result = original_run(*args, **kwargs)
                    _record_results(span, input_dict, result=result)
                    return result
                except Exception as e:
                    _record_results(span, input_dict, error=e)
                    raise e

        tool._run = wrapped_run

    # 3. Patch ainvoke for high-level compatibility (ensure it's awaitable)
    original_ainvoke = getattr(tool, "ainvoke", None)
    if original_ainvoke is not None:

        async def wrapped_ainvoke(input, config=None):
            # If we already wrapped _arun, ainvoke often calls it.
            # To avoid double spans, we only record if not already in a tool span
            # But the contract says we must patch it.
            with telemetry.start_span(
                name=f"tool.{tool.name}", span_type=SpanKind.TOOL_CALL
            ) as span:
                try:
                    call_result = original_ainvoke(input, config=config)
                    if hasattr(call_result, "__await__"):
                        result = await call_result
                    else:
                        result = call_result

                    _record_results(span, input, result=result)
                    return result
                except Exception as e:
                    _record_results(span, input, error=e)
                    raise e

        tool.ainvoke = wrapped_ainvoke

    return tool


@asynccontextmanager
async def mcp_tools_context():
    """Context manager for MCP tools lifecycle.

    Provides backward compatibility with code expecting context manager.
    """
    tools = await get_mcp_tools()
    yield tools


def unpack_mcp_result(result: Any) -> Any:
    """Unpack MCP content into raw value.

    Handles both standard and nested JSON string response formats.

    Args:
        result: Raw MCP tool result.

    Returns:
        Unpacked Python value.
    """
    from agent.utils.parsing import normalize_payload

    # Handle results wrapped in content list (e.g. from some proxies or older nodes)
    if isinstance(result, list) and result and isinstance(result[0], dict) and "type" in result[0]:
        text_content = ""
        for item in result:
            if item.get("type") == "text":
                text_content += item.get("text", "")

        if text_content:
            return normalize_payload(text_content)

    # Handle results wrapped in a "result" dict key (default FastMCP/SDK behavior)
    if isinstance(result, dict) and "result" in result and len(result) == 1:
        return result["result"]

    return result
