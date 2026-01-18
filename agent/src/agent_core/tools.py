"""MCP server tool integration for LangGraph.

This module bridges MCP tools with LangGraph nodes:
- get_mcp_tools(): Discovers tools from MCP server and wraps them
- _wrap_tool(): Adds telemetry to tool invocations
- mcp_tools_context(): Context manager for tool lifecycle
"""

from contextlib import asynccontextmanager
from typing import Any

from agent_core.mcp import MCPClient
from agent_core.mcp.tool_wrapper import create_tool_wrapper
from dotenv import load_dotenv

from common.config.env import get_env_str

load_dotenv()

# Default transport: SSE on /messages endpoint
DEFAULT_MCP_URL = "http://localhost:8000/messages"
DEFAULT_MCP_TRANSPORT = "sse"


async def get_mcp_tools():
    """Connect to MCP server and return LangGraph-compatible tool wrappers.

    Uses official MCP SDK for tool discovery.

    The MCP server provides secure, read-only database access through:
    - list_tables: Discover available tables
    - get_table_schema: Retrieve detailed schema metadata
    - execute_sql_query: Execute read-only SQL queries
    - get_semantic_definitions: Retrieve business metric definitions
    - search_relevant_tables: Semantic search for relevant tables

    Returns:
        list: List of MCPToolWrapper instances with telemetry wrapping.
    """
    mcp_url = get_env_str("MCP_SERVER_URL", DEFAULT_MCP_URL)
    mcp_transport = get_env_str("MCP_TRANSPORT", DEFAULT_MCP_TRANSPORT)

    # Create SDK client and discover tools
    client = MCPClient(server_url=mcp_url, transport=mcp_transport)

    async with client.connect() as mcp:
        tool_infos = await mcp.list_tools()

        # Create wrappers with bound invoke functions
        wrappers = []
        for info in tool_infos:
            # Capture current client context for invoke
            # Note: Each tool needs access to a connected session
            wrapper = create_tool_wrapper(
                name=info.name,
                description=info.description,
                input_schema=info.input_schema,
                invoke_fn=_create_invoke_fn(mcp_url, mcp_transport, info.name),
            )
            wrappers.append(_wrap_tool(wrapper))

        return wrappers


def _create_invoke_fn(server_url: str, transport: str, tool_name: str):
    """Create an async invoke function that connects and calls a tool.

    Each invocation creates a fresh connection to ensure proper session handling.

    Args:
        server_url: MCP server URL.
        transport: Transport type.
        tool_name: Name of the tool to invoke.

    Returns:
        Async function that invokes the tool with given arguments.
    """

    async def invoke(arguments: dict) -> Any:
        client = MCPClient(server_url=server_url, transport=transport)
        async with client.connect() as mcp:
            return await mcp.call_tool(tool_name, arguments)

    return invoke


def _wrap_tool(tool):
    """Wrap a tool with strict telemetry parity.

    Adds OTEL spans around tool invocations with input/output capture.

    Args:
        tool: MCPToolWrapper instance to wrap.

    Returns:
        The same tool instance with ainvoke patched for telemetry.
    """
    from agent_core.telemetry import telemetry
    from agent_core.telemetry_schema import SpanKind, TelemetryKeys, truncate_json

    original_ainvoke = tool.ainvoke

    async def wrapped_ainvoke(input: dict, config=None):
        # Truncate inputs if necessary
        inputs_json, truncated, size, sha256 = truncate_json(input)

        with telemetry.start_span(name=f"tool.{tool.name}", span_type=SpanKind.TOOL_CALL) as span:
            # Set standard attributes
            span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.TOOL_CALL)
            span.set_attribute(TelemetryKeys.EVENT_NAME, tool.name)
            span.set_attribute(TelemetryKeys.TOOL_NAME, tool.name)
            span.set_attribute(TelemetryKeys.INPUTS, inputs_json)
            span.set_attribute(TelemetryKeys.PAYLOAD_SIZE, size)
            if sha256:
                span.set_attribute(TelemetryKeys.PAYLOAD_HASH, sha256)
            if truncated:
                span.set_attribute(TelemetryKeys.PAYLOAD_TRUNCATED, True)

            try:
                # Execute original tool
                result = await original_ainvoke(input, config=config)

                # Capture outputs
                outputs_json, out_truncated, out_size, out_sha = truncate_json({"result": result})
                span.set_attribute(TelemetryKeys.OUTPUTS, outputs_json)

                return result

            except Exception as e:
                # Capture structured error
                error_info = {"error": str(e), "type": type(e).__name__}
                span.set_attribute(TelemetryKeys.ERROR, truncate_json(error_info)[0])
                # Re-raise to maintain agent behavior
                raise e

    # Also wrap _arun for StructuredTool compatibility
    original_arun = getattr(tool, "_arun", None)

    async def wrapped_arun(*args, config=None, **kwargs):
        # Build input dict
        input_dict = {}
        if args:
            if len(args) == 1 and isinstance(args[0], dict):
                input_dict = args[0]
            else:
                input_dict["args"] = args
        input_dict.update(kwargs)
        return await wrapped_ainvoke(input_dict, config=config)

    # Patch methods
    tool.ainvoke = wrapped_ainvoke
    if original_arun is not None:
        tool._arun = wrapped_arun

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
    from agent_core.utils.parsing import normalize_payload

    # Handle results wrapped in content list (e.g. from some proxies or older nodes)
    if isinstance(result, list) and result and isinstance(result[0], dict) and "type" in result[0]:
        text_content = ""
        for item in result:
            if item.get("type") == "text":
                text_content += item.get("text", "")

        if text_content:
            return normalize_payload(text_content)

    return result
