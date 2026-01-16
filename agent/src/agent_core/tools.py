"""MCP server tool integration for LangGraph."""

from contextlib import asynccontextmanager
from typing import Any

from dotenv import load_dotenv

from common.config.env import get_env_str

load_dotenv()

# Default URL for sse transport (endpoint is /messages)
DEFAULT_MCP_URL = "http://localhost:8000/messages"


async def get_mcp_tools():
    """
    Connect to the local MCP server via streamable-http.

    The MCP server provides secure, read-only database access through:
    - list_tables: Discover available tables
    - get_table_schema: Retrieve detailed schema metadata
    - execute_sql_query: Execute read-only SQL queries
    - get_semantic_definitions: Retrieve business metric definitions
    - search_relevant_tables: Semantic search for relevant tables

    Returns:
        list: List of LangChain tool wrappers for MCP tools
    """
    mcp_url = get_env_str("MCP_SERVER_URL", DEFAULT_MCP_URL)
    from langchain_mcp_adapters.client import MultiServerMCPClient

    # Use sse transport for compatibility
    client = MultiServerMCPClient(
        {
            "data-layer": {
                "url": mcp_url,
                "transport": "sse",
            }
        }
    )

    # Returns tools: list_tables, execute_sql_query, get_semantic_definitions,
    # get_table_schema, search_relevant_tables
    tools = await client.get_tools()
    return [_wrap_tool(t) for t in tools]


def _wrap_tool(tool):
    """Wrap a LangChain tool with strict telemetry parity."""
    from agent_core.telemetry import telemetry
    from agent_core.telemetry_schema import SpanKind, TelemetryKeys, truncate_json

    original_arun = tool._arun

    async def wrapped_arun(*args, **kwargs):
        # Tools typically take a single string argument or a dict of args
        # We need to capture this input safely
        inputs = {}
        if args:
            inputs["args"] = args
        if kwargs:
            inputs.update(kwargs)

        # Truncate inputs if necessary
        inputs_json, truncated, size, sha256 = truncate_json(inputs)

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
                result = await original_arun(*args, **kwargs)

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

    # Patch the async run method
    # Note: We only patch async because our agent is async-first.
    # If sync usage is needed, _run should be patched similarly.
    tool._arun = wrapped_arun
    return tool


@asynccontextmanager
async def mcp_tools_context():
    """Context manager for backward compatibility and future stability."""
    # Since MultiServerMCPClient 0.1.0 doesn't support context manager directly,
    # we just yield the tools for now.
    tools = await get_mcp_tools()
    yield tools


def unpack_mcp_result(result: Any) -> Any:
    """Unpack standardized MCP content list/dict into raw value."""
    import json

    # LangChain MCP adapter returns a list of dicts like [{'type': 'text', 'text': '...'}]
    if isinstance(result, list) and result and isinstance(result[0], dict) and "type" in result[0]:
        text_content = ""
        for item in result:
            if item.get("type") == "text":
                text_content += item.get("text", "")

        # Try parsing as JSON if it looks like a JSON object/list
        stripped = text_content.strip()
        if (stripped.startswith("{") and stripped.endswith("}")) or (
            stripped.startswith("[") and stripped.endswith("]")
        ):
            try:
                return json.loads(text_content)
            except json.JSONDecodeError:
                pass
        return text_content

    return result
