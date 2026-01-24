#!/usr/bin/env python3
r"""MCP SDK Client Spike Script.

Non-production script to validate official MCP Python SDK connectivity.

Usage:
    MCP_SERVER_URL=http://localhost:8000/messages MCP_TRANSPORT=sse \
        python agent/scripts/mcp_sdk_spike.py

Requirements:
    - pip install "mcp[cli]"
    - MCP server running on specified URL

Payload Observations:
    - SDK returns CallToolResult with .content (list of TextContent) and .structuredContent
    - TextContent.text contains single-encoded JSON
    - Error responses: result.isError=True, error message in TextContent
    - Tool schemas available via tool.inputSchema (dict)
"""

import asyncio
import json
import os
import sys

# Add agent src to path for common imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common.config.env import get_env_str  # noqa: E402


async def run_spike():
    """Connect to MCP server and test tool discovery/invocation."""
    server_url = get_env_str("MCP_SERVER_URL", "http://localhost:8000/messages")
    transport = get_env_str("MCP_TRANSPORT", "sse").lower()

    print("=== MCP SDK Spike ===")
    print(f"Server URL: {server_url}")
    print(f"Transport: {transport}")
    print()

    # Use MCPClient for unified transport handling and validation
    from agent.mcp_client import MCPClient

    try:
        client = MCPClient(server_url=server_url, transport=transport)
        async with client.connect() as mcp:
            print(f"Connected using {transport}...")
            # Fetch the raw session from the wrapper to reuse existing test logic
            await _test_session(mcp._session)
    except ValueError as ve:
        print(f"FAIL-FAST ERROR: {ve}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


async def _test_session(session):
    """Run tool discovery and invocation tests."""
    # Local import to access types for isinstance checks
    from mcp import types  # noqa: F401

    # 1. List tools
    print("--- list_tools() ---")
    tools_result = await session.list_tools()
    print(f"Found {len(tools_result.tools)} tools:")
    for tool in tools_result.tools:
        schema_keys = (
            list(tool.inputSchema.get("properties", {}).keys()) if tool.inputSchema else []
        )
        print(f"  - {tool.name}: {tool.description[:50]}... (params: {schema_keys})")
    print()

    # 2. Call list_tables
    print("--- call_tool('list_tables', {}) ---")
    try:
        result = await session.call_tool("list_tables", arguments={})
        _print_result(result)
    except Exception as e:
        print(f"ERROR: {e}")
    print()

    # 3. Call execute_sql_query
    print("--- call_tool('execute_sql_query', {'sql_query': 'SELECT 1 AS test'}) ---")
    try:
        result = await session.call_tool(
            "execute_sql_query", arguments={"sql_query": "SELECT 1 AS test"}
        )
        _print_result(result)
    except Exception as e:
        print(f"ERROR: {e}")
    print()

    # 4. Test error handling with invalid query
    print("--- call_tool('execute_sql_query', {'sql_query': 'INVALID SQL'}) ---")
    try:
        result = await session.call_tool(
            "execute_sql_query", arguments={"sql_query": "INVALID SQL"}
        )
        _print_result(result)
    except Exception as e:
        print(f"ERROR (expected): {e}")

    print()

    print("=== Spike Complete ===")


def _print_result(result):
    """Print CallToolResult for payload shape analysis."""
    from mcp import types

    print(f"  isError: {result.isError}")
    print(f"  structuredContent: {result.structuredContent}")
    print(f"  content count: {len(result.content)}")

    for i, content in enumerate(result.content):
        if isinstance(content, types.TextContent):
            text = content.text
            # Try to parse as JSON for pretty printing
            try:
                parsed = json.loads(text)
                print(f"  content[{i}] (TextContent, parsed JSON):")
                print(f"    {json.dumps(parsed, indent=2)[:500]}")
            except json.JSONDecodeError:
                print(f"  content[{i}] (TextContent, raw):")
                print(f"    {text[:500]}")
        else:
            print(f"  content[{i}]: {type(content).__name__}")


if __name__ == "__main__":
    asyncio.run(run_spike())
