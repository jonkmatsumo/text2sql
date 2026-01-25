"""MCP SDK Client and Tool Wrapper Package.

Provides a thin wrapper around the official MCP Python SDK,
exposing LangGraph-compatible tool interfaces.

Exports:
- MCPClient: Handles connection, tool discovery, and tool invocation
- MCPToolWrapper: LangGraph-compatible tool with ainvoke()
"""

from agent.mcp_client.sdk_client import MCPClient
from agent.mcp_client.tool_wrapper import MCPToolWrapper

__all__ = ["MCPClient", "MCPToolWrapper"]
