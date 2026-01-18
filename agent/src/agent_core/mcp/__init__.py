"""MCP SDK Client and Tool Wrapper Package.

This package provides a thin wrapper around the official MCP Python SDK,
exposing LangGraph-compatible tool interfaces.

Phase 3 - Issue #166:
- MCPClient: Handles connection, tool discovery, and tool invocation
- MCPToolWrapper: LangGraph-compatible tool with ainvoke()
"""

from agent_core.mcp.sdk_client import MCPClient
from agent_core.mcp.tool_wrapper import MCPToolWrapper

__all__ = ["MCPClient", "MCPToolWrapper"]
