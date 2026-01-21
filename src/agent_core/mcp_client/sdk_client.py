"""MCP SDK Client Wrapper.

Thin wrapper around the official MCP Python SDK ClientSession that:
- Handles SSE and streamable-http transports
- Provides async context manager for session lifecycle
- Exposes list_tools() and call_tool() with normalized payloads
"""

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Optional

from mcp import ClientSession

from agent_core.utils.parsing import normalize_payload

logger = logging.getLogger(__name__)


@dataclass
class ToolInfo:
    """Metadata for an MCP tool."""

    name: str
    description: str
    input_schema: dict


class MCPClient:
    """Thin wrapper around official MCP SDK ClientSession.

    Provides tool discovery and invocation with normalized payloads.

    Args:
        server_url: MCP server endpoint (e.g., http://localhost:8000/messages)
        transport: Transport type ("sse" or "streamable-http")
    """

    def __init__(self, server_url: str, transport: str = "sse"):
        """Initialize MCP client configuration.

        Args:
            server_url: The MCP server endpoint URL.
            transport: Transport protocol ("sse" or "streamable-http").
        """
        self.server_url = server_url
        self.transport = transport.lower()
        self._session: Optional[ClientSession] = None
        self._streams = None
        self._exit_stack = None

        # Validate transport
        if self.transport not in ("sse", "streamable-http"):
            raise ValueError(
                f"Unsupported MCP_TRANSPORT: {self.transport}. "
                "Supported by client: sse, streamable-http"
            )

        if self.transport == "streamable-http":
            raise ValueError(
                "MCP_TRANSPORT=streamable-http is not currently supported by the "
                "mcp-server configuration. Please use 'sse' for production realism."
            )

    @asynccontextmanager
    async def connect(self):
        """Async context manager for establishing MCP session.

        Yields:
            Self with active session for tool operations.

        Example:
            async with client.connect() as mcp:
                tools = await mcp.list_tools()
        """
        from contextlib import AsyncExitStack

        async with AsyncExitStack() as stack:
            if self.transport == "sse":
                from mcp.client.sse import sse_client

                read_stream, write_stream = await stack.enter_async_context(
                    sse_client(self.server_url)
                )
            else:  # streamable-http
                from mcp.client.streamable_http import streamable_http_client

                read_stream, write_stream, _ = await stack.enter_async_context(
                    streamable_http_client(self.server_url)
                )

            session = await stack.enter_async_context(ClientSession(read_stream, write_stream))
            await session.initialize()

            self._session = session
            try:
                yield self
            finally:
                self._session = None

    async def list_tools(self) -> list[ToolInfo]:
        """List available tools from the MCP server.

        Returns:
            List of ToolInfo with name, description, and input_schema.

        Raises:
            RuntimeError: If called outside of connect() context.
        """
        if self._session is None:
            raise RuntimeError("MCPClient.list_tools() must be called within connect()")

        result = await self._session.list_tools()
        return [
            ToolInfo(
                name=tool.name,
                description=tool.description or "",
                input_schema=tool.inputSchema or {},
            )
            for tool in result.tools
        ]

    async def call_tool(self, name: str, arguments: dict) -> Any:
        """Invoke an MCP tool and return normalized result.

        Args:
            name: The tool name to invoke.
            arguments: Dictionary of tool arguments.

        Returns:
            Normalized Python object from tool response.

        Raises:
            RuntimeError: If called outside of connect() context.
            Exception: If tool execution fails (isError=True).
        """
        if self._session is None:
            raise RuntimeError("MCPClient.call_tool() must be called within connect()")

        from mcp import types

        result = await self._session.call_tool(name, arguments=arguments)

        # Check for tool-level error
        if result.isError:
            error_msg = "Tool execution failed"
            for content in result.content:
                if isinstance(content, types.TextContent):
                    error_msg = content.text
                    break
            raise Exception(f"MCP tool '{name}' error: {error_msg}")

        # Use structuredContent if available, otherwise parse TextContent
        if result.structuredContent is not None:
            return result.structuredContent

        # Aggregate and normalize all text content
        aggregated = []
        for content in result.content:
            if isinstance(content, types.TextContent):
                normalized = normalize_payload(content.text)
                if isinstance(normalized, list):
                    aggregated.extend(normalized)
                else:
                    aggregated.append(normalized)

        # Return single item if only one result, otherwise list
        if len(aggregated) == 1:
            return aggregated[0]
        return aggregated
