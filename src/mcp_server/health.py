"""Health and initialization state tracking for MCP server.

This module provides structured tracking of startup initialization steps
to enable meaningful health endpoint responses.
"""

from __future__ import annotations

from mcp_server.models.health import InitializationState

# Global initialization state instance for the MCP server
init_state = InitializationState()
