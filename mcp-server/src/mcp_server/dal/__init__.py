"""Data Abstraction Layer (DAL) for the MCP Server.

This package exposes the core interfaces and implementation-agnostic contexts for data access,
including the Function Call Context for tenant isolation.
"""

from mcp_server.utils.tenant_context import (
    get_current_tenant,
    reset_tenant_context,
    set_current_tenant,
    tenant_context,
)

__all__ = [
    "get_current_tenant",
    "reset_tenant_context",
    "set_current_tenant",
    "tenant_context",
]
