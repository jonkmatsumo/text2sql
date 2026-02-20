"""Context module for MCP tool execution.

Provides standardized access to user identity, roles, and tenant scoping.
"""

import os
from typing import Optional, Set

from pydantic import BaseModel, Field


class ToolContext(BaseModel):
    """Standardized context passed to or resolved by MCP tools."""

    tenant_id: Optional[int] = Field(None, description="The scoped tenant ID for the request.")
    user_role: Set[str] = Field(
        default_factory=set, description="Set of roles assigned to the user."
    )
    request_id: Optional[str] = Field(None, description="Unique trace/request identifier.")

    def has_role(self, role: str) -> bool:
        """Check if the context has the specified role."""
        return role.upper() in {r.upper() for r in self.user_role}

    @classmethod
    def from_env(cls, tenant_id: Optional[int] = None) -> "ToolContext":
        """Resolve context from environment and provided tenant_id."""
        from common.config.env import get_env_str
        from common.observability.context import request_id_var

        roles_str = get_env_str("MCP_USER_ROLE", "")
        roles = {r.strip().upper() for r in roles_str.split(",") if r.strip()}
        request_id = request_id_var.get() or os.environ.get("OTEL_TRACE_ID")

        return cls(
            tenant_id=tenant_id,
            user_role=roles,
            request_id=request_id,  # Best effort fallback for non-request contexts
        )
