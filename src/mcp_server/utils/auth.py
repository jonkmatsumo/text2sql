"""Authorization utilities for MCP tools."""

from typing import Optional, Set

from common.config.env import get_env_str


def get_user_roles() -> Set[str]:
    """Get roles for the current user from environment."""
    roles_str = get_env_str("MCP_USER_ROLE", "")
    if not roles_str:
        return set()
    return {r.strip().upper() for r in roles_str.split(",") if r.strip()}


def has_role(required_role: str) -> bool:
    """Check if the current user has the required role."""
    # If no roles defined, fail closed unless it's a non-gated tool
    # (But we only call this for gated tools)
    user_roles = get_user_roles()
    return required_role.upper() in user_roles


def validate_role(
    required_role: str, tool_name: str, tenant_id: Optional[int] = None
) -> Optional[str]:
    """Check role and return standardized error if missing.

    Args:
        required_role: The role required for the tool.
        tool_name: Name of the tool for error reporting.
        tenant_id: Optional tenant ID for context.

    Returns:
        JSON error string if unauthorized, None otherwise.
    """
    from mcp_server.utils.context import ToolContext

    context = ToolContext.from_env(tenant_id=tenant_id)

    if not context.has_role(required_role):
        from common.models.error_metadata import ErrorMetadata
        from common.models.tool_envelopes import ToolResponseEnvelope

        envelope = ToolResponseEnvelope(
            result=None,
            error=ErrorMetadata(
                message=f"Unauthorized: Role '{required_role}' required for tool '{tool_name}'.",
                category="auth",
                provider="mcp_server",
                is_retryable=False,
            ),
        )
        return envelope.model_dump_json(exclude_none=True)
    return None
