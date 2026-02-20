"""Canonical reserved metadata keys for MCP tool invocation wrappers."""

from __future__ import annotations

from typing import Any, Mapping

TRACE_CONTEXT_RESERVED_FIELD = "_trace_context"
REQUEST_ID_RESERVED_FIELD = "_request_id"

RESERVED_TOOL_METADATA_KEYS = frozenset(
    {
        TRACE_CONTEXT_RESERVED_FIELD,
        REQUEST_ID_RESERVED_FIELD,
    }
)


def split_reserved_tool_metadata(
    payload: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split a payload into sanitized kwargs and reserved metadata kwargs."""
    sanitized: dict[str, Any] = {}
    reserved: dict[str, Any] = {}

    if not isinstance(payload, Mapping):
        return sanitized, reserved

    for key, value in payload.items():
        if key in RESERVED_TOOL_METADATA_KEYS:
            reserved[key] = value
        else:
            sanitized[key] = value

    return sanitized, reserved
