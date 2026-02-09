"""MCP tool output bounding and truncation utilities."""

import json
import logging
from typing import Any, Dict, Optional, Tuple

from common.config.env import get_env_int
from mcp_server.utils.json_budget import JSONBudget

logger = logging.getLogger(__name__)


def bound_tool_output(obj: Any, max_bytes: Optional[int] = None) -> Tuple[Any, Dict[str, Any]]:
    """Bound a tool response object by byte size, truncating if necessary.

    Args:
        obj: The object to bound (expected to be a dict or list).
        max_bytes: Maximum allowed bytes. Defaults to MCP_JSON_PAYLOAD_LIMIT_BYTES.

    Returns:
        A tuple of (bounded_obj, metadata).
    """
    if max_bytes is None:
        max_bytes = get_env_int("MCP_JSON_PAYLOAD_LIMIT_BYTES", 2 * 1024 * 1024)

    budget = JSONBudget(max_bytes)

    # Estimate original size
    try:
        original_json = json.dumps(obj, default=str, separators=(",", ":"))
        original_bytes = len(original_json.encode("utf-8"))
    except Exception:
        original_bytes = 0

    truncated = False
    reason = None

    # Truncation logic
    if isinstance(obj, list):
        bounded_obj = []
        for item in obj:
            if budget.consume(item):
                bounded_obj.append(item)
            else:
                truncated = True
                reason = "SIZE_LIMIT"
                break
    elif isinstance(obj, dict) and "result" in obj and isinstance(obj["result"], list):
        # Truncate the 'result' list within an envelope-like dict
        # First consume the metadata overhead
        meta_only = {k: v for k, v in obj.items() if k != "result"}
        budget.consume(meta_only)

        bounded_list = []
        for item in obj["result"]:
            if budget.consume(item):
                bounded_list.append(item)
            else:
                truncated = True
                reason = "SIZE_LIMIT"
                break
        bounded_obj = obj.copy()
        bounded_obj["result"] = bounded_list
    else:
        # Non-truncatable or single object
        if budget.consume(obj):
            bounded_obj = obj
        else:
            truncated = True
            reason = "SIZE_LIMIT"
            # If we can't fit even the metadata, return a minimal error
            bounded_obj = {
                "error": "Response size limit exceeded",
                "original_size": original_bytes,
                "limit": max_bytes,
            }

    # Final size estimate
    try:
        returned_json = json.dumps(bounded_obj, default=str, separators=(",", ":"))
        returned_bytes = len(returned_json.encode("utf-8"))
    except Exception:
        returned_bytes = 0

    metadata = {
        "truncated": truncated,
        "original_bytes": original_bytes,
        "returned_bytes": returned_bytes,
        "reason": reason,
    }

    return bounded_obj, metadata
