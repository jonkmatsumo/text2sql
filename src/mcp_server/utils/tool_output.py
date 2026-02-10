"""MCP tool output bounding and truncation utilities."""

import json
import logging
from typing import Any, Dict, Optional, Tuple

from common.config.env import get_env_int
from mcp_server.utils.json_budget import JSONBudget

logger = logging.getLogger(__name__)


def _safe_env_int(name: str, default: int) -> int:
    try:
        value = get_env_int(name, default)
    except ValueError:
        logger.warning("Invalid %s value; defaulting to %s", name, default)
        return default
    if value is None:
        return default
    return int(value)


def _truncate_utf8_text(value: str, max_bytes: int) -> tuple[str, bool]:
    if max_bytes <= 0:
        return "", bool(value)

    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value, False

    clipped = encoded[:max_bytes]
    while clipped:
        try:
            decoded = clipped.decode("utf-8")
            return decoded + "...", True
        except UnicodeDecodeError:
            clipped = clipped[:-1]
    return "...", True


def _bound_strings(payload: Any, max_string_bytes: int) -> tuple[Any, bool]:
    if isinstance(payload, str):
        return _truncate_utf8_text(payload, max_string_bytes)

    if isinstance(payload, list):
        bounded_items = []
        truncated = False
        for item in payload:
            bounded_item, item_truncated = _bound_strings(item, max_string_bytes)
            truncated = truncated or item_truncated
            bounded_items.append(bounded_item)
        return bounded_items, truncated

    if isinstance(payload, dict):
        bounded_dict = {}
        truncated = False
        for key, value in payload.items():
            bounded_value, value_truncated = _bound_strings(value, max_string_bytes)
            truncated = truncated or value_truncated
            bounded_dict[key] = bounded_value
        return bounded_dict, truncated

    return payload, False


def _bound_items(payload: Any, max_items: int) -> tuple[Any, bool, Optional[int], Optional[int]]:
    if max_items <= 0:
        max_items = 1

    if isinstance(payload, list):
        total_items = len(payload)
        if total_items > max_items:
            bounded = payload[:max_items]
            return bounded, True, total_items, len(bounded)
        return payload, False, total_items, total_items

    if isinstance(payload, dict) and "result" in payload:
        result = payload["result"]
        if isinstance(result, list):
            total_items = len(result)
            bounded = payload.copy()
            if total_items > max_items:
                bounded["result"] = result[:max_items]
                return bounded, True, total_items, len(bounded["result"])
            return bounded, False, total_items, total_items

        if isinstance(result, dict):
            total_items = len(result)
            bounded = payload.copy()
            if total_items > max_items:
                bounded["result"] = dict(list(result.items())[:max_items])
                return bounded, True, total_items, len(bounded["result"])
            return bounded, False, total_items, total_items

    return payload, False, None, None


def _extract_item_count(payload: Any) -> Optional[int]:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict) and "result" in payload:
        result = payload["result"]
        if isinstance(result, (list, dict)):
            return len(result)
    return None


def _bound_by_bytes(payload: Any, max_bytes: int) -> tuple[Any, bool]:
    budget = JSONBudget(max_bytes)

    if isinstance(payload, list):
        bounded = []
        for item in payload:
            if budget.consume(item):
                bounded.append(item)
            else:
                return bounded, True
        return bounded, False

    if isinstance(payload, dict) and "result" in payload:
        result = payload["result"]
        if isinstance(result, list):
            meta_only = {k: v for k, v in payload.items() if k != "result"}
            if not budget.consume(meta_only):
                return (
                    {
                        "error": "Response size limit exceeded",
                        "limit": max_bytes,
                    },
                    True,
                )

            bounded_result = []
            for item in result:
                if budget.consume(item):
                    bounded_result.append(item)
                else:
                    bounded = payload.copy()
                    bounded["result"] = bounded_result
                    return bounded, True

            bounded = payload.copy()
            bounded["result"] = bounded_result
            return bounded, False

        if isinstance(result, dict):
            meta_only = {k: v for k, v in payload.items() if k != "result"}
            if not budget.consume(meta_only):
                return (
                    {
                        "error": "Response size limit exceeded",
                        "limit": max_bytes,
                    },
                    True,
                )

            bounded_result = {}
            for key, value in result.items():
                if budget.consume({key: value}):
                    bounded_result[key] = value
                else:
                    bounded = payload.copy()
                    bounded["result"] = bounded_result
                    return bounded, True

            bounded = payload.copy()
            bounded["result"] = bounded_result
            return bounded, False

    if budget.consume(payload):
        return payload, False

    return (
        {
            "error": "Response size limit exceeded",
            "limit": max_bytes,
        },
        True,
    )


def _json_size(payload: Any) -> int:
    try:
        return len(json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8"))
    except Exception:
        return 0


def bound_tool_output(
    obj: Any,
    max_bytes: Optional[int] = None,
    max_items: Optional[int] = None,
    max_string_bytes: Optional[int] = None,
) -> Tuple[Any, Dict[str, Any]]:
    """Bound a tool response object by item count, string size, and byte size.

    Args:
        obj: The object to bound (expected to be a dict or list).
        max_bytes: Maximum allowed payload bytes.
        max_items: Maximum allowed items in result collections.
        max_string_bytes: Maximum bytes per string value.

    Returns:
        A tuple of (bounded_obj, metadata).
    """
    if max_bytes is None:
        max_bytes = _safe_env_int("MCP_JSON_PAYLOAD_LIMIT_BYTES", 2 * 1024 * 1024)
    if max_items is None:
        max_items = _safe_env_int("MCP_MAX_ITEMS", 200)
    if max_string_bytes is None:
        max_string_bytes = _safe_env_int("MCP_MAX_STRING_BYTES", 16 * 1024)

    original_bytes = _json_size(obj)
    reason: Optional[str] = None

    bounded_obj, items_truncated, total_items, returned_items = _bound_items(obj, max_items)
    if items_truncated:
        reason = "max_items"

    bounded_obj, strings_truncated = _bound_strings(bounded_obj, max_string_bytes)
    if strings_truncated and reason is None:
        reason = "max_string_bytes"

    bounded_obj, bytes_truncated = _bound_by_bytes(bounded_obj, max_bytes)
    if bytes_truncated and reason is None:
        reason = "max_bytes"

    returned_bytes = _json_size(bounded_obj)
    if total_items is None:
        total_items = _extract_item_count(obj)
    returned_items = _extract_item_count(bounded_obj)

    metadata = {
        "truncated": bool(items_truncated or strings_truncated or bytes_truncated),
        "original_bytes": original_bytes,
        "returned_bytes": returned_bytes,
        "reason": reason,
        "total_items": total_items,
        "returned_items": returned_items,
    }

    return bounded_obj, metadata


def apply_truncation_metadata(payload: Any, meta: Dict[str, Any]) -> Any:
    """Inject standardized truncation metadata into tool envelope payloads."""
    if not isinstance(payload, dict):
        return payload

    out = payload.copy()
    raw_metadata = out.get("metadata")
    metadata = raw_metadata.copy() if isinstance(raw_metadata, dict) else {}

    metadata["is_truncated"] = bool(meta.get("truncated", False))
    if meta.get("reason"):
        metadata["truncation_reason"] = meta["reason"]
    if meta.get("returned_items") is not None:
        metadata["items_returned"] = int(meta["returned_items"])
    if meta.get("total_items") is not None:
        metadata["items_total"] = int(meta["total_items"])
    if meta.get("returned_bytes") is not None:
        metadata["bytes_returned"] = int(meta["returned_bytes"])
    if meta.get("original_bytes") is not None:
        metadata["bytes_total"] = int(meta["original_bytes"])

    out["metadata"] = metadata
    return out
