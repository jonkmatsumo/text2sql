"""MCP Tool Output Parsing Utilities.

Provides robust parsing for MCP tool outputs, supporting:
- Official MCP SDK payloads (single JSON encoding in TextContent.text)
- Double-encoded JSON strings (e.g. stringified JSON within a JSON field)

Payload Shape Reference:
- SDK: result.content = [TextContent(text='{"key": "value"}')]
- SDK: result.structuredContent = {"key": "value"} (if available)
"""

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def detect_double_json_encoding(payload: Any) -> bool:
    """Detect if payload has double JSON encoding.

    In some cases, JSON results are stringified twice, so parsing once
    yields another JSON string instead of the final data structure.

    Args:
        payload: The raw payload to check (typically a string).

    Returns:
        True if the payload appears to be double-encoded.
    """
    if not isinstance(payload, str):
        return False

    try:
        first_parse = json.loads(payload)
        # If first parse yields a string that is also valid JSON, it's double-encoded
        if isinstance(first_parse, str):
            try:
                json.loads(first_parse)
                return True
            except (json.JSONDecodeError, TypeError):
                return False
    except (json.JSONDecodeError, TypeError):
        return False

    return False


def normalize_payload(payload: Any) -> Any:
    """Normalize SDK or wrapped payloads to Python objects.

    Handles:
    - Already-parsed dicts/lists (pass through)
    - Single-encoded JSON strings
    - Double-encoded JSON strings

    Args:
        payload: The raw payload (string, dict, list, or MCP content object).

    Returns:
        Normalized Python object (dict, list, or primitive).

    Raises:
        ValueError: If payload cannot be parsed as valid JSON.
    """
    # Already parsed - pass through
    if isinstance(payload, (dict, list)):
        return payload

    # Handle MCP TextContent objects
    if hasattr(payload, "text"):
        payload = payload.text

    # Not a string - return as-is
    if not isinstance(payload, str):
        return payload

    # Empty string - return as-is
    if not payload.strip():
        return payload

    # Try to parse JSON
    try:
        parsed = json.loads(payload)

        # Check for double-encoding
        if isinstance(parsed, str):
            try:
                return json.loads(parsed)
            except (json.JSONDecodeError, TypeError):
                # Not double-encoded, just a JSON string value
                return parsed

        return parsed

    except (json.JSONDecodeError, TypeError) as e:
        # Not valid JSON - could be plain text error message
        logger.debug(f"Payload is not JSON, treating as raw text: {str(e)[:50]}")
        return payload


def parse_tool_output(tool_output):
    """Robustly parse MCP tool outputs.

    Handles tool outputs that may be wrapped in LangChain Message lists,
    stringified, or double-encoded.

    Args:
        tool_output: Raw output from MCP tool invocation.

    Returns:
        List of parsed result objects.
    """
    aggregated_results = []

    def _normalize_inputs(value):
        if isinstance(value, tuple) and len(value) == 2:
            content, _artifact = value
            return _normalize_inputs(content)
        if isinstance(value, list):
            if value and all(isinstance(item, dict) and "type" in item for item in value):
                return value
            return value
        return [value]

    # Ensure input is a list to unify processing logic
    inputs = _normalize_inputs(tool_output)

    for item in inputs:
        raw_payload = None

        # 1. Extract the raw string payload
        if isinstance(item, dict):
            if item.get("type") == "text":
                raw_payload = item.get("text")
            else:
                raw_payload = item.get("text") or item.get("content")
            # If no message wrapper keys found, treat the dict itself as a data chunk
            if raw_payload is None:
                aggregated_results.append(item)
                continue
        elif hasattr(item, "text"):
            raw_payload = item.text
        elif hasattr(item, "content"):
            raw_payload = item.content
        elif isinstance(item, str):
            raw_payload = item
        else:
            # Fallback for primitives (int, float, etc) or unknown types
            aggregated_results.append(item)
            continue

        if not raw_payload:
            continue

        # 2. Use normalize_payload for consistent handling
        try:
            normalized = normalize_payload(raw_payload)

            # 3. Aggregate Results
            if isinstance(normalized, list):
                aggregated_results.extend(normalized)
            else:
                aggregated_results.append(normalized)

        except Exception as e:
            logger.warning(
                f"Failed to parse tool output chunk: {str(e)[:100]}... "
                f"Raw Payload Start: {str(raw_payload)[:100]}"
            )
            continue

    return aggregated_results


def unwrap_envelope(data: Any) -> Any:
    """Unwrap ToolResponseEnvelope if present.

    Args:
        data: The parsed tool output (dict, list, or primitive).

    Returns:
        The inner 'result' if data is an envelope, otherwise data itself.
    """
    if isinstance(data, dict) and "result" in data and "schema_version" in data:
        # Basic version check
        from common.models.tool_envelopes import CURRENT_SCHEMA_VERSION, is_compatible

        version = data.get("schema_version", "1.0")
        if not is_compatible(version, CURRENT_SCHEMA_VERSION):
            logger.warning(
                f"Tool envelope version mismatch: received {version}, "
                f"expected compatible with {CURRENT_SCHEMA_VERSION}"
            )
            # For now, we still return the result, but we logged the warning.
            # In strict mode, we might want to raise an error or return raw data.

        return data["result"]
    return data
