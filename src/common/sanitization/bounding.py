"""Recursive redaction and payload bounding utilities."""

from typing import Any

from common.constants.reason_codes import PayloadTruncationReason
from common.sanitization.text import redact_sensitive_info


def redact_recursive(value: Any) -> Any:
    """Recursively redact sensitive info from nested structures."""
    if isinstance(value, str):
        return redact_sensitive_info(value)
    if isinstance(value, list):
        return [redact_recursive(item) for item in value]
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        sensitive_tokens = {"token", "password", "secret", "api_key", "auth", "credential"}
        for key, item in value.items():
            lowered_key = key.lower()
            # Allowlist: operational keys that should NOT be redacted even if they contain 'token'
            if "token_usage" in lowered_key or "page_token" in lowered_key:
                redacted[key] = redact_recursive(item)
                continue

            match = any(token in lowered_key for token in sensitive_tokens)
            if match:
                redacted[key] = "<redacted>"
            else:
                redacted[key] = redact_recursive(item)
        return redacted
    return value


def bound_payload(
    payload: Any,
    max_items: int = 50,
    truncation_reason: PayloadTruncationReason = PayloadTruncationReason.SAFETY_LIMIT,
) -> Any:
    """Bound list/dict payloads to prevent OOM/telemetry explosions.

    If truncated, adds metadata if possible (dict) or truncates (list).
    Default max_items matches old replay bundle default (MAX_TOOL_ROWS=50).
    """
    if isinstance(payload, list):
        if len(payload) > max_items:
            # We can't attach metadata to a list, so we just truncate.
            return payload[:max_items]
        return payload

    if isinstance(payload, dict):
        # Handle "rows" key specifically for DB result sets
        if "rows" in payload and isinstance(payload["rows"], list):
            rows = payload["rows"]
            if len(rows) > max_items:
                payload = payload.copy()
                payload["rows"] = rows[:max_items]
                if "metadata" not in payload:
                    payload["metadata"] = {}
                # Ensure metadata is a dict
                if not isinstance(payload["metadata"], dict):
                    payload["metadata"] = {}

                payload["metadata"]["is_truncated"] = True
                payload["metadata"]["partial_reason"] = truncation_reason.value
        return payload

    return payload
