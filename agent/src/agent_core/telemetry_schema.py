"""Telemetry Schema and Parity Contract.

This module defines the immutable contract for telemetry attributes,
ensuring parity with MLflow traces while using OTEL as the backend.
It includes utilities for payload safety (redaction/truncation).
"""

import hashlib
import json
from enum import Enum
from typing import Any, Optional, Tuple

# --- Constants & Keys ---


class SpanKind(str, Enum):
    """Semantic span kinds for the agent parity protocol."""

    AGENT_NODE = "agent.node"
    TOOL_CALL = "tool.call"
    LLM_CALL = "llm.call"
    CHAIN = "chain"  # Legacy/Generic


class TelemetryKeys(str, Enum):
    """Standardized telemetry attribute keys."""

    # Event Identification
    EVENT_TYPE = "event.type"  # e.g., "tool.call"
    EVENT_NAME = "event.name"  # e.g., "fetch_schema"
    EVENT_SEQ = "event.seq"  # Monotonic sequence number within parent

    # Common Payload
    INPUTS = "telemetry.inputs_json"
    OUTPUTS = "telemetry.outputs_json"
    ERROR = "telemetry.error_json"

    # LLM Specific
    LLM_MODEL = "llm.model"
    LLM_PROMPT_SYSTEM = "llm.prompt.system"
    LLM_PROMPT_USER = "llm.prompt.user"
    LLM_RESPONSE_TEXT = "llm.response.text"
    LLM_TOKEN_INPUT = "llm.token_usage.input_tokens"
    LLM_TOKEN_OUTPUT = "llm.token_usage.output_tokens"
    LLM_TOKEN_TOTAL = "llm.token_usage.total_tokens"

    # Tool Specific
    TOOL_NAME = "tool.name"

    # Safety Meta
    PAYLOAD_TRUNCATED = "telemetry.payload_truncated"
    PAYLOAD_SIZE = "telemetry.payload_size_bytes"
    PAYLOAD_HASH = "telemetry.payload_sha256"


# --- Safety Utilities ---

MAX_PAYLOAD_SIZE = 32 * 1024  # 32KB hard limit for attributes
SENSITIVE_KEYS = {"api_key", "password", "secret", "token", "credential", "auth"}


def redact_secrets(obj: Any) -> Any:
    """Recursively redact sensitive keys in dictionaries."""
    if isinstance(obj, dict):
        new_obj = {}
        for k, v in obj.items():
            if any(s in k.lower() for s in SENSITIVE_KEYS):
                new_obj[k] = "[REDACTED]"
            else:
                new_obj[k] = redact_secrets(v)
        return new_obj
    elif isinstance(obj, list):
        return [redact_secrets(item) for item in obj]
    return obj


def truncate_json(
    obj: Any, max_len: int = MAX_PAYLOAD_SIZE
) -> Tuple[str, bool, int, Optional[str]]:
    """
    Serialize and truncate JSON payload.

    Returns:
        Tuple(serialized_str, was_truncated, size_bytes, sha256_hash)
    """
    # 1. Redact first
    clean_obj = redact_secrets(obj)

    # 2. Serialize deterministically
    try:
        json_str = json.dumps(clean_obj, sort_keys=True, default=str)
    except TypeError:
        # Fallback for non-serializable objects
        json_str = str(clean_obj)

    size = len(json_str.encode("utf-8"))
    sha256 = hashlib.sha256(json_str.encode("utf-8")).hexdigest()

    if size > max_len:
        # Truncate
        truncated = json_str[:max_len] + "... [TRUNCATED]"
        return truncated, True, size, sha256

    return json_str, False, size, sha256
