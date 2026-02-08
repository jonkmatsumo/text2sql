"""Telemetry Schema and Parity Contract.

This module defines the immutable contract for telemetry attributes,
ensuring parity with MLflow traces while using OTEL as the backend.
It includes utilities for payload safety (redaction/truncation).
"""

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Tuple

from common.sanitization.bounding import redact_recursive

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

# Keys that are known to contain large text or high-cardinality data
HIGH_CARDINALITY_KEYS = {
    "schema_context",
    "raw_schema_context",
    "procedural_plan",
    "query_result",
    "last_tool_output",
    "correction_strategy",
    "procedural_plan",
    "explanation",
    "plan",
}

# Keys that contain SQL or other sensitive text that should be hashed + summarized
SENSITIVE_TEXT_KEYS = {
    "current_sql",
    "rewritten_sql",
    "sql",
    "original_sql",
    "failed_sql",
    "bad_query",
    "corrected_sql",
}


def bound_attribute(key: str, value: Any) -> Any:
    """Apply cardinality and safety guardrails to a single attribute."""
    if value is None:
        return None

    # Handle Sensitive Text (SQL, etc.)
    if key in SENSITIVE_TEXT_KEYS and isinstance(value, str):
        val_hash = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
        summary = value[:500] + "..." if len(value) > 500 else value
        return f"hash:{val_hash} | {summary}"

    # Handle High Cardinality / Large Objects
    if key in HIGH_CARDINALITY_KEYS:
        from common.sanitization.bounding import bound_payload

        # Bound collections to a safe size
        bounded = bound_payload(value, max_items=20)
        # Serialize to string if it's still a complex object for OTEL compatibility
        if isinstance(bounded, (dict, list)):
            json_str, _, _, _ = truncate_json(bounded, max_len=2048)
            return json_str
        return bounded

    # General Bounding for all strings
    if isinstance(value, str) and len(value) > 2048:
        return value[:2045] + "..."

    return value


def truncate_json(
    obj: Any, max_len: int = MAX_PAYLOAD_SIZE
) -> Tuple[str, bool, int, Optional[str]]:
    """
    Serialize and truncate JSON payload.

    Returns:
        Tuple(serialized_str, was_truncated, size_bytes, sha256_hash)
    """
    # 1. Redact first
    clean_obj = redact_recursive(obj)

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


# --- Span Contract Enforcement ---


@dataclass(frozen=True)
class SpanContract:
    """Contract defining required and optional attributes for a span type.

    Required attributes must be set before the span ends; violations are logged.
    Optional attributes are recommended but not enforced.
    """

    name: str  # Contract name (e.g., "execute_sql")
    required: frozenset[str]  # Must be set before span ends
    optional: frozenset[str] = frozenset()  # Recommended but not enforced
    required_on_error: frozenset[str] = frozenset()  # Required only if error occurred

    def validate(self, attributes: dict, has_error: bool = False) -> list[str]:
        """Validate attributes against the contract.

        Returns:
            List of missing required attribute names.
        """
        missing = []
        for attr in self.required:
            if attr not in attributes:
                missing.append(attr)
        if has_error:
            for attr in self.required_on_error:
                if attr not in attributes:
                    missing.append(attr)
        return missing


# Predefined contracts for agent nodes
SPAN_CONTRACTS: dict[str, SpanContract] = {
    "execute_sql": SpanContract(
        name="execute_sql",
        required=frozenset({"result.is_truncated", "result.rows_returned"}),
        optional=frozenset({"result.row_limit", "result.partial_reason"}),
        required_on_error=frozenset({"error.category"}),
    ),
    "validate_sql": SpanContract(
        name="validate_sql",
        required=frozenset({"validation.is_valid"}),
        optional=frozenset({"validation.violation_count", "validation.schema_bound_enabled"}),
    ),
    "cache_lookup": SpanContract(
        name="cache_lookup",
        required=frozenset({"cache.hit"}),
        optional=frozenset({"cache.snapshot_mismatch", "cache.cache_id"}),
        required_on_error=frozenset({"error.category"}),
    ),
    "generate_sql": SpanContract(
        name="generate_sql",
        required=frozenset({"from_cache"}),
        optional=frozenset({"generation.model", "generation.latency_seconds"}),
        required_on_error=frozenset({"error.category"}),
    ),
    "synthesize_insight": SpanContract(
        name="synthesize_insight",
        required=frozenset({"result.is_truncated", "result.rows_returned"}),
        optional=frozenset({"result.is_limited"}),
        required_on_error=frozenset({"error.category"}),
    ),
    "retrieve_context": SpanContract(
        name="retrieve_context",
        required=frozenset({"event.type"}),
        optional=frozenset({"retrieval.node_count", "retrieval.table_count"}),
        required_on_error=frozenset({"error.category"}),
    ),
}


def get_span_contract(span_name: str) -> Optional[SpanContract]:
    """Get the contract for a span name, if one exists."""
    return SPAN_CONTRACTS.get(span_name)
