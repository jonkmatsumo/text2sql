"""Structured audit stream for security- and safety-relevant events."""

from __future__ import annotations

import json
import math
import re
import threading
import time
from collections import deque
from enum import Enum
from typing import Any, Optional

from opentelemetry import trace
from pydantic import BaseModel, ConfigDict, Field

from common.config.env import get_env_int
from common.models.error_metadata import ErrorCategory
from common.observability.metrics import agent_metrics
from common.sanitization.text import redact_sensitive_info

_MAX_METADATA_KEYS = 32
_MAX_METADATA_KEY_LEN = 64
_MAX_METADATA_VALUE_LEN = 512
_DEFAULT_METADATA_JSON_LIMIT_BYTES = 3072
_AUDIT_EVENT_JSON_LIMIT = 1024
_SQL_TEXT_PATTERN = re.compile(
    r"\b(select|insert|update|delete|drop|alter|create|truncate|merge|grant|revoke)\b",
    flags=re.IGNORECASE,
)
_BLOCKED_METADATA_KEY_FRAGMENTS = {
    "sql",
    "query_result",
    "result_set",
    "row_data",
    "payload",
    "prompt",
    "tool_input",
    "tool_output",
}
_DROP_VALUE = object()
_ALLOWED_SOURCES = {"agent", "mcp", "dal"}


class AuditEventSource(str, Enum):
    """Known emitters for structured audit events."""

    AGENT = "agent"
    MCP = "mcp"
    DAL = "dal"


class AuditEventType(str, Enum):
    """Canonical audit event types for security-sensitive actions."""

    POLICY_REJECTION = "policy_rejection"
    READONLY_VIOLATION = "readonly_violation"
    SQL_COMPLEXITY_REJECTION = "sql_complexity_rejection"
    TENANT_CONCURRENCY_LIMIT_EXCEEDED = "tenant_concurrency_limit_exceeded"
    TENANT_RATE_LIMITED = "tenant_rate_limited"
    KILL_SWITCH_OVERRIDE = "kill_switch_override"
    REPLAY_MODE_ACTIVATED = "replay_mode_activated"
    REPLAY_MISMATCH = "replay_mismatch"
    RUN_BUDGET_EXCEEDED = "run_budget_exceeded"

    # Backward-compatible aliases.
    TENANT_CONCURRENCY_BLOCK = "tenant_concurrency_limit_exceeded"
    BUDGET_EXCEEDED = "run_budget_exceeded"


class AuditEvent(BaseModel):
    """Structured audit record with bounded safe metadata only."""

    model_config = ConfigDict(extra="forbid")

    event_type: str
    timestamp: float
    source: str
    tenant_id: Optional[int] = None
    run_id: Optional[str] = None
    error_category: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


def _safe_env_int(name: str, default: int, minimum: int) -> int:
    try:
        value = get_env_int(name, default)
    except ValueError:
        value = default
    if value is None:
        value = default
    return max(minimum, int(value))


def _resolve_buffer_size() -> int:
    try:
        value = get_env_int("OPS_AUDIT_EVENT_BUFFER_SIZE", None)
    except ValueError:
        value = None
    if value is None:
        return _safe_env_int("AGENT_AUDIT_BUFFER_SIZE", 200, minimum=1)
    return max(1, int(value))


def _resolve_metadata_json_limit() -> int:
    return _safe_env_int(
        "OPS_AUDIT_EVENT_METADATA_MAX_BYTES",
        _DEFAULT_METADATA_JSON_LIMIT_BYTES,
        minimum=256,
    )


def _normalize_source(value: Any) -> str:
    if isinstance(value, AuditEventSource):
        return value.value
    text = str(value or "").strip().lower()
    if text in _ALLOWED_SOURCES:
        return text
    return AuditEventSource.AGENT.value


def _normalize_error_category(value: Any) -> Optional[str]:
    if isinstance(value, ErrorCategory):
        return value.value
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    return text


def _is_blocked_metadata_key(key: str) -> bool:
    lowered = key.lower()
    return any(fragment in lowered for fragment in _BLOCKED_METADATA_KEY_FRAGMENTS)


def _sanitize_metadata_value(value: Any) -> Any:
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return _DROP_VALUE
        return float(value)
    if isinstance(value, ErrorCategory):
        return value.value
    if isinstance(value, Enum):
        return str(value.value)
    if value is None:
        return None
    if not isinstance(value, str):
        return _DROP_VALUE

    text = redact_sensitive_info(str(value)).strip()
    if not text:
        return ""
    if _SQL_TEXT_PATTERN.search(text):
        return "<redacted_sql>"
    if len(text) > _MAX_METADATA_VALUE_LEN:
        return text[:_MAX_METADATA_VALUE_LEN]
    return text


def _bound_metadata_size(metadata: dict[str, Any]) -> dict[str, Any]:
    if not metadata:
        return {}

    max_bytes = _resolve_metadata_json_limit()
    bounded = dict(metadata)
    truncated = False
    while bounded:
        encoded = json.dumps(bounded, sort_keys=True, separators=(",", ":")).encode("utf-8")
        if len(encoded) <= max_bytes:
            if truncated:
                bounded["metadata_truncated"] = True
            return bounded
        truncated = True
        bounded.pop(next(reversed(bounded)))
    return {"metadata_truncated": True} if truncated else {}


def sanitize_audit_metadata(metadata: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Bound and sanitize metadata to avoid SQL text or row payload leakage."""
    if not isinstance(metadata, dict):
        return {}

    sanitized: dict[str, Any] = {}
    for raw_key, raw_value in metadata.items():
        if len(sanitized) >= _MAX_METADATA_KEYS:
            break
        key = str(raw_key).strip().lower()
        if not key:
            continue
        key = key[:_MAX_METADATA_KEY_LEN]
        if _is_blocked_metadata_key(key):
            continue
        value = _sanitize_metadata_value(raw_value)
        if value is _DROP_VALUE:
            continue
        sanitized[key] = value
    return _bound_metadata_size(sanitized)


class AuditEventBuffer:
    """Thread-safe bounded FIFO audit buffer."""

    def __init__(self, *, max_size: int) -> None:
        """Initialize bounded in-memory retention for recent audit events."""
        self._max_size = max(1, int(max_size))
        self._items: deque[AuditEvent] = deque(maxlen=self._max_size)
        self._lock = threading.Lock()

    def record(self, event: AuditEvent) -> None:
        """Append one event to the bounded buffer."""
        with self._lock:
            self._items.append(event)

    def list_recent(
        self, *, limit: Optional[int] = None, run_id: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """Return newest-first events with optional limit and run filter."""
        max_items = None if limit is None else max(0, int(limit))
        run_filter = str(run_id) if run_id else None
        with self._lock:
            events = list(self._items)
        if run_filter:
            events = [event for event in events if str(event.run_id or "") == run_filter]
        if max_items is not None:
            events = events[-max_items:]
        events.reverse()
        return [json.loads(event.model_dump_json()) for event in events]


_AUDIT_EVENT_BUFFER: Optional[AuditEventBuffer] = None


def get_audit_event_buffer() -> AuditEventBuffer:
    """Return singleton audit event buffer."""
    global _AUDIT_EVENT_BUFFER
    if _AUDIT_EVENT_BUFFER is None:
        _AUDIT_EVENT_BUFFER = AuditEventBuffer(max_size=_resolve_buffer_size())
    return _AUDIT_EVENT_BUFFER


def reset_audit_event_buffer() -> None:
    """Reset singleton audit event buffer (test helper)."""
    global _AUDIT_EVENT_BUFFER
    _AUDIT_EVENT_BUFFER = None


def emit_audit_event(
    event_type: AuditEventType | str,
    *,
    source: AuditEventSource | str = AuditEventSource.AGENT,
    tenant_id: Optional[int] = None,
    run_id: Optional[str] = None,
    error_category: Any = None,
    metadata: Optional[dict[str, Any]] = None,
) -> AuditEvent:
    """Emit a structured audit event to span, counter, and bounded buffer."""
    normalized_type = str(
        event_type.value if isinstance(event_type, AuditEventType) else event_type
    )
    normalized_source = _normalize_source(source)
    normalized_category = _normalize_error_category(error_category)
    safe_metadata = sanitize_audit_metadata(metadata)
    event = AuditEvent(
        event_type=normalized_type,
        timestamp=float(time.time()),
        source=normalized_source,
        tenant_id=int(tenant_id) if tenant_id is not None else None,
        run_id=str(run_id) if run_id else None,
        error_category=normalized_category,
        metadata=safe_metadata,
    )
    get_audit_event_buffer().record(event)

    span = trace.get_current_span()
    if span is not None and span.is_recording():
        span.add_event(
            "agent.audit",
            {
                "event_type": normalized_type,
                "source": normalized_source,
                "tenant_id": int(tenant_id) if tenant_id is not None else -1,
                "run_id": str(run_id) if run_id else "",
                "error_category": normalized_category or "",
                "metadata_json": json.dumps(safe_metadata, sort_keys=True)[
                    :_AUDIT_EVENT_JSON_LIMIT
                ],
            },
        )

    agent_metrics.add_counter(
        "agent.audit.events_total",
        attributes={
            "event_type": normalized_type,
            "source": normalized_source,
            "error_category": normalized_category or "none",
        },
        description="Structured security-sensitive audit events",
    )
    return event
