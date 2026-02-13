"""Structured audit stream for security-sensitive events."""

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

_MAX_METADATA_KEYS = 20
_MAX_METADATA_KEY_LEN = 64
_MAX_METADATA_VALUE_LEN = 256
_AUDIT_EVENT_JSON_LIMIT = 512
_SQL_TEXT_PATTERN = re.compile(
    r"\b(select|insert|update|delete|drop|alter|create|truncate|merge|grant|revoke)\b",
    flags=re.IGNORECASE,
)
_BLOCKED_METADATA_KEY_FRAGMENTS = {
    "sql",
    "query_result",
    "result_set",
    "row_data",
    "rows",
    "payload",
    "prompt",
    "tool_input",
    "tool_output",
}
_DROP_VALUE = object()


class AuditEventType(str, Enum):
    """Canonical audit event types for security-sensitive actions."""

    POLICY_REJECTION = "policy_rejection"
    READONLY_VIOLATION = "readonly_violation"
    SQL_COMPLEXITY_REJECTION = "sql_complexity_rejection"
    TENANT_CONCURRENCY_BLOCK = "tenant_concurrency_block"
    KILL_SWITCH_OVERRIDE = "kill_switch_override"
    REPLAY_MODE_ACTIVATED = "replay_mode_activated"
    REPLAY_MISMATCH = "replay_mismatch"
    BUDGET_EXCEEDED = "budget_exceeded"


class AuditEvent(BaseModel):
    """Structured audit record with bounded safe metadata only."""

    model_config = ConfigDict(extra="forbid")

    event_type: str
    tenant_id: Optional[int] = None
    run_id: Optional[str] = None
    timestamp: float
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
    return sanitized


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

    def list_recent(self, *, limit: Optional[int] = None) -> list[dict[str, Any]]:
        """Return newest-first events with optional limit."""
        max_items = None if limit is None else max(0, int(limit))
        with self._lock:
            events = list(self._items)
        if max_items is not None:
            events = events[-max_items:]
        events.reverse()
        return [json.loads(event.model_dump_json()) for event in events]


_AUDIT_EVENT_BUFFER: Optional[AuditEventBuffer] = None


def get_audit_event_buffer() -> AuditEventBuffer:
    """Return singleton audit event buffer."""
    global _AUDIT_EVENT_BUFFER
    if _AUDIT_EVENT_BUFFER is None:
        _AUDIT_EVENT_BUFFER = AuditEventBuffer(
            max_size=_safe_env_int("AGENT_AUDIT_BUFFER_SIZE", 200, minimum=1)
        )
    return _AUDIT_EVENT_BUFFER


def reset_audit_event_buffer() -> None:
    """Reset singleton audit event buffer (test helper)."""
    global _AUDIT_EVENT_BUFFER
    _AUDIT_EVENT_BUFFER = None


def emit_audit_event(
    event_type: AuditEventType | str,
    *,
    tenant_id: Optional[int] = None,
    run_id: Optional[str] = None,
    error_category: Any = None,
    metadata: Optional[dict[str, Any]] = None,
) -> AuditEvent:
    """Emit a structured audit event to span, counter, and bounded buffer."""
    normalized_type = str(
        event_type.value if isinstance(event_type, AuditEventType) else event_type
    )
    normalized_category = _normalize_error_category(error_category)
    safe_metadata = sanitize_audit_metadata(metadata)
    event = AuditEvent(
        event_type=normalized_type,
        tenant_id=int(tenant_id) if tenant_id is not None else None,
        run_id=str(run_id) if run_id else None,
        timestamp=float(time.time()),
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
            "error_category": normalized_category or "none",
        },
        description="Structured security-sensitive audit events",
    )
    return event
