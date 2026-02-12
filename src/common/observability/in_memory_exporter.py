"""Shared in-memory OTEL span exporter utilities for deterministic tests."""

from __future__ import annotations

import threading
from collections.abc import Sequence

from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_lock = threading.Lock()
_span_exporters: dict[str, InMemorySpanExporter] = {}


def get_or_create_span_exporter(scope: str) -> InMemorySpanExporter:
    """Return an in-memory exporter for a logical scope (e.g., agent, mcp)."""
    normalized_scope = (scope or "default").strip().lower()
    if not normalized_scope:
        normalized_scope = "default"
    with _lock:
        exporter = _span_exporters.get(normalized_scope)
        if exporter is None:
            exporter = InMemorySpanExporter()
            _span_exporters[normalized_scope] = exporter
        return exporter


def get_finished_spans(scope: str) -> Sequence:
    """Fetch finished spans captured for a scope."""
    exporter = get_or_create_span_exporter(scope)
    return exporter.get_finished_spans()


def clear_span_exporter(scope: str) -> None:
    """Clear captured spans for a scope without deleting exporter state."""
    exporter = get_or_create_span_exporter(scope)
    exporter.clear()


def reset_span_exporters() -> None:
    """Clear exporter registry and all captured spans."""
    with _lock:
        _span_exporters.clear()
