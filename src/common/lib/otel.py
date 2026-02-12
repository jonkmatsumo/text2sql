"""OpenTelemetry helper wrappers used by runtime components."""

from __future__ import annotations

from opentelemetry import trace


def get_tracer(name: str):
    """Return an OpenTelemetry tracer for the provided module name."""
    return trace.get_tracer(name)
