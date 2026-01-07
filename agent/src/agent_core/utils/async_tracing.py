"""Asynchronous MLflow trace logging."""

import asyncio
from typing import Any, Dict

import mlflow


async def log_trace_async(trace_data: Dict[str, Any]):
    """
    Log trace data asynchronously to avoid blocking.

    Args:
        trace_data: Dictionary containing trace information
    """
    # Run MLflow logging in thread pool to avoid blocking
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None,
        _sync_log_trace,
        trace_data,
    )


def _sync_log_trace(trace_data: Dict[str, Any]):
    """Log trace synchronously (runs in executor)."""
    # MLflow 3.x uses start_span instead of start_trace
    with mlflow.start_span(
        name=trace_data.get("name", "agent_trace"),
        span_type="AGENT",
    ) as trace:
        trace.set_inputs(trace_data.get("inputs", {}))
        trace.set_outputs(trace_data.get("outputs", {}))

        for key, value in trace_data.get("tags", {}).items():
            trace.set_tag(key, str(value))
