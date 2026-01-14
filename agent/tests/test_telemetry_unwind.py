import contextlib

import pytest
from agent_core.telemetry import (
    DualTelemetryBackend,
    InMemoryTelemetryBackend,
    TelemetryContext,
    telemetry,
)


def test_telemetry_use_context_unwind():
    """Verify that exceptions inside use_context propagate correctly without RuntimeError."""
    # Ensure we use a DualTelemetryBackend to test the specific fix
    primary = InMemoryTelemetryBackend()
    secondary = InMemoryTelemetryBackend()
    dual = DualTelemetryBackend(primary, secondary)

    # Check if telemetry service already has a backend, otherwise use one
    ctx = TelemetryContext()

    # This should NOT raise RuntimeError: generator didn't stop after throw()
    with pytest.raises(ValueError, match="intentional error"):
        with dual.use_context(ctx):
            raise ValueError("intentional error")


def test_telemetry_service_use_context_unwind():
    """Verify TelemetryService.use_context unwinds correctly."""
    # This tests the public API
    ctx = TelemetryContext(sticky_metadata={"foo": "bar"})

    with pytest.raises(RuntimeError, match="nested error"):
        with telemetry.use_context(ctx):
            raise RuntimeError("nested error")


def test_secondary_backend_failure_does_not_mask_primary():
    """Verify that if the secondary backend fails, the primary still works."""

    class FailingBackend(InMemoryTelemetryBackend):
        @contextlib.contextmanager
        def use_context(self, ctx):
            raise RuntimeError("Secondary backend crash")
            yield  # Never reached

    primary = InMemoryTelemetryBackend()
    secondary = FailingBackend()
    dual = DualTelemetryBackend(primary, secondary)

    ctx = TelemetryContext()

    # Should work even if secondary crashes on enter
    with dual.use_context(ctx):
        print("User code running")

    # Should also propagate user error if user code crashes
    with pytest.raises(ValueError, match="user error"):
        with dual.use_context(ctx):
            raise ValueError("user error")
