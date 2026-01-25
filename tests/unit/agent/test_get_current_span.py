from agent.telemetry import InMemoryTelemetryBackend, SpanType, TelemetryService, telemetry


def test_get_current_span_api_exists():
    """Verify get_current_span exists on TelemetryService."""
    assert hasattr(telemetry, "get_current_span")
    result = telemetry.get_current_span()
    # It might be None if no span is active
    assert result is None or hasattr(result, "add_event")


def test_get_current_span_in_memory():
    """Verify get_current_span logic with InMemoryBackend."""
    backend = InMemoryTelemetryBackend()
    svc = TelemetryService(backend=backend)

    # 1. No span
    assert svc.get_current_span() is None

    # 2. Start span
    with svc.start_span("test_span", SpanType.TOOL) as span:
        current = svc.get_current_span()
        assert current is not None
        assert current == span
        assert current.name == "test_span"

    # 3. After span (assuming simple nested logic for InMemory)
    # InMemoryBackend.get_current_span returns last unfinished.
    # After context manager, span is marked finished.
    # Note: InMemoryTelemetrySpan sets is_finished=True in finally block.
    assert svc.get_current_span() is None
