from agent.telemetry_schema import MAX_PAYLOAD_SIZE, redact_secrets, truncate_json


class TestTelemetrySchema:
    """Test suite for telemetry schema utilities."""

    def test_redact_secrets(self):
        """Test recursive redaction of sensitive keys."""
        input_data = {
            "public": "value",
            "api_key": "secret123",
            "nested": {
                "Auth_Token": "secret456",
                "safe": "value",
                "deep": {"password": "pass"},
            },
            "list": [{"credential": "bad", "ok": "good"}],
        }

        redacted = redact_secrets(input_data)

        assert redacted["public"] == "value"
        assert redacted["api_key"] == "[REDACTED]"
        assert redacted["nested"]["Auth_Token"] == "[REDACTED]"
        assert redacted["nested"]["safe"] == "value"
        assert redacted["nested"]["deep"]["password"] == "[REDACTED]"
        assert redacted["list"][0]["credential"] == "[REDACTED]"
        assert redacted["list"][0]["ok"] == "good"

    def test_truncate_json_small(self):
        """Test small payload is not truncated."""
        obj = {"key": "value"}
        json_str, truncated, size, sha = truncate_json(obj)

        assert json_str == '{"key": "value"}'
        assert not truncated
        assert size == len(json_str)
        assert sha is not None

    def test_truncate_json_large(self):
        """Test large payload is truncated."""
        # Create a payload larger than MAX_PAYLOAD_SIZE (32KB)
        big_str = "x" * (MAX_PAYLOAD_SIZE + 100)
        obj = {"big": big_str}

        json_str, truncated, size, sha = truncate_json(obj)

        assert truncated
        assert len(json_str) < len(big_str)
        assert json_str.endswith("... [TRUNCATED]")
        assert size == len('{"big": "' + big_str + '"}')  # Original size reported

    def test_deterministic_serialization(self):
        """Test that JSON keys are sorted."""
        obj = {"b": 2, "a": 1}
        json_str, _, _, _ = truncate_json(obj)
        assert json_str == '{"a": 1, "b": 2}'


class TestTelemetryContract:
    """Test that span contract attributes are auto-set."""

    def test_span_auto_sets_event_type_and_name(self):
        """Verify start_span auto-sets event.type and event.name."""
        from agent.telemetry import InMemoryTelemetryBackend, telemetry
        from agent.telemetry_schema import SpanKind

        backend = InMemoryTelemetryBackend()
        telemetry.set_backend(backend)

        with telemetry.start_span(name="test.span", span_type=SpanKind.TOOL_CALL):
            pass

        span = backend.spans[0]
        assert span.attributes["event.type"] == SpanKind.TOOL_CALL
        assert span.attributes["event.name"] == "test.span"
        assert "event.seq" in span.attributes

    def test_span_does_not_override_explicit_event_type(self):
        """Verify explicit attributes are not overridden."""
        from agent.telemetry import InMemoryTelemetryBackend, telemetry
        from agent.telemetry_schema import SpanKind

        backend = InMemoryTelemetryBackend()
        telemetry.set_backend(backend)

        with telemetry.start_span(
            name="test.span",
            span_type=SpanKind.AGENT_NODE,
            attributes={"event.type": "custom.type", "event.name": "custom.name"},
        ):
            pass

        span = backend.spans[0]
        assert span.attributes["event.type"] == "custom.type"
        assert span.attributes["event.name"] == "custom.name"

    def test_all_spans_have_contract_attributes(self):
        """Verify nested spans all have required contract attributes."""
        from agent.telemetry import InMemoryTelemetryBackend, telemetry
        from agent.telemetry_schema import SpanKind

        backend = InMemoryTelemetryBackend()
        telemetry.set_backend(backend)

        with telemetry.start_span(name="parent", span_type=SpanKind.AGENT_NODE):
            with telemetry.start_span(name="child1", span_type=SpanKind.TOOL_CALL):
                pass
            with telemetry.start_span(name="child2", span_type=SpanKind.LLM_CALL):
                pass

        # All 3 spans should have contract attributes
        for span in backend.spans:
            assert "event.type" in span.attributes, f"Span {span.name} missing event.type"
            assert "event.name" in span.attributes, f"Span {span.name} missing event.name"
            assert "event.seq" in span.attributes, f"Span {span.name} missing event.seq"


class TestPayloadMetadata:
    """Test that payload size and hash metadata are emitted."""

    def test_set_inputs_emits_metadata(self):
        """Verify set_inputs emits size and hash."""
        from unittest.mock import MagicMock

        from agent.telemetry import OTELTelemetrySpan

        mock_span = MagicMock()
        otel_span = OTELTelemetrySpan(mock_span)

        otel_span.set_inputs({"key": "value"})

        # Check that set_attribute was called with metadata keys
        calls = {call[0][0]: call[0][1] for call in mock_span.set_attribute.call_args_list}

        assert "telemetry.inputs_json" in calls
        assert "telemetry.payload_size_bytes" in calls
        assert "telemetry.payload_sha256" in calls
        assert calls["telemetry.payload_size_bytes"] > 0
        assert len(calls["telemetry.payload_sha256"]) == 64  # SHA256 hex

    def test_set_outputs_emits_error_json(self):
        """Verify set_outputs with error emits structured JSON."""
        from unittest.mock import MagicMock

        from agent.telemetry import OTELTelemetrySpan

        mock_span = MagicMock()
        otel_span = OTELTelemetrySpan(mock_span)

        otel_span.set_outputs({"error": "test_error"})

        calls = {call[0][0]: call[0][1] for call in mock_span.set_attribute.call_args_list}

        assert "telemetry.error_json" in calls
        import json

        error = json.loads(calls["telemetry.error_json"])
        assert error["error"] == "test_error"
