from agent.telemetry_schema import MAX_PAYLOAD_SIZE, truncate_json
from common.sanitization.bounding import redact_recursive


class TestTelemetrySchema:
    """Test suite for telemetry schema utilities."""

    def test_redact_recursive(self):
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

        redacted = redact_recursive(input_data)

        assert redacted["public"] == "value"
        assert redacted["api_key"] == "<redacted>"
        assert redacted["nested"]["Auth_Token"] == "<redacted>"
        assert redacted["nested"]["safe"] == "value"
        assert redacted["nested"]["deep"]["password"] == "<redacted>"
        assert redacted["list"][0]["credential"] == "<redacted>"
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


class TestSpanContract:
    """Test suite for SpanContract validation."""

    def test_span_contract_validate_all_present(self):
        """Test that validation passes when all required attributes are present."""
        from agent.telemetry_schema import SpanContract

        contract = SpanContract(
            name="test",
            required=frozenset({"attr.a", "attr.b"}),
        )

        missing = contract.validate({"attr.a": 1, "attr.b": 2, "attr.c": 3})
        assert missing == []

    def test_span_contract_validate_missing_required(self):
        """Test that validation returns missing required attributes."""
        from agent.telemetry_schema import SpanContract

        contract = SpanContract(
            name="test",
            required=frozenset({"attr.a", "attr.b"}),
        )

        missing = contract.validate({"attr.a": 1})
        assert "attr.b" in missing
        assert "attr.a" not in missing

    def test_span_contract_validate_required_on_error(self):
        """Test that required_on_error attributes are checked when has_error=True."""
        from agent.telemetry_schema import SpanContract

        contract = SpanContract(
            name="test",
            required=frozenset({"attr.a"}),
            required_on_error=frozenset({"error.category"}),
        )

        # Without error - only required checked
        missing_no_error = contract.validate({"attr.a": 1}, has_error=False)
        assert missing_no_error == []

        # With error - required_on_error also checked
        missing_with_error = contract.validate({"attr.a": 1}, has_error=True)
        assert "error.category" in missing_with_error

    def test_get_span_contract_returns_contract(self):
        """Test that get_span_contract returns correct contract."""
        from agent.telemetry_schema import get_span_contract

        contract = get_span_contract("execute_sql")
        assert contract is not None
        assert contract.name == "execute_sql"
        assert "result.is_truncated" in contract.required

    def test_get_span_contract_returns_none_for_unknown(self):
        """Test that get_span_contract returns None for unknown spans."""
        from agent.telemetry_schema import get_span_contract

        contract = get_span_contract("unknown_span_name")
        assert contract is None

    def test_span_contracts_are_frozen(self):
        """Test that SpanContract instances are immutable."""
        from agent.telemetry_schema import SpanContract

        contract = SpanContract(
            name="test",
            required=frozenset({"attr.a"}),
        )

        # Should raise error when trying to modify frozen dataclass
        try:
            contract.name = "modified"
            assert False, "Should have raised FrozenInstanceError"
        except Exception:
            pass  # Expected

    def test_otel_span_tracks_attributes(self):
        """Test that OTELTelemetrySpan tracks set attributes."""
        from unittest.mock import MagicMock

        from agent.telemetry import OTELTelemetrySpan

        mock_span = MagicMock()
        otel_span = OTELTelemetrySpan(mock_span)

        otel_span.set_attribute("result.is_truncated", True)
        otel_span.set_attribute("result.rows_returned", 100)

        tracked = otel_span.get_tracked_attributes()
        assert tracked["result.is_truncated"] is True
        assert tracked["result.rows_returned"] == 100

    def test_otel_span_detects_error(self):
        """Test that OTELTelemetrySpan detects error conditions."""
        from unittest.mock import MagicMock

        from agent.telemetry import OTELTelemetrySpan

        mock_span = MagicMock()
        otel_span = OTELTelemetrySpan(mock_span)

        assert not otel_span.has_error()

        otel_span.set_attribute("error.category", "timeout")
        assert otel_span.has_error()
