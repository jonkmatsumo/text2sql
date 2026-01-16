from agent_core.telemetry_schema import MAX_PAYLOAD_SIZE, redact_secrets, truncate_json


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
