import msgpack

from agent.telemetry import TelemetryContext, telemetry


class TestTelemetrySerialization:
    """Test suite for telemetry context serialization logic."""

    def test_serialize_context_is_msgpack_safe(self):
        """Verify that serialize_context produces msgpack-safe dict."""
        # Setup context
        with telemetry.start_span("test_span") as span:
            span.set_attribute("foo", "bar")
            telemetry.update_current_trace({"sticky_key": "sticky_value"})

            ctx = telemetry.capture_context()

            # Serialize
            serialized = telemetry.serialize_context(ctx)

            # Verify structure
            assert isinstance(serialized, dict)
            assert "_sticky_metadata" in serialized
            assert serialized["_sticky_metadata"]["sticky_key"] == "sticky_value"

            # Verify MsgPack safety
            packed = msgpack.packb(serialized)
            unpacked = msgpack.unpackb(packed)

            assert unpacked["_sticky_metadata"]["sticky_key"] == "sticky_value"

    def test_deserialize_context_restores_state(self):
        """Verify that deserialize_context restores context correctly."""
        # Create a serialized context manually or via helper
        serialized = {
            "traceparent": "00-80e1afed08e019fc1110464cfa66635c-7a085853722dc6d2-01",
            "_sticky_metadata": {"user_id": "123"},
        }

        ctx = telemetry.deserialize_context(serialized)

        assert isinstance(ctx, TelemetryContext)
        assert ctx.sticky_metadata["user_id"] == "123"

        # Verify usage
        with telemetry.use_context(ctx):
            # Capture again to verify restoration
            new_ctx = telemetry.capture_context()
            assert new_ctx.sticky_metadata["user_id"] == "123"
