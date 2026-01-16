from unittest.mock import AsyncMock, MagicMock

import pytest
from agent_core.telemetry import InMemoryTelemetryBackend, telemetry
from agent_core.telemetry_schema import SpanKind, TelemetryKeys
from agent_core.tools import _wrap_tool


class TestToolTelemetry:
    """Test suite for tool telemetry wrapping."""

    @pytest.fixture(autouse=True)
    def setup_telemetry(self):
        """Set up in-memory telemetry for testing."""
        self.backend = InMemoryTelemetryBackend()
        telemetry.set_backend(self.backend)
        yield
        # Reset is not strictly needed as fixtures are fresh per test class instance usually,
        # but good practice if singletons persist.

    @pytest.mark.asyncio
    async def test_tool_wrapper_success(self):
        """Test tool wrapper captures inputs and outputs on success."""
        # Mock a LangChain tool
        mock_tool = MagicMock()
        mock_tool.name = "test_tool"
        mock_tool._arun = AsyncMock(return_value="tool_result")

        # Wrap it
        wrapped_tool = _wrap_tool(mock_tool)

        # Execute
        result = await wrapped_tool._arun(arg1="value1")

        assert result == "tool_result"

        # Verify Span
        assert len(self.backend.spans) == 1
        span = self.backend.spans[0]

        assert span.name == "tool.test_tool"
        assert span.attributes[TelemetryKeys.EVENT_TYPE] == SpanKind.TOOL_CALL
        assert span.attributes[TelemetryKeys.TOOL_NAME] == "test_tool"

        # Inputs/Outputs
        assert "value1" in span.attributes[TelemetryKeys.INPUTS]
        assert "tool_result" in span.attributes[TelemetryKeys.OUTPUTS]

    @pytest.mark.asyncio
    async def test_tool_wrapper_error(self):
        """Test tool wrapper captures errors."""
        mock_tool = MagicMock()
        mock_tool.name = "fail_tool"
        mock_tool._arun = AsyncMock(side_effect=ValueError("tool failed"))

        wrapped_tool = _wrap_tool(mock_tool)

        with pytest.raises(ValueError):
            await wrapped_tool._arun(x=1)

        assert len(self.backend.spans) == 1
        span = self.backend.spans[0]

        assert span.name == "tool.fail_tool"
        error_json = span.attributes.get(TelemetryKeys.ERROR)
        assert "tool failed" in error_json
        assert "ValueError" in error_json

    @pytest.mark.asyncio
    async def test_tool_wrapper_truncation(self):
        """Test large inputs are flagged as truncated."""
        mock_tool = MagicMock()
        mock_tool.name = "big_tool"
        mock_tool._arun = AsyncMock(return_value="ok")

        wrapped_tool = _wrap_tool(mock_tool)

        # Huge input
        huge_string = "a" * 40000
        await wrapped_tool._arun(data=huge_string)

        span = self.backend.spans[0]
        assert span.attributes.get(TelemetryKeys.PAYLOAD_TRUNCATED) is True
