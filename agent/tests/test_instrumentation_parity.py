from unittest.mock import AsyncMock, MagicMock

import pytest
from agent_core.llm_client import _wrap_llm
from agent_core.telemetry import InMemoryTelemetryBackend, telemetry
from agent_core.telemetry_schema import SpanKind, TelemetryKeys
from agent_core.tools import _wrap_tool
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage


class TestInstrumentationParity:
    """
    Phase 5: Parity Validation Test Suite.

    Verifies:
    1. Deterministic sequencing (event.seq)
    2. Correct nesting (Parent -> Child)
    3. Presence of all required attributes for Tools and LLMs
    """

    @pytest.fixture(autouse=True)
    def setup_telemetry(self):
        """Set up in-memory backend for telemetry capture."""
        self.backend = InMemoryTelemetryBackend()
        telemetry.set_backend(self.backend)
        yield

    @pytest.mark.asyncio
    async def test_sequence_monotonicity(self):
        """Verify event.seq increases monotonically per parent."""
        # 1. Create a parent span (Node)
        with telemetry.start_span(name="node.test", span_type=SpanKind.AGENT_NODE) as parent:
            parent.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.AGENT_NODE)

            # 2. Simulate 3 children (Tool, LLM, Tool)

            # Child 1: Manual span for simplicity
            with telemetry.start_span(name="child1"):
                pass

            # Child 2: Another manual span
            with telemetry.start_span(name="child2"):
                pass

            # Child 3: Nested span (should have its own sequence context)
            with telemetry.start_span(name="child3"):
                with telemetry.start_span(name="grandchild1"):
                    pass

        # Verify Spans
        spans = self.backend.spans
        # Order in list is execution order

        child1 = spans[1]
        child2 = spans[2]
        child3 = spans[3]
        grandchild1 = spans[4]

        # Check Node Sequence (should be 0 if it's the root of this interaction)
        # But we want to check Children sequences relative to Node

        assert child1.attributes["event.seq"] == 0
        assert child2.attributes["event.seq"] == 1
        assert child3.attributes["event.seq"] == 2

        # Check Grandchild sequence (should start at 0 relative to child3)
        assert grandchild1.attributes["event.seq"] == 0

    @pytest.mark.asyncio
    async def test_llm_wrapper_parity(self):
        """Verify LLM wrapper emits correct attributes and sequence."""
        mock_llm = MagicMock(spec=BaseChatModel)
        mock_llm.model_name = "gpt-test"
        mock_llm.invoke = MagicMock(return_value=AsyncMock(content="response text"))
        # Mock response metadata for token usage
        mock_response = MagicMock()
        mock_response.content = "response text"
        mock_response.response_metadata = {
            "token_usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}
        }
        mock_llm.invoke.return_value = mock_response

        wrapped_llm = _wrap_llm(mock_llm)

        # Run under a parent node
        with telemetry.start_span(name="node.plan", span_type=SpanKind.AGENT_NODE):
            # Invoke LLM
            wrapped_llm.invoke(
                [SystemMessage(content="system prompt"), HumanMessage(content="user prompt")]
            )

        # Verify
        llm_span = [
            s
            for s in self.backend.spans
            if s.attributes.get(TelemetryKeys.EVENT_TYPE) == SpanKind.LLM_CALL
        ][0]

        # Attributes
        attrs = llm_span.attributes
        assert attrs[TelemetryKeys.LLM_MODEL] == "gpt-test"
        assert attrs[TelemetryKeys.LLM_PROMPT_SYSTEM] == '"system prompt"'  # JSON encoded
        assert attrs[TelemetryKeys.LLM_PROMPT_USER] == '"user prompt"'
        assert attrs[TelemetryKeys.LLM_RESPONSE_TEXT] == '"response text"'

        # Token Usage
        assert attrs[TelemetryKeys.LLM_TOKEN_INPUT] == 10
        assert attrs[TelemetryKeys.LLM_TOKEN_OUTPUT] == 20
        assert attrs[TelemetryKeys.LLM_TOKEN_TOTAL] == 30

        # Sequencing
        assert "event.seq" in attrs
        assert attrs["event.seq"] == 0  # First child of node

    @pytest.mark.asyncio
    async def test_mixed_sequence_tool_and_llm(self):
        """Verify sequencing with mixed Tool and LLM calls."""
        # Mock Tool
        mock_tool = MagicMock()
        mock_tool.name = "my_tool"
        mock_tool._arun = AsyncMock(return_value="tool_result")
        wrapped_tool = _wrap_tool(mock_tool)

        # Mock LLM
        mock_llm = MagicMock(spec=BaseChatModel)
        mock_llm.model_name = "gpt-test"
        mock_response = MagicMock()
        mock_response.content = "llm_result"
        mock_llm.ainvoke = AsyncMock(return_value=mock_response)
        wrapped_llm = _wrap_llm(mock_llm)

        # Execution Flow: Tool -> LLM -> Tool
        with telemetry.start_span(name="node.exec", span_type=SpanKind.AGENT_NODE):
            await wrapped_tool._arun("input1")
            await wrapped_llm.ainvoke("prompt")
            await wrapped_tool._arun("input2")

        # Verify
        spans = self.backend.spans
        # Filter for children
        children = [s for s in spans if s.name != "node.exec"]

        assert len(children) == 3

        # Check types
        assert children[0].attributes[TelemetryKeys.EVENT_TYPE] == SpanKind.TOOL_CALL
        assert children[1].attributes[TelemetryKeys.EVENT_TYPE] == SpanKind.LLM_CALL
        assert children[2].attributes[TelemetryKeys.EVENT_TYPE] == SpanKind.TOOL_CALL

        # Check Sequence
        assert children[0].attributes["event.seq"] == 0
        assert children[1].attributes["event.seq"] == 1
        assert children[2].attributes["event.seq"] == 2

    @pytest.mark.asyncio
    async def test_deep_nesting_execution_order(self):
        """Verify execution order is reconstructable from nested spans.

        Tests pattern: 1 → 2 → 2a → 2b → 3
        """
        with telemetry.start_span(name="root", span_type=SpanKind.AGENT_NODE):
            with telemetry.start_span(name="step1"):
                pass
            with telemetry.start_span(name="step2"):
                with telemetry.start_span(name="step2a"):
                    pass
                with telemetry.start_span(name="step2b"):
                    pass
            with telemetry.start_span(name="step3"):
                pass

        spans = self.backend.spans

        # Verify names in execution order
        names = [s.name for s in spans]
        assert names == ["root", "step1", "step2", "step2a", "step2b", "step3"]

        # Verify sibling sequences
        step1 = spans[1]
        step2 = spans[2]
        step3 = spans[5]
        assert step1.attributes["event.seq"] == 0
        assert step2.attributes["event.seq"] == 1
        assert step3.attributes["event.seq"] == 2

        # Verify nested sequences reset
        step2a = spans[3]
        step2b = spans[4]
        assert step2a.attributes["event.seq"] == 0
        assert step2b.attributes["event.seq"] == 1

    def test_redaction_in_tool_wrapper(self):
        """Verify sensitive data is redacted in tool spans."""
        mock_tool = MagicMock()
        mock_tool.name = "auth_tool"
        mock_tool._run = MagicMock(return_value={"result": "ok"})

        wrapped_tool = _wrap_tool(mock_tool)

        # Input with sensitive key
        wrapped_tool._run(api_key="secret123", normal_param="visible")

        span = self.backend.spans[0]
        inputs = span.attributes[TelemetryKeys.INPUTS]

        assert "[REDACTED]" in inputs
        assert "secret123" not in inputs
        assert "visible" in inputs

    def test_all_spans_have_required_contract_attributes(self):
        """Verify every span has event.type, event.name, event.seq."""
        # Create various span types
        with telemetry.start_span(name="node", span_type=SpanKind.AGENT_NODE):
            with telemetry.start_span(name="tool", span_type=SpanKind.TOOL_CALL):
                pass
            with telemetry.start_span(name="llm", span_type=SpanKind.LLM_CALL):
                pass

        for span in self.backend.spans:
            attrs = span.attributes
            assert "event.type" in attrs, f"Missing event.type in {span.name}"
            assert "event.name" in attrs, f"Missing event.name in {span.name}"
            assert "event.seq" in attrs, f"Missing event.seq in {span.name}"
