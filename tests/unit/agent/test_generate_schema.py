"""Unit tests for structured schema handling in generate_sql_node."""

import importlib.machinery
import importlib.util
import json
import sys
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# Mock MCP SDK only if not installed
if importlib.util.find_spec("mcp") is None:

    def create_mock_module(name):
        """Create a mock module for testing."""
        mock = types.ModuleType(name)
        mock.__spec__ = importlib.machinery.ModuleSpec(name, loader=None)
        mock.__path__ = []
        sys.modules[name] = mock
        return mock

    mcp_mock = create_mock_module("mcp")
    mcp_mock.ClientSession = MagicMock()

    mcp_types = create_mock_module("mcp_server.types")
    mcp_mock.types = mcp_types

    create_mock_module("mcp_server.client")
    create_mock_module("mcp_server.client.sse")
    create_mock_module("mcp_server.client.streamable_http")


from langchain_core.messages import HumanMessage  # noqa: E402

from agent.nodes.generate import generate_sql_node  # noqa: E402
from agent.state import AgentState  # noqa: E402


class TestGenerateSchema(unittest.IsolatedAsyncioTestCase):
    """Unit tests for structured schema handling in generate_sql_node."""

    @patch("agent.nodes.generate.telemetry.start_span")
    @patch("agent.llm_client.get_llm")
    @patch("agent.nodes.generate.ChatPromptTemplate")
    @patch("agent.nodes.generate.get_few_shot_examples", new_callable=AsyncMock)
    async def test_generate_sql_uses_schema_context(
        self,
        mock_few_shot,
        mock_prompt_class,
        mock_llm,
        mock_start_span,
    ):
        """Test that generate_sql_node uses schema_context from state."""
        # Setup Defaults
        mock_few_shot.return_value = ""

        # Mock LLM and Span
        mock_span = MagicMock()
        mock_start_span.return_value.__enter__.return_value = mock_span

        mock_prompt_instance = MagicMock()
        mock_chain = MagicMock()
        mock_prompt_class.from_messages.return_value = mock_prompt_instance
        mock_prompt_instance.__or__.return_value = mock_chain
        mock_chain.invoke.return_value.content = "SELECT * FROM film"

        # Simulate schema_context from retrieve node (compact markdown format)
        schema_context = """# Schema Context

## Tables
- **film** (film_id `pk`, title, description)
- **actor** (actor_id `pk`, first_name, last_name)

## Joins
- **film** JOIN **actor** ON film_id"""

        # Setup State with schema_context populated from retrieve node
        state = AgentState(
            messages=[HumanMessage(content="Show films")],
            schema_context=schema_context,
            current_sql=None,
            table_names=["film", "actor"],
        )

        # Execute
        await generate_sql_node(state)

        # Verify Prompt content
        args, _ = mock_chain.invoke.call_args
        chain_input = args[0]

        # Check that schema_context was passed to the LLM
        actual_context = chain_input.get("schema_context")

        self.assertIn("film", actual_context)
        self.assertIn("actor", actual_context)
        self.assertIn("## Tables", actual_context)

    @patch("agent.tools.get_mcp_tools")
    async def test_generate_few_shot_structured(self, mock_get_mcp_tools):
        """Test that get_few_shot_examples output is handled correctly."""
        # 1. Setup Mock for get_few_shot_examples
        mock_example_tool = MagicMock()
        mock_example_tool.name = "get_few_shot_examples"

        # Simulate compact JSON output (no spaces)
        examples_payload = [
            {
                "question": "Find films with rating 'PG'",
                "sql": "SELECT * FROM film WHERE rating = 'PG';",
                "similarity": 0.806,
            }
        ]
        # MCP tool returns stringified compact JSON
        mock_example_tool.ainvoke = AsyncMock(
            return_value=json.dumps(examples_payload, separators=(",", ":"))
        )
        mock_get_mcp_tools.return_value = [mock_example_tool]

        # 2. Call the function under test
        from agent.utils.parsing import parse_tool_output

        # Simulate the tool call result
        tool_output = await mock_example_tool.ainvoke("query")

        # 3. Verify parse_tool_output correctly unpacks it
        parsed = parse_tool_output(tool_output)

        self.assertIsInstance(parsed, list)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["question"], "Find films with rating 'PG'")

        # Verify strict compactness (internal check)
        self.assertNotIn("\n", tool_output)
        self.assertNotIn(": ", tool_output)  # no space after colon


if __name__ == "__main__":
    unittest.main()
