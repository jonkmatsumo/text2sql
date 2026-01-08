import json
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# Mock missing dependency before imports
sys.modules["langchain_mcp_adapters"] = MagicMock()
sys.modules["langchain_mcp_adapters.client"] = MagicMock()

from agent_core.nodes.generate import generate_sql_node  # noqa: E402
from agent_core.state import AgentState  # noqa: E402
from langchain_core.messages import HumanMessage  # noqa: E402


class TestGenerateSchema(unittest.IsolatedAsyncioTestCase):
    """Unit tests for structured schema handling in generate_sql_node."""

    @patch("agent_core.nodes.generate.mlflow.start_span")
    @patch("agent_core.nodes.generate.llm")
    @patch("agent_core.nodes.generate.ChatPromptTemplate")
    @patch("agent_core.tools.get_mcp_tools")
    @patch("agent_core.nodes.generate.check_cache", new_callable=AsyncMock)
    @patch("agent_core.nodes.generate.get_few_shot_examples", new_callable=AsyncMock)
    async def test_generate_sql_with_structured_schema(
        self,
        mock_few_shot,
        mock_check_cache,
        mock_get_mcp_tools,
        mock_prompt_class,
        mock_llm,
        mock_start_span,
    ):
        """Test that generate_sql_node correctly parses and formats structured schema output."""
        # Setup Defaults
        mock_check_cache.return_value = None
        mock_few_shot.return_value = ""

        # Mock LLM and Span
        mock_span = MagicMock()
        mock_start_span.return_value.__enter__.return_value = mock_span

        mock_prompt_instance = MagicMock()
        mock_chain = MagicMock()
        mock_prompt_class.from_messages.return_value = mock_prompt_instance
        mock_prompt_instance.__or__.return_value = mock_chain
        mock_chain.invoke.return_value.content = "SELECT * FROM film"

        # Mock Schema Tool Output (Structured JSON)
        mock_schema_tool = MagicMock()
        mock_schema_tool.name = "get_table_schema_tool"

        schema_payload = [
            {
                "table_name": "film",
                "columns": [
                    {"name": "film_id", "type": "integer", "nullable": False},
                    {"name": "title", "type": "text", "nullable": False},
                ],
                "foreign_keys": [],
            }
        ]
        # Simulate MCP return value (stringified JSON)
        mock_schema_tool.ainvoke = AsyncMock(return_value=json.dumps(schema_payload))
        mock_get_mcp_tools.return_value = [mock_schema_tool]

        # Setup State with table_names populated (triggering schema fetch)
        state = AgentState(
            messages=[HumanMessage(content="Show films")],
            schema_context="Summary...",
            current_sql=None,
            table_names=["film"],
        )

        # Execute
        await generate_sql_node(state)

        # Verify Prompt content
        # We need to capture the formatted prompt passed to the LLM
        args, _ = mock_chain.invoke.call_args
        # args[0] is the input dict to the chain
        chain_input = args[0]

        # In generate.py, the prompt variable "schema_context" is populated with
        # schema_context_to_use which should be our formatted DDL.
        actual_context = chain_input.get("schema_context")

        print(f"Actual Context:\n{actual_context}")

        self.assertIn("Table: film", actual_context)
        self.assertIn("Columns:", actual_context)
        self.assertIn("- film_id (integer, REQUIRED)", actual_context)
        self.assertIn("- title (text, REQUIRED)", actual_context)


if __name__ == "__main__":
    unittest.main()
