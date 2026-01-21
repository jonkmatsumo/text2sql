import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent_core.nodes.generate import generate_sql_node
from agent_core.state import AgentState


@pytest.mark.asyncio
@patch("agent_core.nodes.generate.telemetry.start_span")
@patch("agent_core.llm_client.get_llm")
@patch("agent_core.nodes.generate.ChatPromptTemplate")
@patch("agent_core.tools.get_mcp_tools", new_callable=AsyncMock)
async def test_generate_few_shot_integration(
    mock_get_mcp_tools, mock_prompt_class, mock_get_llm, mock_start_span
):
    """Test integration of few-shot example retrieval."""
    # Setup mocks
    mock_span = MagicMock()
    mock_start_span.return_value.__enter__.return_value = mock_span

    # Mock Tool
    mock_tool = MagicMock()
    mock_tool.name = "get_few_shot_examples"

    examples_data = [
        {"question": "How many users?", "sql": "SELECT count(*) FROM users"},
        {"question": "List admins", "sql": "SELECT * FROM users WHERE role='admin'"},
    ]
    mock_tool.ainvoke = AsyncMock(return_value=json.dumps(examples_data))
    mock_get_mcp_tools.return_value = [mock_tool]

    # Mock Prompt & Chain
    mock_prompt_instance = MagicMock()
    mock_chain = MagicMock()
    mock_prompt_class.from_messages.return_value = mock_prompt_instance
    # When using lazy accessor: chain = prompt | get_llm()
    # mock_get_llm.return_value needs to be set if we were inspecting it,
    # but since we mock the chain result via prompt | llm, we assume valid chain construction.
    # The unused assignment caused lint error.
    mock_prompt_instance.__or__.return_value = mock_chain
    mock_chain.invoke.return_value.content = "SELECT 1"

    # State
    from langchain_core.messages import HumanMessage

    state = AgentState(
        messages=[HumanMessage(content="Test query")],
        schema_context="Table: t1",
        current_sql=None,
    )

    # Run
    await generate_sql_node(state)

    # Verify
    # Check that system prompt contains the formatted examples
    args, _ = mock_prompt_class.from_messages.call_args
    messages_list = args[0]
    system_msg = messages_list[0][1]  # ("system", system_prompt)

    print(f"System Prompt: {system_msg}")

    assert "Question: How many users?" in system_msg
    assert "SQL: SELECT count(*) FROM users" in system_msg
    assert "Question: List admins" in system_msg
