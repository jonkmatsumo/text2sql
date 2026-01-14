from unittest.mock import MagicMock

import pytest
from agent_core.llm_client import extract_token_usage
from langchain_core.messages import AIMessage


def test_extract_token_usage_openai():
    """Verify OpenAI format extraction."""
    response = AIMessage(
        content="test",
        response_metadata={
            "token_usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}
        },
    )
    usage = extract_token_usage(response)
    assert usage["llm.token_usage.input_tokens"] == 10
    assert usage["llm.token_usage.output_tokens"] == 20
    assert usage["llm.token_usage.total_tokens"] == 30


def test_extract_token_usage_anthropic():
    """Verify Anthropic format extraction (dict)."""
    response = AIMessage(
        content="test",
        response_metadata={"usage": {"input_tokens": 5, "output_tokens": 15, "total_tokens": 20}},
    )
    usage = extract_token_usage(response)
    assert usage["llm.token_usage.input_tokens"] == 5
    assert usage["llm.token_usage.output_tokens"] == 15
    # Total tokens might be inferred or present
    assert usage["llm.token_usage.total_tokens"] == 20


def test_extract_token_usage_anthropic_object():
    """Verify Anthropic format extraction (object)."""
    usage_obj = MagicMock()
    usage_obj.input_tokens = 50
    usage_obj.output_tokens = 150
    usage_obj.total_tokens = 200

    response = AIMessage(content="test", response_metadata={"usage": usage_obj})
    usage = extract_token_usage(response)
    assert usage["llm.token_usage.input_tokens"] == 50
    assert usage["llm.token_usage.output_tokens"] == 150
    assert usage["llm.token_usage.total_tokens"] == 200


def test_extract_token_usage_google():
    """Verify Google Gemini format extraction."""
    response = AIMessage(
        content="test",
        response_metadata={
            "usage": {
                "prompt_token_count": 100,
                "candidates_token_count": 200,
                "total_token_count": 300,
            }
        },
    )
    usage = extract_token_usage(response)
    assert usage["llm.token_usage.input_tokens"] == 100
    assert usage["llm.token_usage.output_tokens"] == 200
    assert usage["llm.token_usage.total_tokens"] == 300


# Integration-like unit test for node logic (mocking the chain)
@pytest.mark.asyncio
async def test_generate_node_telemetry():
    """Verify generate_sql_node sets usage attributes."""
    from agent_core.nodes.generate import generate_sql_node

    # mock state
    state = {
        "messages": [MagicMock(content="Wait what?")],
        "schema_context": "context",
        "tenant_id": 1,
    }

    # Mock telemetry to capture span
    mock_telemetry = MagicMock()
    mock_span = MagicMock()
    mock_telemetry.start_span.return_value.__enter__.return_value = mock_span

    # Mock get_llm to return a chain that returns a message with metadata
    mock_chain = MagicMock()
    mock_response = AIMessage(
        content="SELECT 1",
        response_metadata={
            "token_usage": {"prompt_tokens": 11, "completion_tokens": 22, "total_tokens": 33}
        },
    )
    mock_chain.invoke.return_value = mock_response

    # Mock get_few_shot_examples to avoid async issues or tools import
    # We need to patch where it's used
    from unittest.mock import patch

    with patch("agent_core.nodes.generate.telemetry", mock_telemetry), patch(
        "agent_core.llm_client.get_llm", return_value=lambda **k: mock_chain
    ), patch("agent_core.nodes.generate.get_few_shot_examples") as mock_fs, patch(
        "agent_core.nodes.generate.ChatPromptTemplate.from_messages"
    ) as mock_from_messages:

        mock_fs.return_value = ""

        async def async_fs(*args, **kwargs):
            return ""

        mock_fs.side_effect = async_fs

        # Mock prompt construction
        mock_prompt = MagicMock()
        mock_prompt.__or__.return_value = mock_chain
        mock_from_messages.return_value = mock_prompt

        await generate_sql_node(state)

    # Assert
    # Check if set_attributes was called with usage stats
    # We look for a call with the usage dict
    expected_usage = {
        "llm.token_usage.input_tokens": 11,
        "llm.token_usage.output_tokens": 22,
        "llm.token_usage.total_tokens": 33,
    }

    # Verify set_attributes was called with a dict containing these keys
    # Helper to check calls
    found = False
    for call in mock_span.set_attributes.call_args_list:
        args, _ = call
        if args and args[0] == expected_usage:
            found = True
            break

    # If using set_attributes(usage_stats), verify it passed
    if not found:
        # Maybe it was mixed with other attributes?
        # The code does: span.set_attributes(usage_stats) separately.
        mock_span.set_attributes.assert_any_call(expected_usage)
