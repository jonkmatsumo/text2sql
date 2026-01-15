from unittest.mock import MagicMock, patch

import pytest
from agent_core.llm_client import get_llm
from langchain_core.messages import HumanMessage


@pytest.mark.asyncio
async def test_llm_input_sanitization():
    """Verify that LLM inputs are sanitized before being sent to the provider."""
    # We'll clear the cache to ensure we get a new wrapped LLM
    from agent_core.llm_client import _LLM_CACHE

    _LLM_CACHE.clear()

    with patch("agent_core.llm_client.get_llm_client") as mock_get_client, patch(
        "common.sanitization.sanitize_text"
    ) as mock_sanitize:

        # Mock the underlying LLM client
        from langchain_openai import ChatOpenAI

        mock_llm = MagicMock(spec=ChatOpenAI)
        from unittest.mock import AsyncMock

        mock_llm.ainvoke = AsyncMock(return_value=HumanMessage(content="Response"))
        # Ensure it has the properties LangChain expects for the | operator
        mock_llm.lc_serializable = False
        mock_get_client.return_value = mock_llm

        # Mock sanitization result
        from common.sanitization import SanitizationResult

        mock_sanitize.side_effect = lambda text, **kwargs: SanitizationResult(
            sanitized=text.lower().strip(), is_valid=True, errors=[]
        )

        # Get the wrapped LLM
        llm = get_llm(provider="openai", model="gpt-4o")

        # Payload with "dirty" input
        input_msg = HumanMessage(content="  DIRTY Input  ")

        # Invoke
        await llm.ainvoke(input_msg)

        # 1. Assert sanitization was invoked
        assert mock_sanitize.called

        # 2. Assert the underlying LLM received sanitized content
        assert mock_llm.ainvoke.called
        args, kwargs = mock_llm.ainvoke.call_args
        called_args = args[0]
        assert called_args.content == "dirty input"


@pytest.mark.asyncio
async def test_llm_sanitization_large_payload():
    """Verify that LLM sanitization allows large payloads (e.g. prompts)."""
    from agent_core.llm_client import _LLM_CACHE

    _LLM_CACHE.clear()

    with patch("agent_core.llm_client.get_llm_client") as mock_get_client:
        from langchain_openai import ChatOpenAI

        mock_llm = MagicMock(spec=ChatOpenAI)
        from unittest.mock import AsyncMock

        mock_llm.ainvoke = AsyncMock(return_value=HumanMessage(content="Response"))
        mock_llm.lc_serializable = False
        mock_get_client.return_value = mock_llm

        llm = get_llm(provider="openai", model="gpt-4o")

        # Very long string (greater than default 64)
        long_str = "A" * 500
        await llm.ainvoke(long_str)

        assert mock_llm.ainvoke.called
        args, kwargs = mock_llm.ainvoke.call_args
        called_args = args[0]
        # Should not be truncated to 64
        assert len(called_args) == 500
