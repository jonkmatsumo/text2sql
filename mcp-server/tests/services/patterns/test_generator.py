"""Unit tests for pattern generator."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.services.patterns.generator import enrich_values_with_llm, generate_entity_patterns


@pytest.mark.asyncio
async def test_enrich_values_with_llm_success():
    """Test successful LLM enrichment."""
    mock_client = AsyncMock()
    mock_response = MagicMock()
    mock_response.choices = [
        MagicMock(message=MagicMock(content='[{"pattern": "syn1", "id": "VAL"}]'))
    ]
    mock_client.chat.completions.create.return_value = mock_response

    patterns = await enrich_values_with_llm(mock_client, "LABEL", ["VAL"])

    assert len(patterns) == 1
    assert patterns[0]["pattern"] == "syn1"
    assert patterns[0]["label"] == "LABEL"
    assert patterns[0]["id"] == "VAL"


@pytest.mark.asyncio
async def test_enrich_values_with_llm_no_client():
    """Test enrichment with no client returns empty."""
    patterns = await enrich_values_with_llm(None, "LABEL", ["VAL"])
    assert patterns == []


@pytest.mark.asyncio
async def test_generate_entity_patterns():
    """Test the full generation pipeline with mocks."""
    # Mock Database
    mock_conn = AsyncMock()
    # Mock ratings fetch
    mock_conn.fetch.side_effect = [[{"rating": "G"}], [{"name": "Action"}]]  # Ratings  # Genres

    mock_db_ctx = AsyncMock()
    mock_db_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db_ctx.__aexit__ = AsyncMock(return_value=None)

    # Mock OpenAI Client
    mock_client = AsyncMock()
    # Mock LLM calls (called twice: ratings, genres)
    mock_llm_resp = MagicMock()
    mock_llm_resp.choices = [
        MagicMock(message=MagicMock(content='[{"pattern": "synonym", "id": "ID"}]'))
    ]
    mock_client.chat.completions.create.return_value = mock_llm_resp

    with patch(
        "mcp_server.config.database.Database.get_connection", return_value=mock_db_ctx
    ), patch("mcp_server.services.patterns.generator.get_openai_client", return_value=mock_client):

        patterns = await generate_entity_patterns()

        # Verify db interactions
        assert mock_conn.fetch.call_count == 2

        # Verify basic patterns exist
        assert any(p["pattern"] == "G" for p in patterns)
        assert any(p["pattern"] == "action" for p in patterns)

        # Verify enriched patterns exist (from mocked LLM)
        assert any(p["pattern"] == "synonym" for p in patterns)
