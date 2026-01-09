import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from mcp_server.graph_ingestion.enrichment.agent import EnrichmentAgent


@patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"})
class TestEnrichmentAgent(unittest.IsolatedAsyncioTestCase):
    """Test suite for EnrichmentAgent."""

    @patch("mcp_server.graph_ingestion.enrichment.agent.AsyncOpenAI")
    async def test_generate_description_success(self, mock_async_openai):
        """Test successful description generation."""
        # Setup mock client and response
        mock_client = mock_async_openai.return_value
        mock_create = AsyncMock()
        mock_client.chat.completions.create = mock_create

        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content="Mocked Description"))]
        mock_create.return_value = mock_response

        agent = EnrichmentAgent()
        node_data = {"name": "users", "sample_data": '[{"id": 1, "name": "Alice"}]'}

        description = await agent.generate_description(node_data)

        assert description == "Mocked Description"

        # Verify prompt construction
        call_args = mock_create.call_args
        assert call_args is not None
        _, kwargs = call_args
        messages = kwargs["messages"]
        user_content = messages[1]["content"]

        assert "users" in user_content
        assert '[{"id": 1, "name": "Alice"}]' in user_content

    @patch("mcp_server.graph_ingestion.enrichment.agent.AsyncOpenAI")
    async def test_generate_description_empty_response(self, mock_async_openai):
        """Test handling of empty API response."""
        # Setup mock client with empty choices
        mock_client = mock_async_openai.return_value
        mock_create = AsyncMock()
        mock_client.chat.completions.create = mock_create

        mock_response = MagicMock()
        mock_response.choices = []
        mock_create.return_value = mock_response

        agent = EnrichmentAgent()
        node_data = {"name": "users"}

        description = await agent.generate_description(node_data)

        assert description is None

    @patch("mcp_server.graph_ingestion.enrichment.agent.AsyncOpenAI")
    async def test_generate_description_exception(self, mock_async_openai):
        """Test handling of API exception."""
        # Setup mock client to raise exception
        mock_client = mock_async_openai.return_value
        mock_create = AsyncMock()
        mock_client.chat.completions.create = mock_create

        mock_create.side_effect = Exception("API Error")

        agent = EnrichmentAgent()
        node_data = {"name": "users"}

        description = await agent.generate_description(node_data)

        assert description is None
