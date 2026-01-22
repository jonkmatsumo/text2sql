import json
from unittest.mock import MagicMock, patch

import pytest

from mcp_server.services.rag.retrieval import get_relevant_examples


class TestRetrieval:
    """Test suite for retrieval module."""

    @pytest.mark.asyncio
    async def test_get_relevant_examples(self):
        """Test retrieving examples via RegistryService."""
        mock_example = MagicMock()
        mock_example.question = "Q1"
        mock_example.sql_query = "SELECT 1"
        mock_example.signature_key = "abcdef123456"

        async def _fake_get_examples(_user_query, _tenant_id, limit=3):
            return [mock_example]

        with patch(
            "mcp_server.services.registry.RegistryService.get_few_shot_examples",
            new=_fake_get_examples,
        ):

            result_json = await get_relevant_examples("query")

            results = json.loads(result_json)
            assert len(results) == 1
            assert results[0]["question"] == "Q1"
            assert results[0]["sql"] == "SELECT 1"
            assert results[0]["signature"] == "abcdef12"

    @pytest.mark.asyncio
    async def test_get_relevant_examples_no_results(self):
        """Test when search returns nothing."""

        async def _fake_get_examples(_user_query, _tenant_id, limit=3):
            return []

        with patch(
            "mcp_server.services.registry.RegistryService.get_few_shot_examples",
            new=_fake_get_examples,
        ):

            result = await get_relevant_examples("query")
            assert result == ""
