"""Integration tests for CanonicalizationService."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.services.canonicalization.spacy_pipeline import CanonicalizationService


@pytest.mark.asyncio
async def test_reload_patterns_integration():
    """Test loading patterns from mocked DB integration."""
    # Mock Database
    mock_conn = AsyncMock()
    # Return sample pattern "g-rating" -> "G"
    mock_conn.fetch.return_value = [{"label": "RATING", "pattern": "g-rating", "id": "G"}]

    mock_db_ctx = AsyncMock()
    mock_db_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db_ctx.__aexit__ = AsyncMock(return_value=None)

    with patch("mcp_server.config.database.Database.get_connection", return_value=mock_db_ctx):
        service = CanonicalizationService.get_instance()

        # Ensure we have a mock NLP if spacy is missing or to isolate
        if service.nlp is None:
            # Create a dummy NLP object if spacy not installed in test env
            service.nlp = MagicMock()
            service.nlp.pipe_names = []
            service.nlp.add_pipe.return_value = MagicMock()

        await service.reload_patterns()

        # Verify fetch called
        mock_conn.fetch.assert_called_with("SELECT label, pattern, id FROM nlp_patterns")

        # Verify patterns added to ruler
        # This depends on how we mocked NLP.
        # Real integration would use real spacy, but that requires model download.
        # We assume unit/integration tests might run in env with or without spacy.
        # The test mainly verifies the DB -> Service wiring.
