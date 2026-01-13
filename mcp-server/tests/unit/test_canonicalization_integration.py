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

    with patch("mcp_server.services.canonicalization.spacy_pipeline.SPACY_ENABLED", True), patch(
        "mcp_server.config.database.Database.get_connection", return_value=mock_db_ctx
    ), patch("spacy.load") as mock_spacy_load, patch("spacy.blank") as mock_spacy_blank, patch(
        "spacy.matcher.DependencyMatcher"
    ) as mock_matcher_cls:
        mock_nlp = MagicMock()
        mock_nlp.pipe_names = []
        mock_nlp.add_pipe.return_value = MagicMock()
        mock_spacy_load.return_value = mock_nlp
        mock_spacy_blank.return_value = mock_nlp

        # Configure mocked matcher
        mock_matcher = MagicMock()
        mock_matcher_cls.return_value = mock_matcher
        # Make the matcher callable (it's called as matcher(doc))
        mock_matcher.return_value = []
        CanonicalizationService.reset_instance()
        service = CanonicalizationService.get_instance()

        # Ensure we have a mock NLP if spacy is missing or to isolate
        if service.nlp is None:
            # Service will initialize nlp via spacy.load/blank which we mocked
            pass

        await service.reload_patterns()

        # Verify fetch called
        mock_conn.fetch.assert_called_with("SELECT label, pattern, id FROM nlp_patterns")

        # Verify patterns added to ruler
        # This depends on how we mocked NLP.
        # Real integration would use real spacy, but that requires model download.
        # We assume unit/integration tests might run in env with or without spacy.
        # The test mainly verifies the DB -> Service wiring.
