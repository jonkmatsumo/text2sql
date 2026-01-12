from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.services.canonicalization.pattern_reload_service import PatternReloadService
from mcp_server.services.canonicalization.spacy_pipeline import CanonicalizationService


@pytest.mark.asyncio
async def test_reload_success():
    """Test successful pattern reload."""
    with patch(
        "mcp_server.services.canonicalization.pattern_reload_service.CanonicalizationService"
    ) as MockService:
        # Setup mock
        mock_instance = AsyncMock()
        MockService.get_instance.return_value = mock_instance
        mock_instance.reload_patterns.return_value = 42

        # Execute
        result = await PatternReloadService.reload()

        # Assert
        assert result.success is True
        assert result.error is None
        assert result.pattern_count == 42
        assert result.reloaded_at is not None
        mock_instance.reload_patterns.assert_awaited_once()


@pytest.mark.asyncio
async def test_reload_failure():
    """Test pattern reload failure."""
    with patch(
        "mcp_server.services.canonicalization.pattern_reload_service.CanonicalizationService"
    ) as MockService:
        # Setup mock to raise exception
        mock_instance = AsyncMock()
        MockService.get_instance.return_value = mock_instance
        mock_instance.reload_patterns.side_effect = Exception("DB Connection Failed")

        # Execute
        result = await PatternReloadService.reload()

        # Assert
        assert result.success is False
        assert result.error == "DB Connection Failed"
        assert result.pattern_count is None
        assert result.reloaded_at is not None
        mock_instance.reload_patterns.assert_awaited_once()


@pytest.mark.asyncio
async def test_canonicalization_service_reload_returns_count():
    """Test that CanonicalizationService.reload_patterns actually returns a count (mocking DB)."""
    # This test verifies the refactor of spacy_pipeline.py logic, but we need to mock DB.
    # We can mock the database connection to return some rows.

    with patch("mcp_server.config.database.Database") as MockDB:
        # Setup DB mock
        mock_conn = AsyncMock()
        MockDB.get_connection.return_value.__aenter__.return_value = mock_conn

        # Mock rows
        mock_rows = [
            {"label": "ENTITY", "pattern": "foo", "id": 1},
            {"label": "RATING", "pattern": "PG", "id": 2},
        ]
        mock_conn.fetch.return_value = mock_rows

        # Mock spaCy load
        with patch("spacy.load") as mock_load:
            mock_nlp = MagicMock()
            mock_load.return_value = mock_nlp

            # Mock pipe handling
            mock_nlp.pipe_names = ["entity_ruler"]
            mock_ruler = MagicMock()
            mock_nlp.add_pipe.return_value = mock_ruler
            mock_nlp.get_pipe.return_value = mock_ruler

            # Initialize service
            CanonicalizationService.reset_instance()

            # Mock _setup_entity_ruler and _setup_dependency_matcher to avoid IO/Cython issues
            with patch.object(
                CanonicalizationService, "_setup_entity_ruler"
            ) as mock_setup, patch.object(CanonicalizationService, "_setup_dependency_matcher"):
                service = CanonicalizationService("en_core_web_sm")

                # Execute reload
                count = await service.reload_patterns()

                # Assert
                assert count == 2
                # We expect remove_pipe then add_pipe
                # (or add if not exists, but we mocked pipe_names=['entity_ruler'])
                mock_nlp.remove_pipe.assert_called_with("entity_ruler")
                mock_nlp.add_pipe.assert_called_with("entity_ruler", before="ner")
                # _setup_entity_ruler should be called twice (init + reload)
                assert mock_setup.call_count == 2
                # add_patterns should be called with DB patterns
                mock_ruler.add_patterns.assert_called()
