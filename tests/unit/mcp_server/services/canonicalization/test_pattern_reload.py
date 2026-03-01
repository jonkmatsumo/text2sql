import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from common.constants.ml_operability import RELOAD_FAILURE_REASON_RELOAD_EXCEPTION
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
        assert result.reason_code is None
        assert result.pattern_count == 42
        assert result.reloaded_at is not None
        assert result.reload_id is not None
        assert result.duration_ms >= 0
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
        assert result.reason_code == RELOAD_FAILURE_REASON_RELOAD_EXCEPTION
        assert result.pattern_count is None
        assert result.reloaded_at is not None
        assert result.reload_id is not None
        assert result.duration_ms >= 0
        mock_instance.reload_patterns.assert_awaited_once()


@pytest.mark.asyncio
async def test_canonicalization_service_reload_returns_count():
    """Test that CanonicalizationService.reload_patterns actually returns a count (mocking DB)."""
    # This test verifies the refactor of spacy_pipeline.py logic, but we need to mock DB.
    # We can mock the database connection to return some rows.

    with (
        patch("dal.database.Database") as MockDB,
        patch("mcp_server.services.canonicalization.spacy_pipeline.SPACY_ENABLED", True),
    ):
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
            mock_nlp_1 = MagicMock()
            mock_nlp_2 = MagicMock()
            mock_load.side_effect = [mock_nlp_1, mock_nlp_2]

            # Mock pipe handling for both
            mock_ruler = MagicMock()
            mock_nlp_1.add_pipe.return_value = mock_ruler
            mock_nlp_1.get_pipe.return_value = mock_ruler
            mock_nlp_2.add_pipe.return_value = mock_ruler
            mock_nlp_2.get_pipe.return_value = mock_ruler

            # Initialize service (consumes mock_nlp_1)
            CanonicalizationService.reset_instance()

            # Mock _setup_entity_ruler and _setup_dependency_matcher to avoid IO/Cython issues
            with (
                patch.object(
                    CanonicalizationService, "_setup_entity_ruler", return_value=2
                ) as mock_setup,
                patch.object(CanonicalizationService, "_setup_dependency_matcher"),
            ):
                service = CanonicalizationService("en_core_web_sm")

                # Check initial state
                assert service._state.nlp == mock_nlp_1

                # Execute reload (consumes mock_nlp_2)
                count = await service.reload_patterns()

                # Assert
                assert count == 2

                # Verify atomic swap
                assert service._state.nlp == mock_nlp_2

                # Verify _setup_entity_ruler called twice (once per pipeline)
                assert mock_setup.call_count == 2

                # Verify _setup_entity_ruler called with patterns on the second call
                # First call (init): extra_patterns=None (or default)
                # Second call (reload): extra_patterns=[...]
                args, kwargs = mock_setup.call_args_list[1]
                assert args[0] == mock_nlp_2
                assert len(kwargs.get("extra_patterns") or args[1]) == 2


@pytest.mark.asyncio
async def test_reload_concurrency():
    """Test reloading while extracting constraints concurrently."""
    with (
        patch("dal.database.Database") as MockDB,
        patch("mcp_server.services.canonicalization.spacy_pipeline.SPACY_ENABLED", True),
    ):

        # Setup DB mock (return empty patterns to avoid complex setup)
        mock_conn = AsyncMock()
        MockDB.get_connection.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetch.return_value = []

        with patch("spacy.load") as mock_load:
            mock_nlp = MagicMock()
            mock_load.return_value = mock_nlp
            # Configure NLP mock to return a DOC with valid entities when called
            mock_doc = MagicMock()
            mock_doc.ents = []
            mock_nlp.return_value = mock_doc

            mock_nlp.add_pipe.return_value = MagicMock()

            # Init service
            CanonicalizationService.reset_instance()

            with (
                patch.object(CanonicalizationService, "_setup_entity_ruler", return_value=0),
                patch.object(CanonicalizationService, "_setup_dependency_matcher"),
            ):

                service = CanonicalizationService("en_core_web_sm")

                # Background task: keep extracting
                stop_event = asyncio.Event()

                async def extract_loop():
                    count = 0
                    while not stop_event.is_set():
                        service.extract_constraints("some query")
                        count += 1
                        await asyncio.sleep(0.001)
                    return count

                task = asyncio.create_task(extract_loop())

                # Trigger reload multiple times
                for _ in range(5):
                    await service.reload_patterns()
                    await asyncio.sleep(0.01)

                stop_event.set()
                count = await task
                assert count > 0
