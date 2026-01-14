"""Tests for CLI vector index wiring."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.services.seeding.cli import _ingest_graph_schema


@pytest.mark.asyncio
async def test_ingest_graph_schema_wires_vector_index():
    """Verify that _ingest_graph_schema calls ensure_table_embedding_hnsw_index."""
    # Mock all the dependencies
    with patch("mcp_server.services.seeding.cli.get_schema_introspector"), patch(
        "mcp_server.services.seeding.cli.GraphHydrator"
    ) as MockHydrator, patch(
        "mcp_server.services.ingestion.vector_index_ddl.ensure_table_embedding_hnsw_index"
    ) as mock_ensure:

        # Setup mock hydrator
        mock_hydrator_instance = MockHydrator.return_value
        mock_hydrator_instance.hydrate_schema = AsyncMock()

        # Setup mock driver/session
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session

        mock_hydrator_instance.store.driver = mock_driver

        # Run the function
        await _ingest_graph_schema()

        # Verify ensure was called
        mock_ensure.assert_called_once_with(mock_session)

        # Verify sequence: hydrate -> ensure -> close (by context)
        # (Implicit in code flow, verifying Ensure is called is key)


@pytest.mark.asyncio
async def test_ingest_graph_schema_handles_ensure_error():
    """Verify that vector index failure does not crash ingestion."""
    with patch("mcp_server.services.seeding.cli.get_schema_introspector"), patch(
        "mcp_server.services.seeding.cli.GraphHydrator"
    ) as MockHydrator, patch(
        "mcp_server.services.ingestion.vector_index_ddl.ensure_table_embedding_hnsw_index"
    ) as mock_ensure:

        mock_hydrator_instance = MockHydrator.return_value
        mock_hydrator_instance.hydrate_schema = AsyncMock()

        # Mock failure
        mock_ensure.side_effect = Exception("Connection Refused")

        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = MagicMock()
        mock_hydrator_instance.store.driver = mock_driver

        # Should not raise exception (caught and logged)
        await _ingest_graph_schema()

        mock_ensure.assert_called_once()
