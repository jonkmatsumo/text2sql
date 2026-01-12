"""Unit tests for MaintenanceService."""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.services.ops.maintenance import MaintenanceService


@pytest.mark.asyncio
async def test_generate_patterns_run():
    """Test that generate_patterns runs and reports success."""
    # Mock the generator function
    with patch(
        "mcp_server.services.patterns.generator.generate_entity_patterns"
    ) as mock_gen, patch("mcp_server.config.database.Database.get_connection") as mock_db:

        mock_gen.return_value = [{"label": "TEST", "pattern": "p", "id": "t"}]
        mock_conn = AsyncMock()
        mock_db.return_value.__aenter__.return_value = mock_conn

        messages = []
        async for msg in MaintenanceService.generate_patterns(dry_run=False):
            messages.append(msg)

        assert len(messages) > 0
        assert mock_gen.called
        assert "Patterns successfully saved" in messages[-1]
        mock_conn.executemany.assert_called_once()


@pytest.mark.asyncio
async def test_generate_patterns_dry_run():
    """Test dry run mode."""
    with patch("mcp_server.services.patterns.generator.generate_entity_patterns") as mock_gen:
        mock_gen.return_value = [{"label": "TEST", "pattern": "p", "id": "t"}]

        messages = []
        async for msg in MaintenanceService.generate_patterns(dry_run=True):
            messages.append(msg)

        assert "DRY RUN" in messages[2]
        assert "Sample:" in messages[3]


@pytest.mark.asyncio
async def test_hydrate_schema_stub():
    """Test that hydrate_schema yields expected stub messages."""
    messages = []
    async for msg in MaintenanceService.hydrate_schema():
        messages.append(msg)

    assert len(messages) > 0
    assert "Starting schema hydration..." in messages


@pytest.mark.asyncio
async def test_reindex_cache_stub():
    """Test that reindex_cache yields expected stub messages."""
    messages = []
    async for msg in MaintenanceService.reindex_cache():
        messages.append(msg)

    assert len(messages) > 0
    assert "Starting cache re-indexing..." in messages
