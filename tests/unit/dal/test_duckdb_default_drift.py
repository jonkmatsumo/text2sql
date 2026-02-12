from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dal.database import Database


@pytest.mark.asyncio
async def test_duckdb_defaults_to_read_only_when_key_missing_in_guardrails():
    """Verify DuckDB defaults to read_only=True when key is omitted in guardrails."""
    # Mock runtime selection
    mock_selection = MagicMock()
    mock_selection.pending = MagicMock()
    mock_selection.pending.provider = "duckdb"
    mock_selection.pending.metadata = {"path": ":memory:"}
    # Guardrails block present but "read_only" key missing
    mock_selection.pending.guardrails = {"max_rows": 500}
    mock_selection.active = None

    with patch(
        "dal.query_target_config_source.load_query_target_config_selection",
        AsyncMock(return_value=mock_selection),
    ):
        with patch("dal.duckdb.DuckDBQueryTargetDatabase.init", AsyncMock()) as mock_init:
            with patch("dal.capabilities.capabilities_for_provider", MagicMock()):
                # Reset class state for clean test
                Database._query_target_provider = "postgres"

                await Database.init()

                # Verify DuckDB was initialized with read_only=True
                args, _ = mock_init.call_args
                config = args[0]
                assert config.read_only is True
                assert config.max_rows == 500
