"""Unit tests for DuckDB read-only enforcement."""

import os
from unittest.mock import MagicMock, patch

import pytest

from dal.duckdb.config import DuckDBConfig
from dal.duckdb.query_target import DuckDBQueryTargetDatabase

# Test the config/flag logic


@pytest.mark.asyncio
async def test_duckdb_config_defaults_readonly():
    """Verify that DuckDBConfig defaults to read-only."""
    # Use patch.dict instead of affecting global os.environ directly
    with patch.dict(os.environ, clear=True):
        config = DuckDBConfig.from_env()
        assert config.read_only is True


@pytest.mark.asyncio
async def test_duckdb_config_override_readonly():
    """Verify that DUCKDB_READ_ONLY=false works."""
    with patch.dict(os.environ, {"DUCKDB_READ_ONLY": "false"}):
        config = DuckDBConfig.from_env()
        assert config.read_only is False


@pytest.mark.asyncio
async def test_duckdb_readonly_flag_passed():
    """Verify that the read_only flag is passed to duckdb.connect."""
    # Skip if duckdb is not installed to avoid ModuleNotFoundError during patch
    pytest.importorskip("duckdb")

    config = DuckDBConfig(path=":memory:", query_timeout_seconds=5, max_rows=100, read_only=True)
    await DuckDBQueryTargetDatabase.init(config)

    mock_conn = MagicMock()
    with patch("duckdb.connect", return_value=mock_conn) as mock_connect:
        async with DuckDBQueryTargetDatabase.get_connection() as conn:
            assert conn is not None
            mock_connect.assert_called_once_with(":memory:", read_only=True)
