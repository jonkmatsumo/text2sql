"""Unit tests for seeding service."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.services.seeding import cli
from mcp_server.services.seeding.loader import load_from_directory


class TestLoader:
    """Tests for JSON loader."""

    def test_load_from_directory(self, tmp_path):
        """Test loading JSONs from directory."""
        # Create dummy files
        d = tmp_path / "seeds"
        d.mkdir()

        # File 1: List
        f1 = d / "1.json"
        f1.write_text('[{"q": 1}]')

        # File 2: Dict
        f2 = d / "2.json"
        f2.write_text('{"queries": [{"q": 2}]}')

        data = load_from_directory(d)
        assert len(data) == 2
        assert {"q": 1} in data
        assert {"q": 2} in data

    def test_load_invalid_dir(self):
        """Test graceful failure on missing dir."""
        data = load_from_directory(Path("/non/existent"))
        assert data == []


class TestSeederCli:
    """Tests for Seeder CLI functions."""

    @pytest.mark.asyncio
    @patch("mcp_server.seeding.cli.Database")
    @patch("mcp_server.seeding.cli.RagEngine")
    @patch("mcp_server.seeding.cli.format_vector_for_postgres")
    @patch("mcp_server.seeding.cli.load_from_directory")
    async def test_main_processing(self, mock_load, mock_format, mock_rag, mock_db):
        """Test unified processing of seed items."""
        mock_load.return_value = [
            {
                "question": "Q1",
                "query": "SELECT 1",
                "expected_result": [{"a": 1}],
                "difficulty": "hard",
            }
        ]

        mock_rag.embed_text.return_value = [0.1]
        mock_format.return_value = "[0.1]"

        mock_conn = AsyncMock()
        mock_db.get_connection.return_value.__aenter__.return_value = mock_conn

        # Override base path
        # main() uses Path("/app/seeds"). We should mock load_from_directory to ignore path.

        # We can't easily test main() because of hardcoded path,
        # so we test the helper or refactor main.
        # But for now, let's just invoke the helpers.

        await cli._upsert_sql_example(mock_conn, mock_load.return_value[0])
        await cli._upsert_golden_record(mock_conn, mock_load.return_value[0])

        # Verify SQL Examples insert
        assert mock_conn.execute.call_count == 2

        # Call 1: SQL Example
        args1 = mock_conn.execute.call_args_list[0][0]
        assert "INSERT INTO sql_examples" in args1[0]
        assert args1[1] == "Q1"

        # Call 2: Golden Dataset
        args2 = mock_conn.execute.call_args_list[1][0]
        assert "INSERT INTO golden_dataset" in args2[0]
        assert args2[6] == "hard"
