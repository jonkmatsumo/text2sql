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
    @patch("mcp_server.services.seeding.cli.load_from_directory")
    @patch("mcp_server.services.seeding.cli.RegistryService.register_pair", new_callable=AsyncMock)
    async def test_main_processing(self, mock_register, mock_load):
        """Test unified processing of seed items."""
        mock_load.return_value = [
            {
                "question": "Q1",
                "query": "SELECT 1",
                "expected_result": [{"a": 1}],
                "difficulty": "hard",
            }
        ]

        await cli._process_seed_data(Path("/app/queries"))

        mock_register.assert_awaited_once()
        call_kwargs = mock_register.call_args.kwargs
        assert call_kwargs["question"] == "Q1"
        assert call_kwargs["sql_query"] == "SELECT 1"
        assert call_kwargs["roles"] == ["example", "golden"]
        assert call_kwargs["status"] == "verified"
