from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from ingestion.example_loader import ExampleLoader


class TestExampleLoader:
    """Test suite for ExampleLoader."""

    @pytest.mark.asyncio
    async def test_load_examples(self):
        """Test loading examples from store and adding to index."""
        mock_store = AsyncMock()
        mock_examples = [
            MagicMock(id=1, question="Q1", sql_query="SELECT 1", embedding=[0.1, 0.2]),
            MagicMock(id=2, question="Q2", sql_query="SELECT 2", embedding=[0.3, 0.4]),
        ]
        mock_store.fetch_all_examples.return_value = mock_examples

        mock_index = MagicMock()

        with patch(
            "ingestion.example_loader.Database.get_example_store",
            return_value=mock_store,
        ):
            loader = ExampleLoader()
            await loader.load_examples(mock_index)

            mock_index.add_items.assert_called_once()
            args, kwargs = mock_index.add_items.call_args

            vectors = args[0]
            ids = args[1]
            metadata = kwargs.get("metadata")

            assert len(ids) == 2
            assert ids == [1, 2]
            assert isinstance(vectors, np.ndarray)
            assert vectors.shape == (2, 2)
            assert metadata[1]["question"] == "Q1"
            assert metadata[2]["sql"] == "SELECT 2"

    @pytest.mark.asyncio
    async def test_load_examples_empty(self):
        """Test handling empty store results."""
        mock_store = AsyncMock()
        mock_store.fetch_all_examples.return_value = []

        mock_index = MagicMock()

        with patch(
            "ingestion.example_loader.Database.get_example_store",
            return_value=mock_store,
        ):
            loader = ExampleLoader()
            await loader.load_examples(mock_index)

            mock_index.add_items.assert_not_called()
