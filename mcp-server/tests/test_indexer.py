"""Unit tests for schema indexer service."""

from unittest.mock import AsyncMock, patch

import pytest
from src.indexer import index_all_tables


class TestIndexAllTables:
    """Unit tests for index_all_tables function."""

    @pytest.mark.asyncio
    async def test_index_all_tables_success(self):
        """Test successful indexing of multiple tables."""
        mock_conn = AsyncMock()

        # Mock tables query
        mock_tables = [
            {"table_name": "actor"},
            {"table_name": "film"},
        ]
        mock_conn.fetch = AsyncMock(
            side_effect=[
                mock_tables,  # tables query
                [  # columns for actor
                    {"column_name": "actor_id", "data_type": "integer", "is_nullable": "NO"},
                    {"column_name": "first_name", "data_type": "text", "is_nullable": "NO"},
                ],
                [],  # foreign keys for actor
                [  # columns for film
                    {"column_name": "film_id", "data_type": "integer", "is_nullable": "NO"},
                    {"column_name": "title", "data_type": "text", "is_nullable": "NO"},
                ],
                [],  # foreign keys for film
            ]
        )
        mock_conn.execute = AsyncMock()

        # Mock embedding generation
        mock_embedding = [0.1] * 384

        with patch("src.indexer.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.indexer.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                with patch("src.indexer.RagEngine.embed_text", return_value=mock_embedding):
                    mock_get.return_value = mock_conn

                    await index_all_tables()

                    # Verify connection was acquired and released
                    mock_get.assert_called_once()
                    mock_release.assert_called_once_with(mock_conn)

                    # Verify tables query was executed
                    assert mock_conn.fetch.call_count == 5  # 1 tables + 2*2 (cols + fks)

                    # Verify execute was called twice (once per table)
                    assert mock_conn.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_index_all_tables_with_foreign_keys(self):
        """Test indexing table with foreign keys."""
        mock_conn = AsyncMock()

        mock_tables = [{"table_name": "payment"}]
        mock_conn.fetch = AsyncMock(
            side_effect=[
                mock_tables,  # tables query
                [  # columns
                    {"column_name": "payment_id", "data_type": "integer", "is_nullable": "NO"},
                    {"column_name": "customer_id", "data_type": "integer", "is_nullable": "NO"},
                ],
                [  # foreign keys
                    {
                        "column_name": "customer_id",
                        "foreign_table_name": "customer",
                        "foreign_column_name": "customer_id",
                    },
                ],
            ]
        )
        mock_conn.execute = AsyncMock()

        mock_embedding = [0.1] * 384

        with patch("src.indexer.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.indexer.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                with patch("src.indexer.RagEngine.embed_text", return_value=mock_embedding):
                    mock_get.return_value = mock_conn

                    await index_all_tables()

                    # Verify foreign keys were queried
                    fetch_calls = [call[0][0] for call in mock_conn.fetch.call_args_list]
                    assert any("FOREIGN KEY" in call for call in fetch_calls)

                    mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_index_all_tables_empty_database(self):
        """Test handling of empty database."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])  # No tables
        mock_conn.execute = AsyncMock()

        with patch("src.indexer.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.indexer.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                await index_all_tables()

                # Verify no execute calls (no tables to index)
                mock_conn.execute.assert_not_called()

                # Verify connection was still released
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_index_all_tables_on_conflict_update(self):
        """Test ON CONFLICT update behavior."""
        mock_conn = AsyncMock()

        mock_tables = [{"table_name": "actor"}]
        mock_conn.fetch = AsyncMock(
            side_effect=[
                mock_tables,
                [{"column_name": "actor_id", "data_type": "integer", "is_nullable": "NO"}],
                [],
            ]
        )
        mock_conn.execute = AsyncMock()

        mock_embedding = [0.1] * 384

        with patch("src.indexer.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.indexer.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                with patch("src.indexer.RagEngine.embed_text", return_value=mock_embedding):
                    mock_get.return_value = mock_conn

                    await index_all_tables()

                    # Verify upsert query contains ON CONFLICT
                    execute_call = mock_conn.execute.call_args[0][0]
                    assert "ON CONFLICT" in execute_call
                    assert "DO UPDATE SET" in execute_call

                    mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_index_all_tables_connection_cleanup(self):
        """Test connection is always released even on error."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Database error"))

        with patch("src.indexer.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.indexer.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                with pytest.raises(Exception):
                    await index_all_tables()

                # Connection should still be released
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_index_all_tables_embedding_generation(self):
        """Test that embeddings are generated correctly."""
        mock_conn = AsyncMock()

        mock_tables = [{"table_name": "test_table"}]
        mock_conn.fetch = AsyncMock(
            side_effect=[
                mock_tables,
                [{"column_name": "id", "data_type": "integer", "is_nullable": "NO"}],
                [],
            ]
        )
        mock_conn.execute = AsyncMock()

        mock_embedding = [0.5] * 384

        with patch("src.indexer.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.indexer.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                with patch(
                    "src.indexer.RagEngine.embed_text", return_value=mock_embedding
                ) as mock_embed:
                    with patch("src.indexer.format_vector_for_postgres") as mock_format:
                        mock_format.return_value = "[0.5,0.5,...]"
                        mock_get.return_value = mock_conn

                        await index_all_tables()

                        # Verify embedding was generated
                        mock_embed.assert_called_once()

                        # Verify vector was formatted
                        mock_format.assert_called_once_with(mock_embedding)

                        mock_release.assert_called_once_with(mock_conn)
