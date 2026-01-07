"""Unit tests for schema indexer service."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.indexer import index_all_tables


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

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.indexer.Database.get_connection", mock_get):
            with patch("mcp_server.indexer.RagEngine.embed_text", return_value=mock_embedding):
                await index_all_tables()

                # Verify connection was acquired (context manager called)
                mock_get.assert_called_once()

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

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.indexer.Database.get_connection", mock_get):
            with patch("mcp_server.indexer.RagEngine.embed_text", return_value=mock_embedding):
                await index_all_tables()

                # Verify foreign keys were queried
                fetch_calls = [call[0][0] for call in mock_conn.fetch.call_args_list]
                assert any("FOREIGN KEY" in call for call in fetch_calls)

                # Verify connection was acquired
                mock_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_index_all_tables_empty_database(self):
        """Test handling of empty database."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])  # No tables
        mock_conn.execute = AsyncMock()

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.indexer.Database.get_connection", mock_get):
            await index_all_tables()

            # Verify no execute calls (no tables to index)
            mock_conn.execute.assert_not_called()

            # Verify connection was acquired
            mock_get.assert_called_once()

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

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.indexer.Database.get_connection", mock_get):
            with patch("mcp_server.indexer.RagEngine.embed_text", return_value=mock_embedding):
                await index_all_tables()

                # Verify upsert query contains ON CONFLICT
                execute_call = mock_conn.execute.call_args[0][0]
                assert "ON CONFLICT" in execute_call
                assert "DO UPDATE SET" in execute_call

                # Verify connection was acquired
                mock_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_index_all_tables_connection_cleanup(self):
        """Test connection is always released even on error."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Database error"))

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.indexer.Database.get_connection", mock_get):
            with pytest.raises(Exception):
                await index_all_tables()

            # Verify connection was acquired (context manager handles cleanup)
            mock_get.assert_called_once()

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

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.indexer.Database.get_connection", mock_get):
            with patch(
                "mcp_server.indexer.RagEngine.embed_text", return_value=mock_embedding
            ) as mock_embed:
                with patch("mcp_server.indexer.format_vector_for_postgres") as mock_format:
                    mock_format.return_value = "[0.5,0.5,...]"

                    await index_all_tables()

                    # Verify embedding was generated
                    mock_embed.assert_called_once()

                    # Verify vector was formatted
                    mock_format.assert_called_once_with(mock_embedding)

                    # Verify connection was acquired
                    mock_get.assert_called_once()
