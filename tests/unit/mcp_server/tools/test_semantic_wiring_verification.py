from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mcp_server.tools.get_semantic_subgraph import handler as get_semantic_subgraph


class TestSemanticWiringVerification:
    """Verification tests for semantic subgraph wiring."""

    @pytest.mark.asyncio
    async def test_seed_selection_routes_through_ann_indexer(self):
        """Verify seed selection uses VectorIndexer.search_nodes (ANN)."""
        mock_indexer = MagicMock()
        mock_indexer.search_nodes_with_metadata = AsyncMock(
            return_value=([{"node": {"name": "customers"}, "score": 0.9}], {"threshold": 0.0})
        )

        mock_store = MagicMock()

        # Mock Introspector to minimal success so traversal doesn't crash
        mock_table_def = MagicMock()
        mock_table_def.description = "test table"
        mock_table_def.columns = []
        mock_table_def.foreign_keys = []

        mock_introspector = MagicMock()
        mock_introspector.get_table_def = AsyncMock(return_value=mock_table_def)

        with (
            patch(
                "mcp_server.tools.get_semantic_subgraph.Database.get_graph_store",
                return_value=mock_store,
            ),
            patch(
                "mcp_server.tools.get_semantic_subgraph.Database.get_schema_introspector",
                return_value=mock_introspector,
            ),
            patch(
                "mcp_server.tools.get_semantic_subgraph.VectorIndexer", return_value=mock_indexer
            ) as MockVectorIndexer,
        ):

            await get_semantic_subgraph("find customers")

            # Verify VectorIndexer was initialized
            MockVectorIndexer.assert_called_with(store=mock_store)

            # Verify search_nodes_with_metadata was called (Tables first)
            mock_indexer.search_nodes_with_metadata.assert_called()
            call_args = mock_indexer.search_nodes_with_metadata.call_args_list[0]
            assert call_args.args[0] == "find customers"
            assert call_args.kwargs.get("label") == "Table"

            # Verify we did NOT fall back to columns (since tables returned valid hits)
            # This confirms tables-first logic is preserved
            assert mock_indexer.search_nodes_with_metadata.call_count == 1

    @pytest.mark.asyncio
    async def test_column_fallback_wiring(self):
        """Verify fallback to Columns uses VectorIndexer as well."""
        mock_indexer = MagicMock()
        # First call (Tables) returns empty, Second call (Columns) returns hits
        mock_indexer.search_nodes_with_metadata = AsyncMock(
            side_effect=[
                ([], {"threshold": 0.0}),
                ([{"node": {"name": "c1", "table": "t1"}, "score": 0.8}], {"threshold": 0.0}),
            ]
        )

        mock_store = MagicMock()

        mock_table_def = MagicMock()
        mock_table_def.description = "test table"
        mock_table_def.columns = []
        mock_table_def.foreign_keys = []

        mock_introspector = MagicMock()
        mock_introspector.get_table_def = AsyncMock(return_value=mock_table_def)

        with (
            patch(
                "mcp_server.tools.get_semantic_subgraph.Database.get_graph_store",
                return_value=mock_store,
            ),
            patch(
                "mcp_server.tools.get_semantic_subgraph.Database.get_schema_introspector",
                return_value=mock_introspector,
            ),
            patch(
                "mcp_server.tools.get_semantic_subgraph.VectorIndexer", return_value=mock_indexer
            ),
        ):

            await get_semantic_subgraph("find customers")

            assert mock_indexer.search_nodes_with_metadata.call_count == 2

            # 1. Table search
            args1 = mock_indexer.search_nodes_with_metadata.call_args_list[0]
            assert args1.kwargs.get("label") == "Table"

            # 2. Column search
            args2 = mock_indexer.search_nodes_with_metadata.call_args_list[1]
            assert args2.kwargs.get("label") == "Column"
