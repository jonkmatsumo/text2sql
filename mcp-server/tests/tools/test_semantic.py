"""Tests for semantic subgraph retrieval tool."""

import json
from unittest.mock import MagicMock, patch

import pytest
from mcp_server.tools.semantic import get_semantic_subgraph


class TestGetSemanticSubgraph:
    """Unit tests for get_semantic_subgraph tool."""

    @pytest.mark.asyncio
    async def test_get_semantic_subgraph_success(self):
        """Test successful subgraph retrieval with 2-step strategy."""
        mock_indexer = MagicMock()

        # Tables-first: returns table hits
        mock_indexer.search_nodes.return_value = [{"node": {"name": "customers"}, "score": 0.9}]

        # Mock session
        mock_session = MagicMock()
        mock_indexer.driver.session.return_value = mock_session

        # Mock result for Step 1 (Tables/Columns)
        mock_table = MagicMock()
        mock_table.element_id = "t1"
        mock_table.get = lambda k, d=None: {"name": "customers"}.get(k, d)
        mock_table.__iter__ = lambda s: iter([("name", "customers")])

        mock_col = MagicMock()
        mock_col.element_id = "c1"
        mock_col.get = lambda k, d=None: {"name": "customer_id", "type": "integer"}.get(k, d)
        mock_col.__iter__ = lambda s: iter([("name", "customer_id"), ("type", "integer")])

        record1 = {"t": mock_table, "columns": [mock_col]}

        # Mock result for Step 2 (Join Discovery)
        # source_col --FK--> target_col <--HAS_COLUMN-- target_table
        mock_sc = MagicMock()
        mock_sc.element_id = "c1"  # Matches customer_id above

        mock_tc = MagicMock()
        mock_tc.element_id = "c2"  # order's customer_id

        mock_rt = MagicMock()
        mock_rt.element_id = "t2"
        mock_rt.get = lambda k, d=None: {"name": "orders"}.get(k, d)
        mock_rt.__iter__ = lambda s: iter([("name", "orders")])

        record2 = {"source_col": mock_sc, "target_table": mock_rt, "target_col": mock_tc}

        # Setup side_effect for session.run to return different results for step 1 and 2
        mock_session.run.side_effect = [
            [record1],  # Step 1
            [record2],  # Step 2
        ]

        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = None

        with patch("mcp_server.tools.semantic.VectorIndexer", return_value=mock_indexer):
            result_json = await get_semantic_subgraph("find customers")
            result = json.loads(result_json)

            assert "nodes" in result
            assert "relationships" in result
            assert len(result["nodes"]) >= 1

            # Check for join relationships
            rels = result["relationships"]
            has_fk = any(r["type"] == "FOREIGN_KEY_TO" for r in rels)

            # Note: c1 and c2 are in graph. t1 and t2 are in graph.
            # Step 1 added t1, c1. Step 2 added t2, c2 (bridge).
            assert has_fk

    @pytest.mark.asyncio
    async def test_get_semantic_subgraph_no_seeds(self):
        """Test handling no search results."""
        mock_indexer = MagicMock()
        mock_indexer.search_nodes.return_value = []

        with patch("mcp_server.tools.semantic.VectorIndexer", return_value=mock_indexer):
            result = await get_semantic_subgraph("query")
            data = json.loads(result)
            assert data["nodes"] == []
            assert data["relationships"] == []

    @pytest.mark.asyncio
    async def test_get_semantic_subgraph_error(self):
        """Test error handling."""
        mock_indexer = MagicMock()
        mock_indexer.search_nodes.side_effect = Exception("Search failed")

        with patch("mcp_server.tools.semantic.VectorIndexer", return_value=mock_indexer):
            result = await get_semantic_subgraph("query")
            data = json.loads(result)
            assert "error" in data
            assert "Search failed" in data["error"]

    @pytest.mark.asyncio
    async def test_tables_first_strategy(self):
        """Test that tables are searched before columns."""
        mock_indexer = MagicMock()

        # Tables-first should call search with label="Table" first
        call_order = []

        def track_calls(query_text, label, k, apply_threshold=True):
            call_order.append(label)
            return []

        mock_indexer.search_nodes.side_effect = track_calls

        with patch("mcp_server.tools.semantic.VectorIndexer", return_value=mock_indexer):
            await get_semantic_subgraph("test query")

            # Should search tables first
            assert call_order[0] == "Table"
