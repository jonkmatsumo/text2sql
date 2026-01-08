import json
from unittest.mock import MagicMock, patch

import pytest
from mcp_server.tools.semantic import get_semantic_subgraph


class TestGetSemanticSubgraph:
    """Unit tests for get_semantic_subgraph tool."""

    @pytest.mark.asyncio
    async def test_get_semantic_subgraph_success(self):
        """Test successful subgraph retrieval."""
        # Mock VectorIndexer
        mock_indexer = MagicMock()
        mock_indexer.search_nodes.side_effect = [
            # Table hits
            [{"node": {"name": "Table1", "id": "t1"}, "score": 0.9}],
            # Column hits
            [{"node": {"name": "Column1", "id": "c1"}, "score": 0.8}],
        ]

        # Mock Session and Result
        mock_session = MagicMock()
        mock_record = MagicMock()
        # Mocking the complex structure returned from the Cypher query
        # n, related, rels
        mock_n = MagicMock()
        mock_n.element_id = "t1"
        mock_n.labels = {"Table"}
        mock_n.__iter__.return_value = [("name", "Table1"), ("id", "t1")]

        mock_related_node = MagicMock()
        mock_related_node.element_id = "c1"
        mock_related_node.labels = {"Column"}
        mock_related_node.__iter__.return_value = [("name", "Column1"), ("id", "c1")]

        mock_rel = MagicMock()
        mock_rel.element_id = "r1"
        mock_rel.start_node.element_id = "t1"
        mock_rel.end_node.element_id = "c1"
        mock_rel.type = "HAS_COLUMN"
        mock_rel.__iter__.return_value = []

        mock_record.__getitem__.side_effect = lambda key: {
            "n": mock_n,
            "related": [mock_related_node],
            "rels": [[mock_rel]],
        }[key]

        mock_result = [mock_record]
        mock_session.run.return_value = mock_result
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = None

        mock_indexer.driver.session.return_value = mock_session

        with patch("mcp_server.tools.semantic.VectorIndexer", return_value=mock_indexer):
            result_json = await get_semantic_subgraph("query")
            result = json.loads(result_json)

            assert "nodes" in result
            assert "relationships" in result
            assert len(result["nodes"]) == 2
            assert len(result["relationships"]) == 1

            nodes = {n["id"]: n for n in result["nodes"]}
            assert "t1" in nodes
            assert "c1" in nodes
            assert nodes["t1"]["type"] == "Table"

            rel = result["relationships"][0]
            assert rel["source"] == "t1"
            assert rel["target"] == "c1"
            assert rel["type"] == "HAS_COLUMN"

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
