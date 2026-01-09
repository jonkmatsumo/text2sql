import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from mcp_server.graph_ingestion.enrichment.main import EnrichmentPipeline


@patch.dict(os.environ, {"ENABLE_LLM_ENRICHMENT": "true"})
class TestEnrichmentPipeline(unittest.IsolatedAsyncioTestCase):
    """Test suite for the EnrichmentPipeline orchestration."""

    @patch("mcp_server.graph_ingestion.enrichment.main.GraphDatabase")
    @patch("mcp_server.graph_ingestion.enrichment.main.get_nodes_needing_enrichment")
    @patch("mcp_server.graph_ingestion.enrichment.main.EnrichmentAgent")
    @patch("mcp_server.graph_ingestion.enrichment.main.WALManager")
    @patch("mcp_server.graph_ingestion.enrichment.main.replay_wal")
    async def test_run_full_flow(
        self, mock_replay, mock_wal_cls, mock_agent_cls, mock_get_nodes, mock_gdb
    ):
        """Test the full successful execution flow with recovery."""
        # 1. Mock DB Connection & node retrieval
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        mock_driver.session.return_value.__enter__.return_value = mock_session

        # Return 2 stale nodes
        mock_get_nodes.return_value = [
            {"id": 1, "name": "Node A", "elementId": "elem1"},
            {"id": 2, "name": "Node B", "elementId": "elem2"},
        ]
        mock_session.execute_read.return_value = mock_get_nodes.return_value

        # 2. Mock Agent generation
        mock_agent = mock_agent_cls.return_value
        mock_agent.generate_description = AsyncMock(side_effect=["Desc A", "Desc B"])

        # 3. Mock WAL
        mock_wal = mock_wal_cls.return_value
        mock_wal.file_path = "dummy.jsonl"

        # 4. Mock Replay (Sink inputs)
        # Replay called twice:
        # First call (Recovery): Empty (or existing)
        # Second call (Final Commit): The new entries
        mock_replay.side_effect = [
            [],  # Start (Recovery)
            [  # End (Final)
                {"node_id": "elem1", "description": "Desc A", "new_hash": "hash1"},
                {"node_id": "elem2", "description": "Desc B", "new_hash": "hash2"},
            ],
        ]

        # Execute
        pipeline = EnrichmentPipeline()
        await pipeline.run()

        # Verifications
        # A. Check DB Fetch
        mock_session.execute_read.assert_called_with(mock_get_nodes)

        # B. Check Agent calls
        self.assertEqual(mock_agent.generate_description.call_count, 2)

        # C. Check WAL Write
        self.assertEqual(mock_wal.append_entry.call_count, 2)

        # D. Check DB Commit (executed via _commit_entry inside the loop)
        # We expect 2 commit calls in total (from the second replay)
        self.assertEqual(mock_session.execute_write.call_count, 2)

        # Ensure replay_wal called twice (recovery + final)
        self.assertEqual(mock_replay.call_count, 2)

    @patch("mcp_server.graph_ingestion.enrichment.main.GraphDatabase")
    @patch("mcp_server.graph_ingestion.enrichment.main.get_nodes_needing_enrichment")
    @patch("mcp_server.graph_ingestion.enrichment.main.replay_wal")
    @patch("mcp_server.graph_ingestion.enrichment.main.WALManager")
    async def test_run_recovery_only(self, mock_wal_cls, mock_replay, mock_get_nodes, mock_gdb):
        """Test that recovery runs even if no nodes need enrichment."""
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_gdb.driver.return_value = mock_driver
        mock_driver.session.return_value.__enter__.return_value = mock_session

        mock_get_nodes.return_value = []  # No new work
        mock_session.execute_read.return_value = []

        # Replay returns 1 item during recovery
        mock_replay.return_value = [{"node_id": "1", "description": "old", "new_hash": "h"}]

        pipeline = EnrichmentPipeline()
        await pipeline.run()

        # Should have committed the 1 recovery item
        self.assertEqual(mock_session.execute_write.call_count, 1)
        # Should NOT have entered generation loop
        # (Verified by lack of agent mock interaction needed)

    @patch("mcp_server.graph_ingestion.enrichment.main.EnrichmentAgent")
    @patch("mcp_server.graph_ingestion.enrichment.main.WALManager")
    async def test_process_node_safely_handles_error(self, mock_wal_cls, mock_agent_cls):
        """Test that individual node failure doesn't crash pipeline and doesn't write to WAL."""
        mock_agent = mock_agent_cls.return_value
        mock_agent.generate_description = AsyncMock(side_effect=Exception("LLM Crash"))

        mock_wal = mock_wal_cls.return_value

        pipeline = EnrichmentPipeline()

        # Should not raise exception
        await pipeline._process_node_safely(mock_agent, mock_wal, {"id": 1, "name": "Bad Node"})

        # WAL should strictly NOT be written to
        mock_wal.append_entry.assert_not_called()
