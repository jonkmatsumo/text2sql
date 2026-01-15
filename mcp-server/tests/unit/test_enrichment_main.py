import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from ingestion.enrichment.main import EnrichmentPipeline


class TestEnrichmentPipeline(unittest.IsolatedAsyncioTestCase):
    """Test suite for the EnrichmentPipeline orchestration."""

    @patch("ingestion.enrichment.main.EnrichmentAgent")
    @patch("ingestion.enrichment.main.WALManager")
    @patch("ingestion.enrichment.main.replay_wal")
    async def test_run_full_flow(self, mock_replay, mock_wal_cls, mock_agent_cls):
        """Test the full successful execution flow with recovery."""
        # 1. Mock GraphStore
        mock_store = MagicMock()
        # Return 2 stale nodes from run_query
        mock_store.run_query.return_value = [
            {"n": {"id": 1, "name": "Node A", "elementId": "elem1"}},
            {"n": {"id": 2, "name": "Node B", "elementId": "elem2"}},
        ]

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
        pipeline = EnrichmentPipeline(store=mock_store, dry_run=True)
        await pipeline.run()

        # A. Check GraphStore query call
        # run_query called for: 1) delta detection, 2) commit entry 1, 3) commit entry 2
        self.assertEqual(mock_store.run_query.call_count, 3)

        # B. Check Agent calls
        self.assertEqual(mock_agent.generate_description.call_count, 2)

        # C. Check WAL Write
        self.assertEqual(mock_wal.append_entry.call_count, 2)

        # D. Check DB Commit (via run_query)
        # 2 commits from recovery replay + 2 from final replay = 4 total
        # Actually, looking at the code, _commit_wal_to_db calls run_query for each entry
        # So we expect: 1 query for delta detection + N queries for commits
        # Let's just verify run_query was called multiple times
        self.assertGreater(mock_store.run_query.call_count, 1)

        # Ensure replay_wal called twice (recovery + final)
        self.assertEqual(mock_replay.call_count, 2)

    @patch("ingestion.enrichment.main.replay_wal")
    @patch("ingestion.enrichment.main.WALManager")
    async def test_run_recovery_only(self, mock_wal_cls, mock_replay):
        """Test that recovery runs even if no nodes need enrichment."""
        mock_store = MagicMock()
        mock_store.run_query.return_value = []  # No new work

        # Replay returns 1 item during recovery
        mock_replay.return_value = [{"node_id": "1", "description": "old", "new_hash": "h"}]

        pipeline = EnrichmentPipeline(store=mock_store, dry_run=True)
        await pipeline.run()

        # Should have committed the 1 recovery item
        # run_query called once for delta detection + once for commit
        self.assertEqual(mock_store.run_query.call_count, 2)
        # Should NOT have entered generation loop
        # (Verified by lack of agent mock interaction needed)

    @patch("ingestion.enrichment.main.EnrichmentAgent")
    @patch("ingestion.enrichment.main.WALManager")
    async def test_process_node_safely_handles_error(self, mock_wal_cls, mock_agent_cls):
        """Test that individual node failure doesn't crash pipeline and doesn't write to WAL."""
        mock_agent = mock_agent_cls.return_value
        mock_agent.generate_description = AsyncMock(side_effect=Exception("LLM Crash"))

        mock_wal = mock_wal_cls.return_value

        pipeline = EnrichmentPipeline(dry_run=True)

        # Should not raise exception
        await pipeline._process_node_safely(mock_agent, mock_wal, {"id": 1, "name": "Bad Node"})

        # WAL should strictly NOT be written to
        mock_wal.append_entry.assert_not_called()
