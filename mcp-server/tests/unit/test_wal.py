import os
import tempfile
import unittest

from mcp_server.dal.ingestion.enrichment.loader import replay_wal
from mcp_server.dal.ingestion.enrichment.wal import WALManager


class TestWAL(unittest.TestCase):
    """Test suite for Write-Ahead Log (WAL) functionality."""

    def setUp(self):
        """Create a temporary file for testing."""
        self.temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.temp_file.close()  # Close so we can open it in the manager
        self.wal_path = self.temp_file.name

    def tearDown(self):
        """Remove the temporary file."""
        if os.path.exists(self.wal_path):
            os.remove(self.wal_path)

    def test_wal_write_and_read(self):
        """Test writing multiple records and reading them back in order."""
        manager = WALManager(file_path=self.wal_path)

        # Write 3 dummy records
        entries = [
            ("node1", "desc1", "hash1"),
            ("node2", "desc2", "hash2"),
            ("node3", "desc3", "hash3"),
        ]

        for nid, desc, h in entries:
            manager.append_entry(nid, desc, h)

        # Read them back
        replayed = list(replay_wal(file_path=self.wal_path))

        # Verify count
        self.assertEqual(len(replayed), 3)

        # Verify order and content
        for i, (nid, desc, h) in enumerate(entries):
            self.assertEqual(replayed[i]["node_id"], nid)
            self.assertEqual(replayed[i]["description"], desc)
            self.assertEqual(replayed[i]["new_hash"], h)
            self.assertIn("timestamp", replayed[i])

    def test_replay_missing_file(self):
        """Test replaying a non-existent file yields nothing."""
        # Use a path guaranteed not to exist
        missing_path = self.wal_path + "_missing"
        replayed = list(replay_wal(file_path=missing_path))
        self.assertEqual(len(replayed), 0)
