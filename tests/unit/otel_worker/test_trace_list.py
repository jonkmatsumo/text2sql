"""Unit tests for trace list helpers."""

import os
import unittest
from unittest.mock import MagicMock, patch

os.environ["POSTGRES_URL"] = "postgresql://user:pass@localhost/dbname"

from otel_worker.storage.postgres import list_traces  # noqa: E402


class TestTraceList(unittest.TestCase):
    """Ensure list_traces respects optional filters."""

    def test_duration_filters_included_in_query(self):
        """Duration bounds should appear in the SQL WHERE clause."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = []

        with patch("otel_worker.storage.postgres.engine", mock_engine):
            list_traces(duration_min_ms=150, duration_max_ms=500)
            executed_query = mock_conn.execute.call_args[0][0]
            executed_sql = str(executed_query)
            self.assertIn("duration_ms >= :duration_min_ms", executed_sql)
            self.assertIn("duration_ms <= :duration_max_ms", executed_sql)
