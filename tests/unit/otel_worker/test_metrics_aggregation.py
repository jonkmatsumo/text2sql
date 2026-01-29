"""Unit tests for metrics preview aggregation."""

import os
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# Set dummy URL before importing modules that initialize engines
os.environ["POSTGRES_URL"] = "postgresql://user:pass@localhost/dbname"

from otel_worker.storage.postgres import get_metrics_preview  # noqa: E402


class TestMetricsPreview(unittest.TestCase):
    """Tests for server-side metrics aggregation."""

    def test_get_metrics_preview_logic(self):
        """Test that get_metrics_preview calls engine with correct SQL."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Mock summary result
        mock_summary = MagicMock()
        mock_summary._mapping = {
            "total_count": 100,
            "error_count": 5,
            "avg_duration": 120.5,
            "p95_duration": 450.0,
        }

        # Mock timeseries result
        mock_ts = MagicMock()
        mock_ts._mapping = {
            "timestamp": datetime.now(timezone.utc),
            "count": 10,
            "error_count": 1,
            "avg_duration": 100.0,
        }

        mock_conn.execute.side_effect = [
            MagicMock(fetchone=lambda: mock_summary),  # First call: summary
            MagicMock(fetchall=lambda: [mock_ts]),  # Second call: timeseries
        ]

        with patch("otel_worker.storage.postgres.engine", mock_engine):
            result = get_metrics_preview(window_minutes=60)

            self.assertEqual(result["summary"]["total_count"], 100)
            self.assertEqual(len(result["timeseries"]), 1)
            self.assertEqual(result["window_minutes"], 60)

            # Verify SQL contained filtering
            calls = mock_conn.execute.call_args_list
            summary_sql = str(calls[0][0][0])
            self.assertIn("COUNT(*) as total_count", summary_sql)
            self.assertIn("start_time >= :start_time", summary_sql)

            ts_sql = str(calls[1][0][0])
            self.assertIn("GROUP BY 1 ORDER BY 1 ASC", ts_sql)

    def test_get_metrics_preview_empty(self):
        """Test aggregation when no data exists."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        mock_conn.execute.side_effect = [
            MagicMock(fetchone=lambda: None),  # summary
            MagicMock(fetchall=lambda: []),  # timeseries
        ]

        with patch("otel_worker.storage.postgres.engine", mock_engine):
            result = get_metrics_preview(window_minutes=15)
            self.assertEqual(result["summary"]["total_count"], 0)
            self.assertEqual(result["timeseries"], [])
