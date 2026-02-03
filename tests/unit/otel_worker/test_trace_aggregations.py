"""Unit tests for trace aggregation helpers."""

import os
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

os.environ["POSTGRES_URL"] = "postgresql://user:pass@localhost/dbname"

from otel_worker.storage.postgres import (  # noqa: E402
    _build_trace_filter_clause,
    _compute_histogram_bins,
    _compute_percentiles,
    compute_trace_aggregations,
)


class TestTraceAggregations(unittest.TestCase):
    """Tests for trace aggregation helper utilities."""

    def test_filter_clause_builder(self):
        """Ensure filter clause builder adds expected predicates."""
        where, params = _build_trace_filter_clause(
            service="api",
            status="ERROR",
            has_errors="yes",
            duration_min_ms=100,
        )
        self.assertIn("service_name = :service", where)
        self.assertIn("status = :status", where)
        self.assertIn("error_count > 0", where)
        self.assertIn("duration_ms >= :duration_min_ms", where)
        self.assertEqual(params["service"], "api")
        self.assertEqual(params["status"], "ERROR")
        self.assertEqual(params["duration_min_ms"], 100)

    def test_histogram_bins_deterministic(self):
        """Histogram bins should be deterministic and cover all values."""
        bins = _compute_histogram_bins([10, 20, 30, 40], bin_count=2)
        self.assertEqual(len(bins), 2)
        self.assertEqual(sum(b["count"] for b in bins), 4)
        self.assertLess(bins[0]["start_ms"], bins[0]["end_ms"])

    def test_percentiles(self):
        """Percentile helper should return expected values."""
        values = [10, 20, 30, 40, 50]
        percentiles = _compute_percentiles(values)
        self.assertEqual(percentiles["p50_ms"], 30)
        self.assertEqual(percentiles["p95_ms"], 50)
        self.assertEqual(percentiles["p99_ms"], 50)

    def test_compute_trace_aggregations(self):
        """Aggregation helper should combine SQL results into response shape."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        mock_conn.execute.side_effect = [
            MagicMock(fetchone=lambda: (5,)),  # total_count
            MagicMock(fetchall=lambda: [("svc", 3)]),  # service counts
            MagicMock(fetchall=lambda: [("OK", 4), ("ERROR", 1)]),  # status counts
            MagicMock(fetchone=lambda: (1, 4)),  # error counts
            MagicMock(fetchall=lambda: [(100,), (200,), (300,)]),  # durations
        ]

        with patch("otel_worker.storage.postgres.engine", mock_engine):
            result = compute_trace_aggregations(
                service="svc",
                start_time_gte=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )

            self.assertEqual(result["total_count"], 5)
            self.assertEqual(result["facet_counts"]["service"]["svc"], 3)
            self.assertEqual(result["facet_counts"]["status"]["ok"], 4)
            self.assertEqual(result["facet_counts"]["error"]["has_errors"], 1)
            self.assertEqual(len(result["duration_histogram"]), 20)
            self.assertIn("p50_ms", result["percentiles"])
