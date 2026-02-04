"""Unit tests for exclusive span duration computation."""

import unittest
from datetime import datetime, timezone

from otel_worker.storage.postgres import _compute_self_time_map


class TestSelfTimeComputation(unittest.TestCase):
    """Tests for exclusive span duration calculation."""

    def test_self_time_with_children(self):
        """Children overlap should be subtracted from parent duration."""
        parent = {
            "span_id": "p",
            "parent_span_id": None,
            "start_time": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "end_time": datetime(2024, 1, 1, 0, 0, 0, 100000, tzinfo=timezone.utc),
            "duration_ms": 100,
        }
        child1 = {
            "span_id": "c1",
            "parent_span_id": "p",
            "start_time": datetime(2024, 1, 1, 0, 0, 0, 10000, tzinfo=timezone.utc),
            "end_time": datetime(2024, 1, 1, 0, 0, 0, 30000, tzinfo=timezone.utc),
            "duration_ms": 20,
        }
        child2 = {
            "span_id": "c2",
            "parent_span_id": "p",
            "start_time": datetime(2024, 1, 1, 0, 0, 0, 40000, tzinfo=timezone.utc),
            "end_time": datetime(2024, 1, 1, 0, 0, 0, 70000, tzinfo=timezone.utc),
            "duration_ms": 30,
        }
        spans = [parent, child1, child2]
        result = _compute_self_time_map(spans)
        self.assertEqual(result["p"], 50)

    def test_self_time_without_children(self):
        """Spans without children should keep full duration."""
        span = {
            "span_id": "solo",
            "parent_span_id": None,
            "start_time": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "end_time": datetime(2024, 1, 1, 0, 0, 0, 50000, tzinfo=timezone.utc),
            "duration_ms": 50,
        }
        result = _compute_self_time_map([span])
        self.assertEqual(result["solo"], 50)
