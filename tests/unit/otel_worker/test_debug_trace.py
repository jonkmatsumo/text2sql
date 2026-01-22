"""Tests for debug_trace.py tree reconstruction and formatting logic."""

import sys
from datetime import datetime, timezone
from pathlib import Path

# Add scripts directory to path for import
# Path: observability/otel-worker/tests/test_debug_trace.py -> scripts/
scripts_dir = Path(__file__).parent.parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))


class TestSpanParsing:
    """Test Span dataclass parsing from API response."""

    def test_span_from_dict_basic(self):
        """Test parsing a basic span dict."""
        from debug_trace import Span

        data = {
            "span_id": "abc123",
            "parent_span_id": "parent456",
            "name": "test_span",
            "start_time": "2026-01-17T10:00:00Z",
            "end_time": "2026-01-17T10:00:01Z",
            "duration_ms": 1000,
            "status_code": "STATUS_CODE_OK",
            "span_attributes": {},
        }
        span = Span.from_dict(data)
        assert span.span_id == "abc123"
        assert span.parent_span_id == "parent456"
        assert span.name == "test_span"
        assert span.duration_ms == 1000
        assert span.event_seq is None
        assert span.event_type is None

    def test_span_with_event_seq(self):
        """Test parsing span with event.seq attribute."""
        from debug_trace import Span

        data = {
            "span_id": "abc123",
            "parent_span_id": None,
            "name": "test_span",
            "start_time": "2026-01-17T10:00:00Z",
            "duration_ms": 100,
            "status_code": "STATUS_CODE_OK",
            "span_attributes": {"event.seq": "5", "event.type": "tool_call"},
        }
        span = Span.from_dict(data)
        assert span.event_seq == 5
        assert span.event_type == "tool_call"

    def test_span_root_has_no_parent(self):
        """Test root span has None parent_span_id."""
        from debug_trace import Span

        data = {
            "span_id": "root123",
            "name": "root_span",
            "start_time": "2026-01-17T10:00:00Z",
            "duration_ms": 100,
            "status_code": "STATUS_CODE_OK",
            "span_attributes": {},
        }
        span = Span.from_dict(data)
        assert span.parent_span_id is None


class TestTreeReconstruction:
    """Test span tree building logic."""

    def test_build_tree_groups_by_parent(self):
        """Test that build_tree groups spans by parent_span_id."""
        from debug_trace import Span, build_tree

        spans = [
            Span(
                span_id="root",
                parent_span_id=None,
                name="root",
                start_time=datetime(2026, 1, 17, 10, 0, 0, tzinfo=timezone.utc),
                end_time=None,
                duration_ms=100,
                status_code="OK",
                event_seq=None,
                event_type=None,
            ),
            Span(
                span_id="child1",
                parent_span_id="root",
                name="child1",
                start_time=datetime(2026, 1, 17, 10, 0, 1, tzinfo=timezone.utc),
                end_time=None,
                duration_ms=50,
                status_code="OK",
                event_seq=1,
                event_type=None,
            ),
            Span(
                span_id="child2",
                parent_span_id="root",
                name="child2",
                start_time=datetime(2026, 1, 17, 10, 0, 2, tzinfo=timezone.utc),
                end_time=None,
                duration_ms=30,
                status_code="OK",
                event_seq=2,
                event_type=None,
            ),
        ]
        tree = build_tree(spans)

        assert None in tree  # Root spans
        assert "root" in tree  # Children of root
        assert len(tree[None]) == 1
        assert len(tree["root"]) == 2


class TestSortingLogic:
    """Test span sorting by start_time and event.seq."""

    def test_sort_by_start_time(self):
        """Test spans are sorted by start_time."""
        from debug_trace import Span, sort_key

        span1 = Span(
            span_id="1",
            parent_span_id=None,
            name="first",
            start_time=datetime(2026, 1, 17, 10, 0, 0, tzinfo=timezone.utc),
            end_time=None,
            duration_ms=100,
            status_code="OK",
            event_seq=None,
            event_type=None,
        )
        span2 = Span(
            span_id="2",
            parent_span_id=None,
            name="second",
            start_time=datetime(2026, 1, 17, 10, 0, 1, tzinfo=timezone.utc),
            end_time=None,
            duration_ms=100,
            status_code="OK",
            event_seq=None,
            event_type=None,
        )
        assert sort_key(span1) < sort_key(span2)

    def test_sort_by_event_seq_when_same_time(self):
        """Test spans with same start_time are sorted by event.seq."""
        from debug_trace import Span, sort_key

        same_time = datetime(2026, 1, 17, 10, 0, 0, tzinfo=timezone.utc)
        span1 = Span(
            span_id="1",
            parent_span_id=None,
            name="first",
            start_time=same_time,
            end_time=None,
            duration_ms=100,
            status_code="OK",
            event_seq=1,
            event_type=None,
        )
        span2 = Span(
            span_id="2",
            parent_span_id=None,
            name="second",
            start_time=same_time,
            end_time=None,
            duration_ms=100,
            status_code="OK",
            event_seq=2,
            event_type=None,
        )
        assert sort_key(span1) < sort_key(span2)

    def test_null_event_seq_treated_as_zero(self):
        """Test that None event_seq is treated as 0 for sorting."""
        from debug_trace import Span, sort_key

        same_time = datetime(2026, 1, 17, 10, 0, 0, tzinfo=timezone.utc)
        span_null = Span(
            span_id="1",
            parent_span_id=None,
            name="null_seq",
            start_time=same_time,
            end_time=None,
            duration_ms=100,
            status_code="OK",
            event_seq=None,
            event_type=None,
        )
        span_one = Span(
            span_id="2",
            parent_span_id=None,
            name="seq_one",
            start_time=same_time,
            end_time=None,
            duration_ms=100,
            status_code="OK",
            event_seq=1,
            event_type=None,
        )
        assert sort_key(span_null) < sort_key(span_one)
