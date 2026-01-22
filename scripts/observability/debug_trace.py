#!/usr/bin/env python3
"""Debug script to visualize trace spans as an indented tree timeline.

This script fetches spans from the OTEL Worker API and reconstructs
the parent/child tree, then prints an indented timeline ordered by
start_time and event.seq.

Usage:
    python scripts/debug_trace.py --trace-id <trace_id>
    python scripts/debug_trace.py --trace-id <trace_id> --api-url http://localhost:4320

Example:
    python scripts/debug_trace.py --trace-id abc123def456
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any

try:
    import requests
except ImportError:
    print("Error: requests library required. Install with: pip install requests")
    sys.exit(1)


@dataclass
class Span:
    """Represents a single span from the trace."""

    span_id: str
    parent_span_id: str | None
    name: str
    start_time: datetime
    end_time: datetime | None
    duration_ms: int
    status_code: str
    event_seq: int | None
    event_type: str | None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Span:
        """Create a Span from API response dict."""
        attrs = data.get("span_attributes", {}) or {}
        event_seq_str = attrs.get("event.seq")
        event_seq = int(event_seq_str) if event_seq_str else None

        start_time = datetime.fromisoformat(data["start_time"].replace("Z", "+00:00"))
        end_time = None
        if data.get("end_time"):
            end_time = datetime.fromisoformat(data["end_time"].replace("Z", "+00:00"))

        return cls(
            span_id=data["span_id"],
            parent_span_id=data.get("parent_span_id"),
            name=data.get("name", "unknown"),
            start_time=start_time,
            end_time=end_time,
            duration_ms=data.get("duration_ms", 0),
            status_code=data.get("status_code", "UNSET"),
            event_seq=event_seq,
            event_type=attrs.get("event.type"),
        )


def fetch_spans(trace_id: str, api_url: str) -> list[dict]:
    """Fetch spans for a trace from OTEL Worker API."""
    url = f"{api_url}/api/v1/traces/{trace_id}/spans?include=attributes"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("items", [])
    except requests.RequestException as e:
        print(f"Error fetching spans: {e}")
        sys.exit(1)


def build_tree(spans: list[Span]) -> dict[str | None, list[Span]]:
    """Build a mapping of parent_span_id -> list of child spans."""
    tree = defaultdict(list)
    for span in spans:
        tree[span.parent_span_id].append(span)
    return tree


def sort_key(span: Span) -> tuple:
    """Sort key for spans: (start_time, event_seq or 0)."""
    return (span.start_time, span.event_seq or 0)


def print_tree(
    tree: dict[str | None, list[Span]],
    parent_id: str | None = None,
    depth: int = 0,
    trace_start: datetime | None = None,
) -> None:
    """Recursively print the span tree with indentation."""
    children = sorted(tree.get(parent_id, []), key=sort_key)

    for span in children:
        indent = "  " * depth
        offset_ms = 0
        if trace_start:
            offset_ms = int((span.start_time - trace_start).total_seconds() * 1000)

        status_icon = (
            "✓"
            if span.status_code == "STATUS_CODE_OK"
            else "✗" if "ERROR" in span.status_code else "○"
        )
        seq_str = f"[seq:{span.event_seq}]" if span.event_seq is not None else ""
        type_str = f"({span.event_type})" if span.event_type else ""

        print(
            f"{indent}{status_icon} {span.name} {type_str}{seq_str} "
            f"+{offset_ms}ms [{span.duration_ms}ms]"
        )

        print_tree(tree, span.span_id, depth + 1, trace_start)


def main() -> None:
    """Run the trace visualization script."""
    parser = argparse.ArgumentParser(
        description="Visualize trace spans as an indented tree timeline."
    )
    parser.add_argument(
        "--trace-id",
        required=True,
        help="Trace ID to fetch and visualize",
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:4320",
        help="OTEL Worker API base URL (default: http://localhost:4320)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw JSON instead of formatted tree",
    )
    args = parser.parse_args()

    print(f"Fetching trace {args.trace_id} from {args.api_url}...")
    raw_spans = fetch_spans(args.trace_id, args.api_url)

    if not raw_spans:
        print("No spans found for this trace.")
        return

    if args.json:
        print(json.dumps(raw_spans, indent=2, default=str))
        return

    spans = [Span.from_dict(s) for s in raw_spans]
    tree = build_tree(spans)

    # Find trace start time
    trace_start = min(s.start_time for s in spans)

    print(f"\n{'='*60}")
    print(f"Trace: {args.trace_id}")
    print(f"Spans: {len(spans)}")
    print(f"Start: {trace_start.isoformat()}")
    print(f"{'='*60}\n")

    print_tree(tree, parent_id=None, depth=0, trace_start=trace_start)
    print()


if __name__ == "__main__":
    main()
