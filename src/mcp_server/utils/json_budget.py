"""JSON payload size budgeting."""

import json
from typing import Any


class JSONBudget:
    """Tracks JSON payload size against a hard limit."""

    def __init__(self, max_bytes: int):
        """Initialize budget with max bytes limit."""
        self.max_bytes = max_bytes
        self.current_bytes = 0

    def consume(self, obj: Any) -> bool:
        """Consume budget for an object. Returns True if budget remains."""
        # Fast estimation: json dumps
        try:
            # separators=(",", ":") removes whitespace for compactness
            encoded = json.dumps(obj, default=str, separators=(",", ":")).encode("utf-8")
            size = len(encoded)
        except TypeError:
            size = len(str(obj).encode("utf-8"))

        # Add comma overhead if not first item (approx 1 byte)
        if self.current_bytes > 0:
            size += 1

        # Check against limit (leaving room for envelope overhead roughly)
        if self.current_bytes + size > self.max_bytes:
            return False

        self.current_bytes += size
        return True

    @property
    def remaining(self) -> int:
        """Return remaining budget in bytes."""
        return max(0, self.max_bytes - self.current_bytes)
