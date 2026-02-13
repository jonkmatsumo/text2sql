"""Bounded in-memory store for safe run decision summaries."""

from __future__ import annotations

import json
import threading
import time
from collections import OrderedDict
from typing import Any, Optional

from common.config.env import get_env_int

_DISALLOWED_FIELDS = {"current_sql", "query_result", "rows", "sql", "result"}


def _safe_env_int(name: str, default: int, minimum: int) -> int:
    try:
        value = get_env_int(name, default)
    except ValueError:
        value = default
    if value is None:
        value = default
    return max(minimum, int(value))


def _sanitize_summary(summary: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(summary, dict):
        return {}
    # JSON roundtrip for defensive deep-copy and serialization safety.
    payload = json.loads(json.dumps(summary, default=str))
    for key in list(payload.keys()):
        if key in _DISALLOWED_FIELDS:
            payload.pop(key, None)
    return payload


class RunSummaryStore:
    """Thread-safe bounded map keyed by run_id."""

    def __init__(self, *, max_size: int) -> None:
        """Initialize bounded storage with fixed max item count."""
        self._max_size = max(1, int(max_size))
        self._items: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._lock = threading.Lock()

    def record(
        self,
        *,
        run_id: str,
        summary: dict[str, Any],
        timestamp: Optional[float] = None,
    ) -> None:
        """Record or update a run summary by run_id."""
        if not run_id:
            return
        safe_summary = _sanitize_summary(summary)
        item = {
            "run_id": str(run_id),
            "timestamp": float(timestamp if timestamp is not None else time.time()),
            "summary": safe_summary,
        }
        with self._lock:
            if run_id in self._items:
                self._items.pop(run_id, None)
            self._items[run_id] = item
            while len(self._items) > self._max_size:
                self._items.popitem(last=False)

    def get(self, run_id: str) -> Optional[dict[str, Any]]:
        """Fetch one stored run summary by run_id."""
        if not run_id:
            return None
        with self._lock:
            item = self._items.get(run_id)
            if item is None:
                return None
            return json.loads(json.dumps(item, default=str))

    def list_recent(self, *, limit: Optional[int] = None) -> list[dict[str, Any]]:
        """List recent run summaries in reverse chronological order."""
        max_items = None if limit is None else max(0, int(limit))
        with self._lock:
            items = list(reversed(self._items.values()))
            if max_items is not None:
                items = items[:max_items]
            return json.loads(json.dumps(items, default=str))


_RUN_SUMMARY_STORE: Optional[RunSummaryStore] = None


def get_run_summary_store() -> RunSummaryStore:
    """Return singleton run summary store."""
    global _RUN_SUMMARY_STORE
    if _RUN_SUMMARY_STORE is None:
        _RUN_SUMMARY_STORE = RunSummaryStore(
            max_size=_safe_env_int("OPS_RUN_SUMMARY_BUFFER_SIZE", 200, minimum=1)
        )
    return _RUN_SUMMARY_STORE


def reset_run_summary_store() -> None:
    """Reset singleton run summary store (test helper)."""
    global _RUN_SUMMARY_STORE
    _RUN_SUMMARY_STORE = None
