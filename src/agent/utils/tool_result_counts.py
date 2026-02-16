"""Helpers for extracting normalized item/row counts from tool results."""

from __future__ import annotations

import json
from typing import Any, Optional

_MAX_JSON_PARSE_BYTES = 1_000_000
_COUNT_KEYS = ("rows_returned", "items_returned", "returned_count", "page_size")


def _coerce_non_negative_int(value: Any) -> Optional[int]:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def _metadata_count(metadata: Any) -> Optional[int]:
    if not isinstance(metadata, dict):
        return None
    for key in _COUNT_KEYS:
        if key not in metadata:
            continue
        parsed = _coerce_non_negative_int(metadata.get(key))
        if parsed is not None:
            return parsed
    return None


def extract_items_count(envelope: Any) -> Optional[int]:
    """Extract item/row count from a tool envelope.

    Count precedence:
    1) explicit metadata counters (`rows_returned`, `items_returned`, `returned_count`, `page_size`)
    2) in-memory list sizes for `rows` or `result`
    3) in-memory top-level lists

    Returns ``None`` when no safe count is available.
    """
    if envelope is None:
        return None

    if isinstance(envelope, list):
        return len(envelope)

    payload = envelope
    allow_list_fallback = True
    if hasattr(payload, "model_dump"):
        try:
            payload = payload.model_dump()
        except Exception:
            return None
    elif isinstance(payload, str):
        # JSON strings are parsed for metadata counters, but we avoid list-size fallback
        # because materialized list lengths can be large and opaque at this layer.
        if len(payload.encode("utf-8")) > _MAX_JSON_PARSE_BYTES:
            return None
        try:
            payload = json.loads(payload)
        except Exception:
            return None
        allow_list_fallback = False

    if isinstance(payload, list):
        return len(payload) if allow_list_fallback else None
    if not isinstance(payload, dict):
        return None

    metadata_count = _metadata_count(payload.get("metadata"))
    if metadata_count is not None:
        return metadata_count

    if allow_list_fallback:
        rows = payload.get("rows")
        if isinstance(rows, list):
            return len(rows)
        result = payload.get("result")
        if isinstance(result, list):
            return len(result)

    return None
