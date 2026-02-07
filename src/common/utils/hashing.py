"""Stable hashing utilities."""

import hashlib
import json
from typing import Any


def canonical_json_hash(obj: Any) -> str:
    """Compute stable SHA256 hash of a JSON-serializable object.

    Uses sort_keys=True for deterministic ordering of dict keys.
    """
    try:
        # separators=(",", ":") removes whitespace for compactness and stability
        encoded = json.dumps(obj, sort_keys=True, default=str, separators=(",", ":")).encode(
            "utf-8"
        )
    except TypeError:
        # Fallback for non-serializable objects
        encoded = str(obj).encode("utf-8")

    return hashlib.sha256(encoded).hexdigest()
