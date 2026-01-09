import hashlib
import json
from typing import Any, Dict


def generate_canonical_hash(data: Dict[str, Any]) -> str:
    """Generate a deterministic SHA-256 hash for a dictionary.

    Keys are sorted to ensure determinism.
    """
    # sort_keys=True ensures that {"a": 1, "b": 2} and {"b": 2, "a": 1} produce
    # the same JSON string.
    serialized = json.dumps(data, sort_keys=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
