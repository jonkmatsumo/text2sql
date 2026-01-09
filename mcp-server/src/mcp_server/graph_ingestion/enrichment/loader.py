import json
import os
from typing import Any, Dict, Iterator


def replay_wal(file_path: str = "enrichment_wal.jsonl") -> Iterator[Dict[str, Any]]:
    """Replay valid JSON entries from the WAL file."""
    if not os.path.exists(file_path):
        return

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue
