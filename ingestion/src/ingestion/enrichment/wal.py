import json
import time


class WALManager:
    """Manages writing to the Enrichment Write-Ahead Log (WAL)."""

    def __init__(self, file_path: str = "enrichment_wal.jsonl"):
        """Initialize the WAL manager."""
        self.file_path = file_path

    def append_entry(self, node_id: str, description: str, new_hash: str) -> None:
        """Append a new enrichment entry to the log file."""
        entry = {
            "node_id": node_id,
            "description": description,
            "new_hash": new_hash,
            "timestamp": time.time(),
        }
        with open(self.file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
