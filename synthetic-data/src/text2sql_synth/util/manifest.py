"""Manifest generation for synthetic data."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from text2sql_synth.config import SynthConfig
    from text2sql_synth.context import GenerationContext

from text2sql_synth import __version__
from text2sql_synth.util.hashing import stable_hash_str


def generate_manifest(
    ctx: GenerationContext,
    cfg: SynthConfig,
    files: list[dict[str, Any]],
) -> dict[str, Any]:
    """Generate a manifest dictionary for the generated data.

    Args:
        ctx: Generation context.
        cfg: Synth configuration.
        files: List of file metadata (name, format, rows, hash).

    Returns:
        Manifest dictionary.
    """
    # Calculate a content hash for the entire run
    # This hash should be deterministic based on the data produced
    # We use table names, row counts, and file hashes (if available)
    content_parts = []
    
    # Sort files by table name then format for stable hashing
    sorted_files = sorted(files, key=lambda x: (x["table"], x["format"]))
    
    for f in sorted_files:
        part = f"{f['table']}|{f['format']}|{f['rows']}"
        if f.get("hash"):
            part += f"|{f['hash']}"
        content_parts.append(part)
        
    content_hash = stable_hash_str(":".join(content_parts))

    return {
        "manifest_version": "1.0",
        "generator_version": __version__,
        "generation_timestamp": datetime.now().isoformat(),
        "content_hash": content_hash,
        "seed": cfg.seed,
        "schema_snapshot_id": ctx.schema_snapshot_id,
        "time_window": {
            "start": cfg.time_window.start_date.isoformat(),
            "end": cfg.time_window.end_date.isoformat(),
        },
        "config": cfg.to_dict(),
        "tables": {
            table_name: {
                "rows": len(df),
                "columns": list(df.columns),
            }
            # Use sorted keys for stable JSON if needed, though this is a dict
            for table_name, df in sorted(ctx.tables.items())
        },
        "files": sorted_files,
    }
