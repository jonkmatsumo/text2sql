"""Output serialization for generated data."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from text2sql_synth.config import SynthConfig
    from text2sql_synth.context import GenerationContext

from text2sql_synth.util.manifest import generate_manifest

logger = logging.getLogger(__name__)


def _calculate_file_hash(file_path: Path) -> str:
    """Calculate SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # Read in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def export_to_directory(
    ctx: GenerationContext,
    cfg: SynthConfig,
    out_dir: str | Path,
) -> Path:
    """Export all tables in context to a directory.

    Args:
        ctx: Generation context with tables.
        cfg: Configuration with output options.
        out_dir: Base directory for output.

    Returns:
        Path to the generated manifest file.
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    csv_dir = out_path / "csv"
    pq_dir = out_path / "parquet"

    if cfg.output.csv:
        csv_dir.mkdir(exist_ok=True)
    if cfg.output.parquet:
        pq_dir.mkdir(exist_ok=True)

    file_metadata = []

    # Sort table names for deterministic output order
    sorted_tables = sorted(ctx.tables.keys())

    for table_name in sorted_tables:
        df = ctx.tables[table_name]
        
        # Export CSV
        if cfg.output.csv:
            csv_file = f"{table_name}.csv"
            csv_path = csv_dir / csv_file
            logger.info("Exporting %s to CSV...", table_name)
            df.to_csv(csv_path, index=False)
            
            # Record metadata
            file_metadata.append({
                "table": table_name,
                "file": f"csv/{csv_file}",
                "format": "csv",
                "rows": len(df),
                "hash": _calculate_file_hash(csv_path) if cfg.output.include_file_hashes else None
            })

        # Export Parquet
        if cfg.output.parquet:
            pq_file = f"{table_name}.parquet"
            pq_path = pq_dir / pq_file
            logger.info("Exporting %s to Parquet...", table_name)
            
            compression = cfg.output.compression
            if compression == "none":
                compression = None
                
            df.to_parquet(pq_path, index=False, compression=compression)
            
            # Record metadata
            file_metadata.append({
                "table": table_name,
                "file": f"parquet/{pq_file}",
                "format": "parquet",
                "rows": len(df),
                "hash": _calculate_file_hash(pq_path) if cfg.output.include_file_hashes else None
            })

    # Generate and save manifest
    manifest = generate_manifest(ctx, cfg, file_metadata)
    manifest_path = out_path / "manifest.json"
    
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    logger.info("Export complete. Manifest saved to %s", manifest_path)
    return manifest_path
