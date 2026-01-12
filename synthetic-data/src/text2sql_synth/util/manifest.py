"""Manifest generation for synthetic data."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from text2sql_synth.config import SynthConfig
    from text2sql_synth.context import GenerationContext

from text2sql_synth import __version__


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
    return {
        "manifest_version": "1.0",
        "generator_version": __version__,
        "generated_at": datetime.now().isoformat(),
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
            for table_name, df in ctx.tables.items()
        },
        "files": files,
    }
