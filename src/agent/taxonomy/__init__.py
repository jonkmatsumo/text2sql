"""Error taxonomy package for SQL failure classification."""

from agent.taxonomy.error_taxonomy import (
    ERROR_TAXONOMY,
    classify_error,
    generate_correction_strategy,
)

__all__ = [
    "ERROR_TAXONOMY",
    "classify_error",
    "generate_correction_strategy",
]
