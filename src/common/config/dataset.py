"""Dataset mode configuration.

DATASET_MODE controls which dataset is the default for the system:
- "synthetic" (default): Financial transactions domain
- "pagila" (legacy): Film rental domain

This module provides a single source of truth for dataset-related defaults.
"""

import os
from typing import Literal

DatasetMode = Literal["synthetic", "pagila"]


def get_dataset_mode() -> DatasetMode:
    """Get the current dataset mode from environment.

    Returns:
        "synthetic" (default) or "pagila"

    Raises:
        ValueError: If DATASET_MODE is set to an invalid value.
    """
    mode = os.getenv("DATASET_MODE", "synthetic").lower()
    if mode not in ("synthetic", "pagila"):
        raise ValueError(f"Invalid DATASET_MODE: {mode}. Must be 'synthetic' or 'pagila'")

    if mode == "pagila":
        import warnings

        warnings.warn(
            "Pagila dataset is deprecated and will be removed in a future release. "
            "Please migrate to 'synthetic' mode.",
            DeprecationWarning,
            stacklevel=2,
        )

    return mode  # type: ignore[return-value]


def get_default_db_name() -> str:
    """Get the default database name based on dataset mode.

    Returns:
        "synthetic" for synthetic mode, "pagila" for pagila mode.
    """
    mode = get_dataset_mode()
    return "query_target" if mode == "synthetic" else "pagila"
