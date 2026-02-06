"""Dataset mode configuration.

DATASET_MODE controls which dataset is the default for the system:
- "synthetic" (default): Financial transactions domain

This module provides a single source of truth for dataset-related defaults.
"""

import os
from typing import Literal

DatasetMode = Literal["synthetic"]


def get_dataset_mode() -> DatasetMode:
    """Get the current dataset mode from environment.

    Returns:
        "synthetic" (default)

    Raises:
        ValueError: If DATASET_MODE is set to an invalid value.
    """
    mode = os.getenv("DATASET_MODE", "synthetic").lower()
    if mode != "synthetic":
        raise ValueError(f"Invalid DATASET_MODE: {mode}. Must be 'synthetic'")

    return mode  # type: ignore[return-value]


def get_default_db_name() -> str:
    """Get the default database name based on dataset mode.

    Returns:
        "query_target" for synthetic mode.
    """
    return "query_target"
