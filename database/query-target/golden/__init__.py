"""Golden dataset package for synthetic domain evaluation."""

from .golden_schema import GOLDEN_DATASET_SCHEMA, GoldenDataset, GoldenTestCase
from .loader import (
    GoldenDatasetError,
    GoldenDatasetNotFoundError,
    GoldenDatasetValidationError,
    load_golden_dataset,
    load_test_cases,
    validate_golden_dataset,
)

__all__ = [
    # Schema
    "GOLDEN_DATASET_SCHEMA",
    "GoldenDataset",
    "GoldenTestCase",
    # Loader
    "GoldenDatasetError",
    "GoldenDatasetNotFoundError",
    "GoldenDatasetValidationError",
    "load_golden_dataset",
    "load_test_cases",
    "validate_golden_dataset",
]
