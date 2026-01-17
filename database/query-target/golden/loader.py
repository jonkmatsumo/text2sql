"""Golden dataset loader with schema validation.

Provides functions to load and validate golden test cases from JSON files.
"""

import json
import logging
from pathlib import Path
from typing import List, Optional, Union

from .golden_schema import GoldenDataset, GoldenTestCase

logger = logging.getLogger(__name__)

# Default golden dataset paths by mode
GOLDEN_DATASET_DIR = Path(__file__).parent
GOLDEN_DATASET_FILES = {
    "synthetic": GOLDEN_DATASET_DIR / "synthetic_golden_dataset.json",
    "pagila": GOLDEN_DATASET_DIR / "pagila_golden_dataset.json",
}


class GoldenDatasetError(Exception):
    """Raised when golden dataset loading or validation fails."""

    pass


class GoldenDatasetNotFoundError(GoldenDatasetError):
    """Raised when golden dataset file is not found."""

    pass


class GoldenDatasetValidationError(GoldenDatasetError):
    """Raised when golden dataset fails schema validation."""

    pass


def validate_golden_dataset(data: dict) -> List[str]:
    """Validate golden dataset against JSON schema.

    Args:
        data: Parsed JSON data from golden dataset file.

    Returns:
        List of validation error messages (empty if valid).
    """
    errors = []

    # Check required top-level keys
    for key in ["version", "dataset_mode", "test_cases"]:
        if key not in data:
            errors.append(f"Missing required field: {key}")

    if "test_cases" in data:
        if not isinstance(data["test_cases"], list):
            errors.append("'test_cases' must be an array")
        elif len(data["test_cases"]) == 0:
            errors.append("'test_cases' must have at least one item")
        else:
            # Validate each test case
            for i, tc in enumerate(data["test_cases"]):
                tc_errors = _validate_test_case(tc, i)
                errors.extend(tc_errors)

    if "dataset_mode" in data and data["dataset_mode"] not in ("synthetic", "pagila"):
        errors.append(
            f"Invalid dataset_mode: {data['dataset_mode']}. Must be 'synthetic' or 'pagila'"
        )

    return errors


def _validate_test_case(tc: dict, index: int) -> List[str]:
    """Validate a single test case."""
    errors = []
    prefix = f"test_cases[{index}]"

    for key in ["id", "nlq", "expected_sql", "category", "difficulty"]:
        if key not in tc:
            errors.append(f"{prefix}: Missing required field '{key}'")

    if "id" in tc and not isinstance(tc["id"], str):
        errors.append(f"{prefix}: 'id' must be a string")

    if "nlq" in tc and (not isinstance(tc["nlq"], str) or len(tc["nlq"]) < 5):
        errors.append(f"{prefix}: 'nlq' must be a string with at least 5 characters")

    if "expected_sql" in tc and (
        not isinstance(tc["expected_sql"], str) or len(tc["expected_sql"]) < 5
    ):
        errors.append(f"{prefix}: 'expected_sql' must be a string with at least 5 characters")

    valid_categories = {"basic", "aggregation", "join", "filter", "time-series", "edge-case"}
    if "category" in tc and tc["category"] not in valid_categories:
        errors.append(
            f"{prefix}: 'category' must be one of {valid_categories}, got '{tc['category']}'"
        )

    valid_difficulties = {"easy", "medium", "hard"}
    if "difficulty" in tc and tc["difficulty"] not in valid_difficulties:
        errors.append(
            f"{prefix}: 'difficulty' must be one of {valid_difficulties}, got '{tc['difficulty']}'"
        )

    return errors


def load_golden_dataset(
    dataset_mode: str = "synthetic",
    *,
    path: Optional[Union[Path, str]] = None,
    validate: bool = True,
) -> GoldenDataset:
    """Load golden dataset for the specified dataset mode.

    Args:
        dataset_mode: Either 'synthetic' or 'pagila'
        path: Optional explicit path to golden dataset file.
              If not provided, uses default path based on dataset_mode.
        validate: Whether to validate the dataset against schema.

    Returns:
        GoldenDataset object with loaded test cases.

    Raises:
        GoldenDatasetNotFoundError: If golden dataset file not found.
        GoldenDatasetValidationError: If dataset fails validation.
    """
    if path is not None:
        file_path = Path(path)
    elif dataset_mode in GOLDEN_DATASET_FILES:
        file_path = GOLDEN_DATASET_FILES[dataset_mode]
    else:
        raise GoldenDatasetError(
            f"Invalid dataset_mode: {dataset_mode}. Must be 'synthetic' or 'pagila'"
        )

    if not file_path.exists():
        raise GoldenDatasetNotFoundError(
            f"Golden dataset not found: {file_path}. "
            f"Dataset mode '{dataset_mode}' requires a golden dataset file."
        )

    logger.info(f"Loading golden dataset from {file_path}")

    try:
        with open(file_path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise GoldenDatasetValidationError(f"Invalid JSON in {file_path}: {e}") from e

    if validate:
        errors = validate_golden_dataset(data)
        if errors:
            raise GoldenDatasetValidationError(
                "Golden dataset validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            )

    # Warn if dataset_mode in file doesn't match requested mode
    if data.get("dataset_mode") != dataset_mode:
        logger.warning(
            f"Golden dataset mode mismatch: file says '{data.get('dataset_mode')}' "
            f"but requested mode is '{dataset_mode}'"
        )

    dataset = GoldenDataset.from_dict(data)
    logger.info(
        f"Loaded {len(dataset.test_cases)} golden test cases "
        f"({len(dataset.get_active())} active)"
    )

    return dataset


def load_test_cases(
    dataset_mode: str = "synthetic",
    *,
    category: Optional[str] = None,
    difficulty: Optional[str] = None,
    include_skipped: bool = False,
) -> List[GoldenTestCase]:
    """Load and filter golden test cases.

    Args:
        dataset_mode: Either 'synthetic' or 'pagila'
        category: Optional category filter.
        difficulty: Optional difficulty filter.
        include_skipped: Whether to include skipped test cases.

    Returns:
        List of matching GoldenTestCase objects.
    """
    dataset = load_golden_dataset(dataset_mode)

    if include_skipped:
        test_cases = dataset.test_cases
    else:
        test_cases = dataset.get_active()

    if category:
        test_cases = [tc for tc in test_cases if tc.category == category]

    if difficulty:
        test_cases = [tc for tc in test_cases if tc.difficulty == difficulty]

    return test_cases
