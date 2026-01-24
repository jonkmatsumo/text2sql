"""Golden dataset schema and types for synthetic domain evaluation.

This module defines the contract for golden test cases used in regression
testing and CI validation.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

# JSON Schema for golden dataset validation
GOLDEN_DATASET_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Synthetic Golden Dataset",
    "description": "Golden test cases for synthetic financial domain evaluation",
    "type": "object",
    "required": ["version", "dataset_mode", "test_cases"],
    "properties": {
        "version": {
            "type": "string",
            "description": "Schema version",
            "pattern": "^\\d+\\.\\d+$",
        },
        "dataset_mode": {
            "type": "string",
            "enum": ["synthetic", "pagila"],
            "description": "Dataset this golden set is designed for",
        },
        "test_cases": {
            "type": "array",
            "items": {"$ref": "#/definitions/test_case"},
            "minItems": 1,
        },
    },
    "definitions": {
        "test_case": {
            "type": "object",
            "required": ["id", "nlq", "expected_sql", "category", "difficulty"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Unique test case identifier",
                    "pattern": "^[a-z][a-z0-9_-]*$",
                },
                "nlq": {
                    "type": "string",
                    "description": "Natural language question",
                    "minLength": 5,
                },
                "expected_sql": {
                    "type": "string",
                    "description": "Ground truth SQL query",
                    "minLength": 5,
                },
                "expected_columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Expected column names in result",
                },
                "expected_row_count": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Expected number of rows (exact match)",
                },
                "expected_row_count_min": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Minimum expected rows (range check)",
                },
                "expected_row_count_max": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Maximum expected rows (range check)",
                },
                "intent": {
                    "type": "string",
                    "description": "High-level intent classification",
                },
                "category": {
                    "type": "string",
                    "enum": [
                        "basic",
                        "aggregation",
                        "join",
                        "filter",
                        "time-series",
                        "edge-case",
                    ],
                    "description": "Test category",
                },
                "difficulty": {
                    "type": "string",
                    "enum": ["easy", "medium", "hard"],
                    "description": "Test difficulty level",
                },
                "notes": {
                    "type": "string",
                    "description": "Free-form notes or metadata",
                },
                "skip": {
                    "type": "boolean",
                    "description": "Skip this test case",
                    "default": False,
                },
                "skip_reason": {
                    "type": "string",
                    "description": "Reason for skipping",
                },
            },
        },
    },
}


@dataclass
class GoldenTestCase:
    """A single golden test case."""

    id: str
    nlq: str
    expected_sql: str
    category: Literal["basic", "aggregation", "join", "filter", "time-series", "edge-case"]
    difficulty: Literal["easy", "medium", "hard"]
    expected_columns: List[str] = field(default_factory=list)
    expected_row_count: Optional[int] = None
    expected_row_count_min: Optional[int] = None
    expected_row_count_max: Optional[int] = None
    intent: Optional[str] = None
    notes: Optional[str] = None
    skip: bool = False
    skip_reason: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GoldenTestCase":
        """Create a GoldenTestCase from a dictionary."""
        return cls(
            id=data["id"],
            nlq=data["nlq"],
            expected_sql=data["expected_sql"],
            category=data["category"],
            difficulty=data["difficulty"],
            expected_columns=data.get("expected_columns", []),
            expected_row_count=data.get("expected_row_count"),
            expected_row_count_min=data.get("expected_row_count_min"),
            expected_row_count_max=data.get("expected_row_count_max"),
            intent=data.get("intent"),
            notes=data.get("notes"),
            skip=data.get("skip", False),
            skip_reason=data.get("skip_reason"),
        )


@dataclass
class GoldenDataset:
    """Container for a set of golden test cases."""

    version: str
    dataset_mode: Literal["synthetic", "pagila"]
    test_cases: List[GoldenTestCase]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GoldenDataset":
        """Create a GoldenDataset from a dictionary."""
        return cls(
            version=data["version"],
            dataset_mode=data["dataset_mode"],
            test_cases=[GoldenTestCase.from_dict(tc) for tc in data["test_cases"]],
        )

    def get_by_category(self, category: str) -> List[GoldenTestCase]:
        """Filter test cases by category."""
        return [tc for tc in self.test_cases if tc.category == category]

    def get_by_difficulty(self, difficulty: str) -> List[GoldenTestCase]:
        """Filter test cases by difficulty."""
        return [tc for tc in self.test_cases if tc.difficulty == difficulty]

    def get_active(self) -> List[GoldenTestCase]:
        """Get only non-skipped test cases."""
        return [tc for tc in self.test_cases if not tc.skip]
