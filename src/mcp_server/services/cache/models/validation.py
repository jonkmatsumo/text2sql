from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ConstraintMismatch:
    """Details about a constraint that failed validation."""

    constraint_type: str  # "rating", "limit", etc.
    expected: str
    found: Optional[str]
    message: str

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "constraint_type": self.constraint_type,
            "expected": self.expected,
            "found": self.found,
            "message": self.message,
        }


@dataclass
class ValidationResult:
    """Result of SQL constraint validation."""

    is_valid: bool
    mismatches: List[ConstraintMismatch] = field(default_factory=list)
    extracted_predicates: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "is_valid": self.is_valid,
            "mismatches": [m.to_dict() for m in self.mismatches],
            "extracted_predicates": self.extracted_predicates,
        }
