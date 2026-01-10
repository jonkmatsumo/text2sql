"""Cache module for SQL query caching with constraint-based validation."""

from agent_core.cache.constraint_extractor import (
    QueryConstraints,
    extract_constraints,
    normalize_rating,
)
from agent_core.cache.intent_signature import IntentSignature, build_signature_from_constraints
from agent_core.cache.sql_constraint_validator import (
    ConstraintMismatch,
    ValidationResult,
    validate_sql_constraints,
)

__all__ = [
    "QueryConstraints",
    "extract_constraints",
    "normalize_rating",
    "ConstraintMismatch",
    "ValidationResult",
    "validate_sql_constraints",
    "IntentSignature",
    "build_signature_from_constraints",
]
