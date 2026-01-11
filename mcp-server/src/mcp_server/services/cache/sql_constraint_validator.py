"""SQL constraint validation using AST analysis.

This module validates that a SQL query satisfies extracted constraints
(rating, limit, etc.) using sqlglot AST parsing. It provides deterministic
validation without LLM calls.
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional

import sqlglot
from mcp_server.services.cache.constraint_extractor import QueryConstraints
from sqlglot import exp


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


VALID_RATINGS = frozenset({"G", "PG", "PG-13", "R", "NC-17"})


def extract_rating_from_sql(sql: str) -> Optional[str]:
    """Extract rating predicate value from SQL WHERE clause."""
    try:
        ast = sqlglot.parse_one(sql, dialect="postgres")
    except Exception:
        return _extract_rating_regex(sql)

    for eq in ast.find_all(exp.EQ):
        left = eq.left
        if isinstance(left, exp.Column) and left.name.lower() == "rating":
            right = eq.right
            if isinstance(right, exp.Literal) and right.is_string:
                rating = right.this.upper()
                if rating in VALID_RATINGS:
                    return rating

    for in_expr in ast.find_all(exp.In):
        if isinstance(in_expr.this, exp.Column) and in_expr.this.name.lower() == "rating":
            for val in in_expr.expressions:
                if isinstance(val, exp.Literal) and val.is_string:
                    rating = val.this.upper()
                    if rating in VALID_RATINGS:
                        return rating

    return _extract_rating_regex(sql)


def _extract_rating_regex(sql: str) -> Optional[str]:
    """Regex fallback for rating extraction."""
    patterns = [
        r"rating\s*=\s*'(NC-17|PG-13|PG|G|R)'",
        r'rating\s*=\s*"(NC-17|PG-13|PG|G|R)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            return match.group(1).upper()
    return None


def extract_limit_from_sql(sql: str) -> Optional[int]:
    """Extract LIMIT value from SQL query."""
    try:
        ast = sqlglot.parse_one(sql, dialect="postgres")
    except Exception:
        return _extract_limit_regex(sql)

    limit_expr = ast.find(exp.Limit)
    if limit_expr and limit_expr.expression:
        if isinstance(limit_expr.expression, exp.Literal):
            try:
                return int(limit_expr.expression.this)
            except (ValueError, TypeError):
                pass

    return _extract_limit_regex(sql)


def _extract_limit_regex(sql: str) -> Optional[int]:
    """Regex fallback for limit extraction."""
    match = re.search(r"\bLIMIT\s+(\d+)\b", sql, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def validate_sql_constraints(sql: str, constraints: QueryConstraints) -> ValidationResult:
    """Validate that a SQL query satisfies the given constraints."""
    mismatches = []
    extracted = {}

    if constraints.rating:
        sql_rating = extract_rating_from_sql(sql)
        extracted["rating"] = sql_rating

        if sql_rating is None:
            msg = f"Expected rating '{constraints.rating}' but " "no rating predicate found in SQL"
            mismatches.append(
                ConstraintMismatch(
                    constraint_type="rating",
                    expected=constraints.rating,
                    found=None,
                    message=msg,
                )
            )
        elif sql_rating != constraints.rating:
            msg = f"Rating mismatch: expected '{constraints.rating}', found '{sql_rating}'"
            mismatches.append(
                ConstraintMismatch(
                    constraint_type="rating",
                    expected=constraints.rating,
                    found=sql_rating,
                    message=msg,
                )
            )

    if constraints.limit:
        sql_limit = extract_limit_from_sql(sql)
        extracted["limit"] = sql_limit

        if sql_limit and not constraints.include_ties:
            if sql_limit != constraints.limit:
                mismatches.append(
                    ConstraintMismatch(
                        constraint_type="limit",
                        expected=str(constraints.limit),
                        found=str(sql_limit),
                        message=f"Limit mismatch: expected {constraints.limit}, found {sql_limit}",
                    )
                )

    return ValidationResult(
        is_valid=len(mismatches) == 0,
        mismatches=mismatches,
        extracted_predicates=extracted,
    )
